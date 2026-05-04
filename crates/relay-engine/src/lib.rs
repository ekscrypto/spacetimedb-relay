// SPDX-License-Identifier: MIT

//! Subscription and query evaluation engine.
//!
//! Owns each downstream client's subscription state. When the upstream
//! delivers a transaction update, the engine determines which clients
//! are affected, applies WHERE predicates, and projects rows to the
//! columns each query requested before handing the per-client diffs
//! back to the downstream server.
//!
//! Public entry points:
//!   * [`Engine::compile`] / [`Engine::compile_for_sender`] —
//!     SQL → [`CompiledQuery`].
//!   * [`Engine::subscribe`] / [`Engine::unsubscribe`] /
//!     [`Engine::drop_client`] — registry operations.
//!   * [`Engine::route_table_diff`] — hot path for upstream
//!     `TransactionUpdate`.
//!   * [`Engine::snapshot_for`] — cold path for `SubscribeApplied`.
//!
//! `JOIN` is the one piece deliberately out of scope; the parser
//! returns `SqlFrom::Join`, but the engine still rejects it with
//! [`CompileError::Unsupported`]. Single-table subscriptions cover
//! every downstream SDK we know about.

use std::sync::Arc;

use bytes::Bytes;
use thiserror::Error;

use relay_protocol::{decode_row, BsatnError, DecodedRow, MirroredSchema};
use relay_storage::{Storage, StorageError};

mod predicate;
mod project;
mod query;
mod registry;

pub use predicate::{Literal, LogicOp, Predicate, PredicateOp};
pub use query::{compile, compile_for_sender, CompiledQuery, CompileError, Projection};
pub use registry::{ClientId, QuerySetId};
pub use spacetimedb_lib::Identity;

use registry::Registry;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("bsatn decode: {0}")]
    Bsatn(#[from] BsatnError),
    #[error("internal: {0}")]
    Internal(&'static str),
}

pub struct Engine {
    schema: Arc<MirroredSchema>,
    registry: Registry,
}

impl Engine {
    pub fn new(schema: Arc<MirroredSchema>) -> Self {
        Self {
            schema,
            registry: Registry::new(),
        }
    }

    /// Parse + validate one subscription query against the schema.
    /// Queries that reference `:sender` are rejected — use
    /// [`Engine::compile_for_sender`] instead.
    pub fn compile(&self, sql: &str) -> Result<CompiledQuery, CompileError> {
        compile(&self.schema, sql)
    }

    /// Compile a query whose `:sender` parameter resolves to `sender`.
    pub fn compile_for_sender(
        &self,
        sql: &str,
        sender: Identity,
    ) -> Result<CompiledQuery, CompileError> {
        compile_for_sender(&self.schema, sql, sender)
    }

    /// Register a compiled query for a client.
    pub fn subscribe(&self, client: ClientId, qset: QuerySetId, query: CompiledQuery) {
        let table = query.table.clone();
        self.registry.insert(client, qset, Arc::new(query));
        tracing::debug!(
            target: "relay::engine",
            client = ?client,
            qset = ?qset,
            table = %table,
            n_clients = self.registry.n_clients(),
            "subscribe"
        );
    }

    pub fn unsubscribe(&self, client: ClientId, qset: QuerySetId) {
        self.registry.remove_qset(client, qset);
        tracing::debug!(
            target: "relay::engine",
            client = ?client,
            qset = ?qset,
            n_clients = self.registry.n_clients(),
            "unsubscribe"
        );
    }

    pub fn drop_client(&self, client: ClientId) {
        self.registry.drop_client(client);
        tracing::debug!(
            target: "relay::engine",
            client = ?client,
            n_clients = self.registry.n_clients(),
            "drop_client"
        );
    }

    pub fn schema(&self) -> &Arc<MirroredSchema> {
        &self.schema
    }

    /// Hot path: turn one upstream table update into the per-client
    /// filtered diffs to forward downstream.
    ///
    /// Borrowed `&[DecodedRow]` is what relay/main.rs already produces
    /// (it decodes inserts/deletes for the storage write); the engine
    /// reuses those `Cell` values for predicate evaluation and the raw
    /// `bsatn` bytes for forwarding without re-encoding.
    pub fn route_table_diff(
        &self,
        table: &str,
        deletes: &[DecodedRow],
        inserts: &[DecodedRow],
    ) -> Vec<ClientTableDiff> {
        let Some(fields) = self
            .schema
            .tables
            .iter()
            .find(|t| t.name == table)
            .and_then(|t| self.schema.table_product(t))
        else {
            return Vec::new();
        };

        let mut routed = Vec::new();
        let mut total_kept_deletes = 0usize;
        let mut total_kept_inserts = 0usize;
        self.registry.for_table(table, |client, qset, query| {
            let kept_deletes = filter_and_project(&self.schema, query, fields, deletes);
            let kept_inserts = filter_and_project(&self.schema, query, fields, inserts);
            if kept_deletes.is_empty() && kept_inserts.is_empty() {
                return;
            }
            total_kept_deletes += kept_deletes.len();
            total_kept_inserts += kept_inserts.len();
            routed.push(ClientTableDiff {
                client,
                qset,
                table: query.table.clone(),
                deletes: kept_deletes,
                inserts: kept_inserts,
            });
        });
        if !routed.is_empty() {
            tracing::debug!(
                target: "relay::engine",
                table,
                deletes_in = deletes.len(),
                deletes_out = total_kept_deletes,
                inserts_in = inserts.len(),
                inserts_out = total_kept_inserts,
                clients = routed.len(),
                "route_table_diff"
            );
        }
        routed
    }

    /// Cold path: compute the rows a newly-subscribed client should
    /// receive in `SubscribeApplied`. Decodes every row currently
    /// mirrored in Postgres for the target table, applies the query's
    /// predicate, and projects the kept rows to the requested column
    /// subset.
    pub async fn snapshot_for(
        &self,
        storage: &Storage,
        query: &CompiledQuery,
    ) -> Result<Vec<Bytes>, EngineError> {
        let raw = storage.fetch_all_bsatn(query.table.as_ref()).await?;
        let table = self
            .schema
            .tables
            .get(query.table_idx)
            .ok_or(EngineError::Internal("compiled query references missing table"))?;
        let fields = self
            .schema
            .table_product(table)
            .ok_or(EngineError::Internal("table has no product type"))?;

        let raw_len = raw.len();
        let mut out = Vec::with_capacity(raw_len);
        for bytes in raw {
            if let Some(p) = &query.predicate {
                let cells = decode_row(&bytes, fields, &self.schema)?;
                if !p.matches(&cells) {
                    continue;
                }
            }
            match project::project_row(&self.schema, query, fields, &bytes) {
                Ok(b) => out.push(b),
                Err(e) => tracing::warn!(
                    target: "relay::engine",
                    error = %e,
                    table = %query.table,
                    "snapshot row projection failed; skipping"
                ),
            }
        }
        tracing::debug!(
            target: "relay::engine",
            table = %query.table,
            rows_in = raw_len,
            rows_out = out.len(),
            "snapshot_for"
        );
        Ok(out)
    }
}

#[derive(Debug, Clone)]
pub struct ClientTableDiff {
    pub client: ClientId,
    pub qset: QuerySetId,
    pub table: Arc<str>,
    pub deletes: Vec<Bytes>,
    pub inserts: Vec<Bytes>,
}

fn filter_and_project(
    schema: &MirroredSchema,
    query: &CompiledQuery,
    fields: &[relay_protocol::MirroredField],
    rows: &[DecodedRow],
) -> Vec<Bytes> {
    let mut out = Vec::new();
    for r in rows {
        if let Some(p) = &query.predicate {
            if !p.matches(&r.cells) {
                continue;
            }
        }
        match project::project_row(schema, query, fields, &r.bsatn) {
            Ok(b) => out.push(b),
            Err(e) => tracing::warn!(
                target: "relay::engine",
                error = %e,
                table = %query.table,
                "diff row projection failed; skipping"
            ),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use relay_protocol::{
        Cell, DecodedRow, MirroredField, MirroredSchema, MirroredTable, MirroredType, TableAccess,
        TableKind,
    };

    fn schema_one_table(fields: Vec<MirroredField>) -> MirroredSchema {
        let product = MirroredType::Product(fields);
        MirroredSchema {
            typespace: vec![product],
            tables: vec![MirroredTable {
                name: "thing".into(),
                product_type_ref: 0,
                primary_key: vec![],
                access: TableAccess::Public,
                kind: TableKind::User,
            }],
        }
    }

    fn row(cells: Vec<Cell>, tag: u8) -> DecodedRow {
        DecodedRow {
            cells,
            bsatn: Bytes::from(vec![tag]),
        }
    }

    #[test]
    fn route_filters_two_clients_by_different_predicates() {
        let schema = Arc::new(schema_one_table(vec![
            MirroredField {
                name: Some("kind".into()),
                ty: MirroredType::String,
            },
            MirroredField {
                name: Some("qty".into()),
                ty: MirroredType::I32,
            },
        ]));
        let engine = Engine::new(schema);

        let a = engine.compile("SELECT * FROM thing WHERE kind = 'sword'").unwrap();
        let b = engine.compile("SELECT * FROM thing WHERE qty = 4").unwrap();
        engine.subscribe(ClientId(1), QuerySetId(1), a);
        engine.subscribe(ClientId(2), QuerySetId(1), b);

        let inserts = vec![
            row(vec![Cell::Text(Some("sword".into())), Cell::Integer(Some(4))], 1),
            row(vec![Cell::Text(Some("shield".into())), Cell::Integer(Some(4))], 2),
            row(vec![Cell::Text(Some("sword".into())), Cell::Integer(Some(7))], 3),
            row(vec![Cell::Text(Some("potion".into())), Cell::Integer(Some(9))], 4),
        ];
        let routed = engine.route_table_diff("thing", &[], &inserts);
        assert_eq!(routed.len(), 2);

        let by_client: std::collections::HashMap<_, _> =
            routed.iter().map(|d| (d.client, d)).collect();
        let kept_a: Vec<u8> = by_client[&ClientId(1)]
            .inserts
            .iter()
            .map(|b| b[0])
            .collect();
        let kept_b: Vec<u8> = by_client[&ClientId(2)]
            .inserts
            .iter()
            .map(|b| b[0])
            .collect();
        assert_eq!(kept_a, vec![1, 3]); // both 'sword' rows
        assert_eq!(kept_b, vec![1, 2]); // both qty=4 rows
    }

    #[test]
    fn no_predicate_passes_everything() {
        let schema = Arc::new(schema_one_table(vec![MirroredField {
            name: Some("kind".into()),
            ty: MirroredType::String,
        }]));
        let engine = Engine::new(schema);
        let q = engine.compile("SELECT * FROM thing").unwrap();
        engine.subscribe(ClientId(1), QuerySetId(1), q);
        let inserts = vec![
            row(vec![Cell::Text(Some("a".into()))], 1),
            row(vec![Cell::Text(Some("b".into()))], 2),
        ];
        let routed = engine.route_table_diff("thing", &[], &inserts);
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].inserts.len(), 2);
    }

    #[test]
    fn no_match_produces_no_diff() {
        let schema = Arc::new(schema_one_table(vec![MirroredField {
            name: Some("kind".into()),
            ty: MirroredType::String,
        }]));
        let engine = Engine::new(schema);
        let q = engine.compile("SELECT * FROM thing WHERE kind = 'sword'").unwrap();
        engine.subscribe(ClientId(1), QuerySetId(1), q);
        let inserts = vec![row(vec![Cell::Text(Some("shield".into()))], 1)];
        let routed = engine.route_table_diff("thing", &[], &inserts);
        assert!(routed.is_empty());
    }

    #[test]
    fn drop_client_removes_routing() {
        let schema = Arc::new(schema_one_table(vec![MirroredField {
            name: Some("kind".into()),
            ty: MirroredType::String,
        }]));
        let engine = Engine::new(schema);
        let q = engine.compile("SELECT * FROM thing").unwrap();
        engine.subscribe(ClientId(7), QuerySetId(1), q);
        engine.drop_client(ClientId(7));
        let inserts = vec![row(vec![Cell::Text(Some("a".into()))], 1)];
        let routed = engine.route_table_diff("thing", &[], &inserts);
        assert!(routed.is_empty());
    }

    #[test]
    fn unsubscribe_removes_one_qset() {
        let schema = Arc::new(schema_one_table(vec![MirroredField {
            name: Some("kind".into()),
            ty: MirroredType::String,
        }]));
        let engine = Engine::new(schema);
        let q1 = engine.compile("SELECT * FROM thing WHERE kind = 'a'").unwrap();
        let q2 = engine.compile("SELECT * FROM thing WHERE kind = 'b'").unwrap();
        engine.subscribe(ClientId(1), QuerySetId(1), q1);
        engine.subscribe(ClientId(1), QuerySetId(2), q2);
        engine.unsubscribe(ClientId(1), QuerySetId(1));

        let inserts = vec![
            row(vec![Cell::Text(Some("a".into()))], 1),
            row(vec![Cell::Text(Some("b".into()))], 2),
        ];
        let routed = engine.route_table_diff("thing", &[], &inserts);
        assert_eq!(routed.len(), 1);
        assert_eq!(routed[0].qset, QuerySetId(2));
        assert_eq!(routed[0].inserts.len(), 1);
        assert_eq!(routed[0].inserts[0][0], 2);
    }
}
