// SPDX-License-Identifier: MIT

//! Helpers for emitting v2 ServerMessage frames downstream.
//!
//! Wire framing matches what `relay-upstream` expects on its incoming
//! side: `[u8 compression][BSATN ServerMessage]`. We always emit
//! compression byte 0 (None); compression is a future optimisation.

use std::sync::Arc;

use bytes::Bytes;
use rand::RngCore;

use relay_protocol::api_messages::websocket::common::{BsatnRowList, QuerySetId, RowSizeHint};
use relay_protocol::api_messages::websocket::v2::{
    InitialConnection, PersistentTableRows, QueryRows, QuerySetUpdate, ServerMessage,
    SingleTableRows, SubscribeApplied, SubscriptionError, TableUpdate, TableUpdateRows,
    TransactionUpdate,
};
use relay_protocol::lib::{ConnectionId, Identity};
use relay_protocol::sats::bsatn;

#[derive(Debug, thiserror::Error)]
pub enum EmitError {
    #[error("BSATN encode failed: {0}")]
    Encode(String),
}

pub fn frame(msg: &ServerMessage) -> Result<Vec<u8>, EmitError> {
    let body = bsatn::to_vec(msg).map_err(|e| EmitError::Encode(e.to_string()))?;
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(0u8);
    out.extend_from_slice(&body);
    Ok(out)
}

pub fn random_identity_and_connection() -> (Identity, ConnectionId) {
    let mut rng = rand::thread_rng();
    let mut id_bytes = [0u8; 32];
    rng.fill_bytes(&mut id_bytes);
    let mut conn_bytes = [0u8; 16];
    rng.fill_bytes(&mut conn_bytes);
    (
        Identity::from_byte_array(id_bytes),
        ConnectionId::from_le_byte_array(conn_bytes),
    )
}

pub fn initial_connection(
    identity: Identity,
    connection_id: ConnectionId,
    token: &str,
) -> ServerMessage {
    ServerMessage::InitialConnection(InitialConnection {
        identity,
        connection_id,
        token: token.into(),
    })
}

pub fn subscribe_applied(
    request_id: u32,
    query_set_id: u32,
    table_name: &str,
    rows_bsatn: &[Bytes],
) -> ServerMessage {
    let rows = bsatn_row_list(rows_bsatn);
    ServerMessage::SubscribeApplied(SubscribeApplied {
        request_id,
        query_set_id: QuerySetId::new(query_set_id),
        rows: QueryRows {
            tables: vec![SingleTableRows {
                table: table_name.to_string().into(),
                rows,
            }]
            .into_boxed_slice(),
        },
    })
}

pub fn subscription_error(
    request_id: Option<u32>,
    query_set_id: u32,
    error: &str,
) -> ServerMessage {
    ServerMessage::SubscriptionError(SubscriptionError {
        request_id,
        query_set_id: QuerySetId::new(query_set_id),
        error: error.into(),
    })
}

pub fn transaction_update_for_table(
    query_set_id: u32,
    table_name: &str,
    deletes: &[Bytes],
    inserts: &[Bytes],
) -> ServerMessage {
    let table_update = TableUpdate {
        table_name: table_name.to_string().into(),
        rows: vec![TableUpdateRows::PersistentTable(PersistentTableRows {
            inserts: bsatn_row_list(inserts),
            deletes: bsatn_row_list(deletes),
        })]
        .into_boxed_slice(),
    };
    ServerMessage::TransactionUpdate(TransactionUpdate {
        query_sets: vec![QuerySetUpdate {
            query_set_id: QuerySetId::new(query_set_id),
            tables: vec![table_update].into_boxed_slice(),
        }]
        .into_boxed_slice(),
    })
}

fn bsatn_row_list(rows: &[Bytes]) -> BsatnRowList {
    let mut offsets: Vec<u64> = Vec::with_capacity(rows.len());
    let total: usize = rows.iter().map(|r| r.len()).sum();
    let mut data = Vec::with_capacity(total);
    for row in rows {
        offsets.push(data.len() as u64);
        data.extend_from_slice(row);
    }
    BsatnRowList::new(RowSizeHint::RowOffsets(Arc::from(offsets)), data.into())
}
