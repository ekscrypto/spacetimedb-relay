// SPDX-License-Identifier: MIT

//! Parity test: every mutating call on `Storage` writes to both the
//! Postgres mirror and the in-memory store. After each phase, dump the
//! contents of both and assert they match.
//!
//! Reads `TEST_DATABASE_URL` (falling back to `DATABASE_URL`, then to
//! the workspace default `postgres://relay:relay@localhost:5432/relay`).
//! Skipped (with a printed warning) if no Postgres is reachable, so
//! `cargo test` doesn't fail on machines without docker.

use bytes::{BufMut, Bytes, BytesMut};
use relay_protocol::{
    decode_row, DecodedRow, MirroredField, MirroredSchema, MirroredTable, MirroredType,
    TableAccess, TableKind,
};
use relay_storage::{Storage, StorageConfig};
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://relay:relay@localhost:5432/relay".into())
}

fn sample_schema() -> MirroredSchema {
    MirroredSchema {
        typespace: vec![MirroredType::Product(vec![
            MirroredField {
                name: Some("id".into()),
                ty: MirroredType::I32,
            },
            MirroredField {
                name: Some("payload".into()),
                ty: MirroredType::String,
            },
        ])],
        tables: vec![MirroredTable {
            name: "thing".into(),
            product_type_ref: 0,
            primary_key: vec![0],
            access: TableAccess::Public,
            kind: TableKind::User,
        }],
    }
}

fn encode_row(id: i32, payload: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i32_le(id);
    buf.put_u32_le(payload.len() as u32);
    buf.put_slice(payload.as_bytes());
    buf.freeze()
}

fn row(id: i32, payload: &str, fields: &[MirroredField], schema: &MirroredSchema) -> DecodedRow {
    let bsatn = encode_row(id, payload);
    let cells = decode_row(&bsatn, fields, schema).expect("decode our own bsatn");
    DecodedRow { cells, bsatn }
}

async fn cleanup(storage: &Storage, upstream_database: &str) {
    let pool = storage.pool().clone();
    let postgres_tables: Vec<(String,)> =
        sqlx::query_as("SELECT postgres_table FROM relay_meta_table WHERE upstream_database = $1")
            .bind(upstream_database)
            .fetch_all(&pool)
            .await
            .unwrap_or_default();
    for (pt,) in postgres_tables {
        let _ = sqlx::query(&format!("DROP TABLE IF EXISTS \"{pt}\" CASCADE"))
            .execute(&pool)
            .await;
    }
    let _ = sqlx::query("DELETE FROM relay_meta_database WHERE upstream_database = $1")
        .bind(upstream_database)
        .execute(&pool)
        .await;
}

fn sorted(bytes: &[Bytes]) -> Vec<Vec<u8>> {
    let mut out: Vec<Vec<u8>> = bytes.iter().map(|b| b.to_vec()).collect();
    out.sort();
    out
}

async fn assert_parity(storage: &Storage, table: &str) {
    let pg = storage.fetch_all_bsatn(table).await.expect("pg dump");
    let mem = storage.mem().fetch_all_bsatn(table).expect("mem dump");
    assert_eq!(
        sorted(&pg),
        sorted(&mem),
        "pg vs mem divergence in table {table}: pg={pg:?} mem={mem:?}"
    );
}

#[tokio::test]
async fn memstore_matches_pg_after_each_mutation() {
    let upstream_database = format!("relay_parity_{}", Uuid::new_v4().simple());
    let cfg = StorageConfig {
        database_url: database_url(),
        upstream_host: "test://localhost".into(),
        upstream_database: upstream_database.clone(),
    };

    let storage = match Storage::connect(cfg).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "[memstore_pg_parity] skipping — Postgres not reachable: {e}\n\
                 Bring it up with: docker compose up -d postgres"
            );
            return;
        }
    };

    let schema = sample_schema();
    storage.sync_schema(&schema).await.expect("sync_schema");

    let table = &schema.tables[0];
    let fields = schema
        .table_product(table)
        .expect("product fields")
        .to_vec();

    // ---- Phase 1: insert via apply_snapshot_diff (initial) ----
    let phase1 = vec![
        row(1, "a-v1", &fields, &schema),
        row(2, "b-v1", &fields, &schema),
        row(3, "c-v1", &fields, &schema),
    ];
    storage
        .apply_snapshot_diff("thing", &phase1, &fields, &schema)
        .await
        .expect("snapshot phase 1");
    assert_parity(&storage, "thing").await;

    // ---- Phase 2: mixed snapshot — update id=2, drop id=3, add id=4 ----
    let phase2 = vec![
        row(1, "a-v1", &fields, &schema),
        row(2, "b-v2", &fields, &schema),
        row(4, "d-v1", &fields, &schema),
    ];
    storage
        .apply_snapshot_diff("thing", &phase2, &fields, &schema)
        .await
        .expect("snapshot phase 2");
    assert_parity(&storage, "thing").await;

    // ---- Phase 3: TransactionUpdate-style apply_diff ----
    // delete id=4, insert id=5
    let deletes = vec![row(4, "d-v1", &fields, &schema)];
    let inserts = vec![row(5, "e-v1", &fields, &schema)];
    storage
        .apply_diff("thing", &deletes, &inserts)
        .await
        .expect("apply_diff phase 3");
    assert_parity(&storage, "thing").await;

    // ---- Phase 4: replay the same snapshot — pure no-op ----
    let phase4 = vec![
        row(1, "a-v1", &fields, &schema),
        row(2, "b-v2", &fields, &schema),
        row(5, "e-v1", &fields, &schema),
    ];
    storage
        .apply_snapshot_diff("thing", &phase4, &fields, &schema)
        .await
        .expect("snapshot phase 4");
    assert_parity(&storage, "thing").await;

    // ---- Phase 5: insert_rows path ----
    let extra = vec![row(6, "f-v1", &fields, &schema)];
    storage
        .insert_rows("thing", &extra)
        .await
        .expect("insert_rows");
    assert_parity(&storage, "thing").await;

    cleanup(&storage, &upstream_database).await;
}
