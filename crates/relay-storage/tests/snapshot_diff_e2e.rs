// SPDX-License-Identifier: MIT

//! End-to-end test for `Storage::apply_snapshot_diff` against a live
//! Postgres.
//!
//! Reads `TEST_DATABASE_URL` (falling back to `DATABASE_URL`, then to
//! the workspace default `postgres://relay:relay@localhost:5432/relay`).
//! Skipped (with a printed warning) if no Postgres is reachable, so
//! `cargo test` doesn't fail on machines without docker.
//!
//! Bring Postgres up with `docker compose up -d postgres` from the
//! repo root.

use bytes::{BufMut, Bytes, BytesMut};
use relay_protocol::{
    decode_row, Cell, DecodedRow, MirroredField, MirroredSchema, MirroredTable, MirroredType,
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

fn extract_id_payload(
    row_bytes: &[u8],
    fields: &[MirroredField],
    schema: &MirroredSchema,
) -> (i32, String) {
    let cells = decode_row(row_bytes, fields, schema).expect("decode stored row");
    let id = match &cells[0] {
        Cell::Integer(Some(v)) => *v,
        other => panic!("unexpected id cell {other:?}"),
    };
    let payload = match &cells[1] {
        Cell::Text(Some(s)) => s.clone(),
        other => panic!("unexpected payload cell {other:?}"),
    };
    (id, payload)
}

fn payload_of(row: &DecodedRow) -> String {
    match &row.cells[1] {
        Cell::Text(Some(s)) => s.clone(),
        other => panic!("unexpected payload cell {other:?}"),
    }
}

async fn cleanup(storage: &Storage, upstream_database: &str) {
    let pool = storage.pool().clone();
    // Drop the actual mirror tables first; `relay_meta_database`
    // CASCADEs `relay_meta_table` rows but never the data tables.
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

#[tokio::test]
async fn snapshot_diff_reconciles_gap_against_real_postgres() {
    let upstream_database = format!("relay_e2e_{}", Uuid::new_v4().simple());
    let cfg = StorageConfig {
        database_url: database_url(),
        upstream_host: "test://localhost".into(),
        upstream_database: upstream_database.clone(),
    };

    let storage = match Storage::connect(cfg).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "[snapshot_diff_e2e] skipping — Postgres not reachable: {e}\n\
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

    // ---- Phase 1: initial snapshot, PG empty ----
    let phase1 = vec![
        row(1, "a-v1", &fields, &schema),
        row(2, "b-v1", &fields, &schema),
        row(3, "c-v1", &fields, &schema),
    ];
    let diff1 = storage
        .apply_snapshot_diff("thing", &phase1, &fields, &schema)
        .await
        .expect("apply_snapshot_diff phase 1");
    assert!(
        diff1.deletes.is_empty(),
        "first snapshot should produce no deletes: {:?}",
        diff1.deletes
    );
    assert_eq!(
        diff1.inserts.len(),
        3,
        "first snapshot should insert all 3 rows"
    );

    let stored = storage.fetch_all_bsatn("thing").await.unwrap();
    assert_eq!(stored.len(), 3, "PG should have 3 rows after phase 1");

    // ---- Phase 2: simulated gap — relay was disconnected, upstream
    // applied changes:
    //   - id=1 unchanged (no-op)
    //   - id=2 payload updated (delete-then-insert)
    //   - id=3 dropped from snapshot (pure delete)
    //   - id=4 brand new (pure insert)
    let phase2 = vec![
        row(1, "a-v1", &fields, &schema),
        row(2, "b-v2", &fields, &schema),
        row(4, "d-v1", &fields, &schema),
    ];
    let diff2 = storage
        .apply_snapshot_diff("thing", &phase2, &fields, &schema)
        .await
        .expect("apply_snapshot_diff phase 2");

    let mut deleted: Vec<String> = diff2.deletes.iter().map(payload_of).collect();
    deleted.sort();
    assert_eq!(
        deleted,
        vec!["b-v1".to_string(), "c-v1".to_string()],
        "expected updates' old version (b-v1) and pure delete (c-v1)"
    );

    let mut inserted: Vec<String> = diff2.inserts.iter().map(payload_of).collect();
    inserted.sort();
    assert_eq!(
        inserted,
        vec!["b-v2".to_string(), "d-v1".to_string()],
        "expected updates' new version (b-v2) and pure insert (d-v1)"
    );

    // PG state should reflect the merged snapshot.
    let stored = storage.fetch_all_bsatn("thing").await.unwrap();
    let mut state: Vec<(i32, String)> = stored
        .iter()
        .map(|b| extract_id_payload(b, &fields, &schema))
        .collect();
    state.sort();
    assert_eq!(
        state,
        vec![(1, "a-v1".into()), (2, "b-v2".into()), (4, "d-v1".into())],
        "row 3 should be gone, row 2 updated, row 4 inserted"
    );

    // ---- Phase 3: replay the same snapshot — must be a pure no-op ----
    let diff3 = storage
        .apply_snapshot_diff("thing", &phase2, &fields, &schema)
        .await
        .expect("apply_snapshot_diff phase 3");
    assert!(
        diff3.deletes.is_empty(),
        "no-op replay should produce no deletes: {:?}",
        diff3.deletes
    );
    assert!(
        diff3.inserts.is_empty(),
        "no-op replay should produce no inserts: {:?}",
        diff3.inserts
    );

    cleanup(&storage, &upstream_database).await;
}
