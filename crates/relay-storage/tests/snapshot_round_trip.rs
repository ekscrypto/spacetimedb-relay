// SPDX-License-Identifier: MIT

//! Stage 3 integration test: write rows to a `MemStore`, persist via
//! `write_snapshots`, instantiate a fresh `MemStore` against the same
//! schema, call `load_snapshots`, and confirm the rows round-trip
//! intact. Files whose schema hash doesn't match the new schema are
//! ignored — covered by `mismatched_schema_hash_skips_load`.

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use relay_protocol::{
    decode_row, DecodedRow, MirroredField, MirroredSchema, MirroredTable, MirroredType,
    TableAccess, TableKind,
};
use relay_storage::{database_prefix, MemStore, TableSpec};

mod tempdir {
    use std::path::{Path, PathBuf};
    pub struct TempDir(PathBuf);
    impl TempDir {
        pub fn new(label: &str) -> std::io::Result<Self> {
            let pid = std::process::id();
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let dir = std::env::temp_dir().join(format!("relay-snap-{label}-{pid}-{nonce}"));
            std::fs::create_dir_all(&dir)?;
            Ok(Self(dir))
        }
        pub fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

fn schema_v1() -> Arc<MirroredSchema> {
    Arc::new(MirroredSchema {
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
    })
}

fn schema_v2_same_topology() -> Arc<MirroredSchema> {
    // Identical topology to v1 — fingerprint should match.
    schema_v1()
}

fn schema_v2_different() -> Arc<MirroredSchema> {
    // Adds a third column; different fingerprint, different shape.
    Arc::new(MirroredSchema {
        typespace: vec![MirroredType::Product(vec![
            MirroredField {
                name: Some("id".into()),
                ty: MirroredType::I32,
            },
            MirroredField {
                name: Some("payload".into()),
                ty: MirroredType::String,
            },
            MirroredField {
                name: Some("extra".into()),
                ty: MirroredType::I32,
            },
        ])],
        tables: vec![MirroredTable {
            name: "thing".into(),
            product_type_ref: 0,
            primary_key: vec![0],
            access: TableAccess::Public,
            kind: TableKind::User,
        }],
    })
}

fn specs(schema: &MirroredSchema) -> Vec<TableSpec> {
    relay_storage::build_table_specs(schema, "snap_test_db").expect("specs")
}

fn row(id: i32, payload: &str, schema: &MirroredSchema) -> DecodedRow {
    let mut buf = BytesMut::new();
    buf.put_i32_le(id);
    buf.put_u32_le(payload.len() as u32);
    buf.put_slice(payload.as_bytes());
    let bsatn = buf.freeze();
    let fields = schema
        .table_product(&schema.tables[0])
        .expect("product fields");
    let cells = decode_row(&bsatn, fields, schema).expect("decode");
    DecodedRow { cells, bsatn }
}

fn payloads_from_bytes(bytes: &[Bytes]) -> Vec<String> {
    let mut out = Vec::new();
    for b in bytes {
        let len = u32::from_le_bytes(b[4..8].try_into().unwrap()) as usize;
        out.push(String::from_utf8(b[8..8 + len].to_vec()).unwrap());
    }
    out.sort();
    out
}

#[test]
fn round_trip_through_disk() {
    let tmp = tempdir::TempDir::new("rt").unwrap();
    let dir = tmp.path().join(database_prefix("snap_test_db").unwrap());

    let schema = schema_v1();
    let writer = MemStore::new();
    writer.sync_schema(schema.clone(), &specs(&schema));
    writer
        .insert_rows(
            "thing",
            &[
                row(1, "alpha", &schema),
                row(2, "beta", &schema),
                row(3, "gamma", &schema),
            ],
        )
        .unwrap();
    let stats = writer.write_snapshots(&dir).unwrap();
    assert_eq!(stats.tables_written, 1);
    assert_eq!(stats.rows_written, 3);

    let reader = MemStore::new();
    reader.sync_schema(schema_v2_same_topology(), &specs(&schema));
    let stats = reader.load_snapshots(&dir).unwrap();
    assert_eq!(stats.tables_loaded, 1);
    assert_eq!(stats.rows_loaded, 3);

    let raw = reader.fetch_all_bsatn("thing").unwrap();
    assert_eq!(payloads_from_bytes(&raw), vec!["alpha", "beta", "gamma"]);
}

#[test]
fn mismatched_schema_hash_skips_load() {
    let tmp = tempdir::TempDir::new("mismatch").unwrap();
    let dir = tmp.path().join(database_prefix("snap_test_db").unwrap());

    let schema = schema_v1();
    let writer = MemStore::new();
    writer.sync_schema(schema.clone(), &specs(&schema));
    writer
        .insert_rows("thing", &[row(1, "alpha", &schema)])
        .unwrap();
    writer.write_snapshots(&dir).unwrap();

    let new_schema = schema_v2_different();
    let reader = MemStore::new();
    reader.sync_schema(new_schema.clone(), &specs(&new_schema));
    let stats = reader.load_snapshots(&dir).unwrap();
    assert_eq!(
        stats.tables_loaded, 0,
        "drifted snapshot must not load against new schema"
    );
    assert_eq!(reader.fetch_all_bsatn("thing").unwrap().len(), 0);
}

#[test]
fn empty_dir_load_is_a_noop() {
    let tmp = tempdir::TempDir::new("empty").unwrap();
    let dir = tmp.path().join("nonexistent");

    let schema = schema_v1();
    let store = MemStore::new();
    store.sync_schema(schema.clone(), &specs(&schema));
    let stats = store.load_snapshots(&dir).unwrap();
    assert_eq!(stats.tables_loaded, 0);
    assert_eq!(stats.rows_loaded, 0);
}

#[test]
fn second_snapshot_overwrites_first() {
    let tmp = tempdir::TempDir::new("over").unwrap();
    let dir = tmp.path().join(database_prefix("snap_test_db").unwrap());

    let schema = schema_v1();
    let store = MemStore::new();
    store.sync_schema(schema.clone(), &specs(&schema));
    store
        .insert_rows("thing", &[row(1, "v1", &schema)])
        .unwrap();
    store.write_snapshots(&dir).unwrap();

    // Replace the row and snapshot again.
    store
        .apply_diff("thing", &[row(1, "v1", &schema)], &[row(1, "v2", &schema)])
        .unwrap();
    store.write_snapshots(&dir).unwrap();

    let reader = MemStore::new();
    reader.sync_schema(schema.clone(), &specs(&schema));
    reader.load_snapshots(&dir).unwrap();
    let raw = reader.fetch_all_bsatn("thing").unwrap();
    assert_eq!(payloads_from_bytes(&raw), vec!["v2"]);
}
