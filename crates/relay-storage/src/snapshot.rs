// SPDX-License-Identifier: MIT

//! On-disk persistence format for the in-memory mirror.
//!
//! Layout: one file per upstream table at
//! `<data_dir>/<db_prefix>/<table>.snapshot`.
//!
//! ```text
//! +-----------------------------------+
//! | 64 ASCII bytes  schema fingerprint hex
//! +-----------------------------------+
//! |  8 LE bytes     row count (u64)
//! +-----------------------------------+
//! | repeated row_count times:
//! |   4 LE bytes  pk_len (u32)
//! |   pk_len bytes  pk bytes
//! |   4 LE bytes  bsatn_len (u32)
//! |   bsatn_len bytes  bsatn payload
//! +-----------------------------------+
//! ```
//!
//! Writes are atomic-per-file: serialize to `<table>.snapshot.tmp`,
//! `fsync`, then `rename` over the live name. Per-table files mean a
//! crash mid-write only loses one table, never the whole mirror.
//!
//! On load we compare the header's hash to the upstream's current
//! schema fingerprint — mismatched files are silently ignored (the
//! caller's plan is to gap-fill via the next `SubscribeApplied`).

use std::fs;
use std::io::{self, BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

use bytes::Bytes;

pub const SCHEMA_HASH_LEN: usize = 64;
const SNAPSHOT_EXT: &str = "snapshot";
const TMP_EXT: &str = "snapshot.tmp";

pub type SnapshotRow = (Vec<u8>, Bytes);

pub fn table_path(dir: &Path, sanitized_table: &str) -> PathBuf {
    dir.join(format!("{sanitized_table}.{SNAPSHOT_EXT}"))
}

/// Write one table's rows. Caller pre-computed `(pk, bsatn)` pairs;
/// `schema_hash_hex` must be exactly `SCHEMA_HASH_LEN` ASCII bytes.
pub fn write_table<I>(path: &Path, schema_hash_hex: &str, n_rows: usize, rows: I) -> io::Result<()>
where
    I: IntoIterator<Item = SnapshotRow>,
{
    if schema_hash_hex.len() != SCHEMA_HASH_LEN {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "schema hash must be {SCHEMA_HASH_LEN} ASCII bytes, got {}",
                schema_hash_hex.len()
            ),
        ));
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(TMP_EXT);
    {
        let f = fs::File::create(&tmp)?;
        let mut w = BufWriter::new(f);
        w.write_all(schema_hash_hex.as_bytes())?;
        w.write_all(&(n_rows as u64).to_le_bytes())?;
        for (pk, bsatn) in rows {
            w.write_all(&(pk.len() as u32).to_le_bytes())?;
            w.write_all(&pk)?;
            w.write_all(&(bsatn.len() as u32).to_le_bytes())?;
            w.write_all(&bsatn)?;
        }
        w.flush()?;
        w.into_inner()
            .map_err(|e| io::Error::other(format!("flush: {e}")))?
            .sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Read one table's rows. Returns `Ok(None)` if the schema hash header
/// doesn't match `expected_hash_hex` — the file is from a previous
/// schema and the caller must gap-fill from upstream.
pub fn read_table(path: &Path, expected_hash_hex: &str) -> io::Result<Option<Vec<SnapshotRow>>> {
    let f = fs::File::open(path)?;
    let mut r = BufReader::new(f);

    let mut hash = [0u8; SCHEMA_HASH_LEN];
    r.read_exact(&mut hash)?;
    if hash != expected_hash_hex.as_bytes() {
        return Ok(None);
    }

    let mut count_buf = [0u8; 8];
    r.read_exact(&mut count_buf)?;
    let n = u64::from_le_bytes(count_buf) as usize;
    let mut out = Vec::with_capacity(n);

    let mut len_buf = [0u8; 4];
    for _ in 0..n {
        r.read_exact(&mut len_buf)?;
        let pk_len = u32::from_le_bytes(len_buf) as usize;
        let mut pk = vec![0u8; pk_len];
        r.read_exact(&mut pk)?;

        r.read_exact(&mut len_buf)?;
        let body_len = u32::from_le_bytes(len_buf) as usize;
        let mut body = vec![0u8; body_len];
        r.read_exact(&mut body)?;
        out.push((pk, Bytes::from(body)));
    }
    Ok(Some(out))
}

/// Iterate `<dir>` for files with the snapshot extension. Used by
/// `MemStore::load_snapshots` to know which tables to attempt to load.
pub fn list_snapshot_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some(SNAPSHOT_EXT) {
            out.push(path);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile_dir::TempDir;

    mod tempfile_dir {
        use std::path::{Path, PathBuf};
        pub struct TempDir(PathBuf);
        impl TempDir {
            pub fn new() -> std::io::Result<Self> {
                let pid = std::process::id();
                let nonce = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let dir = std::env::temp_dir().join(format!("relay-snap-test-{pid}-{nonce}"));
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

    fn fake_hash() -> String {
        "a".repeat(SCHEMA_HASH_LEN)
    }

    #[test]
    fn round_trip_one_table() {
        let dir = TempDir::new().unwrap();
        let path = table_path(dir.path(), "thing");
        let rows = vec![
            (vec![1, 0, 0, 0], Bytes::from(b"row-1".to_vec())),
            (vec![2, 0, 0, 0], Bytes::from(b"row-2-longer".to_vec())),
        ];
        write_table(&path, &fake_hash(), rows.len(), rows.clone()).unwrap();

        let loaded = read_table(&path, &fake_hash())
            .unwrap()
            .expect("hash matches");
        assert_eq!(loaded.len(), rows.len());
        assert_eq!(loaded[0].0, rows[0].0);
        assert_eq!(loaded[0].1, rows[0].1);
        assert_eq!(loaded[1].0, rows[1].0);
        assert_eq!(loaded[1].1, rows[1].1);
    }

    #[test]
    fn mismatched_hash_returns_none() {
        let dir = TempDir::new().unwrap();
        let path = table_path(dir.path(), "thing");
        write_table(&path, &fake_hash(), 0, std::iter::empty()).unwrap();
        let loaded = read_table(&path, &"b".repeat(SCHEMA_HASH_LEN)).unwrap();
        assert!(loaded.is_none(), "hash mismatch should return None");
    }

    #[test]
    fn empty_table_round_trips() {
        let dir = TempDir::new().unwrap();
        let path = table_path(dir.path(), "thing");
        write_table(&path, &fake_hash(), 0, std::iter::empty()).unwrap();
        let loaded = read_table(&path, &fake_hash()).unwrap().unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn list_snapshot_files_skips_non_snapshots() {
        let dir = TempDir::new().unwrap();
        write_table(
            &table_path(dir.path(), "a"),
            &fake_hash(),
            0,
            std::iter::empty(),
        )
        .unwrap();
        write_table(
            &table_path(dir.path(), "b"),
            &fake_hash(),
            0,
            std::iter::empty(),
        )
        .unwrap();
        std::fs::write(dir.path().join("not-a-snapshot.txt"), b"hi").unwrap();
        let files = list_snapshot_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn missing_dir_lists_empty() {
        let files = list_snapshot_files(Path::new("/nonexistent-relay-dir-2026")).unwrap();
        assert!(files.is_empty());
    }
}
