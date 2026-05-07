// SPDX-License-Identifier: MIT

//! Drives the codegen → cargo build → `spacetime publish` pipeline that
//! materializes a SpacetimeDB module mirroring an upstream's table set.
//!
//! The relay calls [`Publisher::publish_if_drifted`] each time it
//! receives a fresh upstream schema. If the schema's fingerprint matches
//! what we last published, the call is a no-op. Otherwise we regenerate
//! the mirror crate, build its wasm, and `spacetime publish -y` it,
//! which **wipes the entire local database** — see invariant #4 in
//! `CLAUDE.md`. We do not attempt partial preservation, because the
//! upstream's migration semantics aren't visible to us.

use std::path::PathBuf;
use std::process::Stdio;

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tracing::{debug, info, warn};

const FINGERPRINT_FILENAME: &str = "fingerprint.json";

#[derive(Debug, Clone)]
pub struct PublisherConfig {
    /// Workspace directory for the generated mirror crate. We materialize
    /// `Cargo.toml` from `template_dir`, write `src/lib.rs` from codegen,
    /// and run `cargo build` here.
    pub workdir: PathBuf,
    /// Source directory containing `Cargo.toml` and `src/.gitkeep` for the
    /// mirror crate. Copied into `workdir` on first run.
    pub template_dir: PathBuf,
    /// Path to the python3 codegen script.
    pub codegen_script: PathBuf,
    /// `spacetime` CLI binary.
    pub spacetime_bin: PathBuf,
    /// Server nickname known to the spacetime CLI (e.g. "relay-local").
    pub stdb_server: String,
    /// Database name to publish under.
    pub database_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Sidecar {
    fingerprint: String,
    database_name: String,
}

#[derive(Debug)]
pub struct PublishOutcome {
    pub fingerprint: String,
    /// True when we (re)published. False on no-op.
    pub republished: bool,
}

pub struct Publisher {
    cfg: PublisherConfig,
}

impl Publisher {
    pub fn new(cfg: PublisherConfig) -> Self {
        Self { cfg }
    }

    /// Idempotent: if `schema_json`'s fingerprint matches what we last
    /// published under `cfg.database_name`, returns immediately.
    /// Otherwise regenerates, rebuilds, and republishes — wiping the
    /// local database in the process.
    pub async fn publish_if_drifted(&self, schema_json: &[u8]) -> Result<PublishOutcome> {
        let fingerprint = fingerprint_hex(schema_json);
        if let Some(prev) = self.read_sidecar()? {
            if prev.fingerprint == fingerprint && prev.database_name == self.cfg.database_name {
                debug!(
                    target: "relay::publisher",
                    fingerprint = %fingerprint,
                    "schema unchanged — skipping republish"
                );
                return Ok(PublishOutcome {
                    fingerprint,
                    republished: false,
                });
            }
            info!(
                target: "relay::publisher",
                old = %prev.fingerprint,
                new = %fingerprint,
                "schema drift detected — full republish (database will be wiped)"
            );
        } else {
            info!(
                target: "relay::publisher",
                fingerprint = %fingerprint,
                "no prior publish — bootstrapping module"
            );
        }
        self.materialize_workdir().await?;
        self.write_schema(schema_json).await?;
        self.run_codegen().await?;
        self.run_cargo_build().await?;
        self.run_spacetime_publish().await?;
        self.write_sidecar(&fingerprint)?;
        Ok(PublishOutcome {
            fingerprint,
            republished: true,
        })
    }

    fn sidecar_path(&self) -> PathBuf {
        self.cfg.workdir.join(FINGERPRINT_FILENAME)
    }

    fn read_sidecar(&self) -> Result<Option<Sidecar>> {
        let p = self.sidecar_path();
        if !p.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&p).with_context(|| format!("read {}", p.display()))?;
        let s = serde_json::from_slice::<Sidecar>(&bytes)
            .with_context(|| format!("parse {}", p.display()))?;
        Ok(Some(s))
    }

    fn write_sidecar(&self, fingerprint: &str) -> Result<()> {
        let s = Sidecar {
            fingerprint: fingerprint.to_string(),
            database_name: self.cfg.database_name.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&s)?;
        let p = self.sidecar_path();
        std::fs::write(&p, bytes).with_context(|| format!("write {}", p.display()))?;
        Ok(())
    }

    async fn materialize_workdir(&self) -> Result<()> {
        if self.cfg.workdir.join("Cargo.toml").exists() {
            return Ok(());
        }
        tokio::fs::create_dir_all(&self.cfg.workdir.join("src")).await?;
        for fname in ["Cargo.toml", "rust-toolchain.toml"] {
            let src = self.cfg.template_dir.join(fname);
            let dst = self.cfg.workdir.join(fname);
            if !src.exists() {
                continue;
            }
            tokio::fs::copy(&src, &dst)
                .await
                .with_context(|| format!("copy {} to {}", src.display(), dst.display()))?;
        }
        info!(
            target: "relay::publisher",
            workdir = %self.cfg.workdir.display(),
            "materialized mirror crate workspace"
        );
        Ok(())
    }

    async fn write_schema(&self, schema_json: &[u8]) -> Result<()> {
        let p = self.cfg.workdir.join("schema.json");
        let mut f = tokio::fs::File::create(&p)
            .await
            .with_context(|| format!("create {}", p.display()))?;
        f.write_all(schema_json).await?;
        f.flush().await?;
        Ok(())
    }

    async fn run_codegen(&self) -> Result<()> {
        let schema_path = self.cfg.workdir.join("schema.json");
        let lib_path = self.cfg.workdir.join("src").join("lib.rs");
        info!(
            target: "relay::publisher",
            "running codegen"
        );
        let output = Command::new("python3")
            .arg(&self.cfg.codegen_script)
            .arg(&schema_path)
            .arg("-o")
            .arg(&lib_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::piped())
            .output()
            .await
            .with_context(|| format!("spawn python3 {}", self.cfg.codegen_script.display()))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!(
                "codegen failed (status {}): {stderr}",
                output.status
            ));
        }
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.trim().is_empty() {
            for line in stderr.lines() {
                warn!(target: "relay::publisher", "codegen: {line}");
            }
        }
        Ok(())
    }

    async fn run_cargo_build(&self) -> Result<()> {
        info!(target: "relay::publisher", "building mirror crate to wasm32");
        let status = Command::new("cargo")
            .args(["build", "--release", "--target", "wasm32-unknown-unknown"])
            .current_dir(&self.cfg.workdir)
            .status()
            .await
            .context("spawn cargo build")?;
        if !status.success() {
            return Err(anyhow!("cargo build failed: {status}"));
        }
        Ok(())
    }

    async fn run_spacetime_publish(&self) -> Result<()> {
        info!(
            target: "relay::publisher",
            server = %self.cfg.stdb_server,
            database = %self.cfg.database_name,
            "spacetime publish (wipes existing data via --delete-data)"
        );
        // `--delete-data` (`-c`) forces a full wipe of the database on
        // republish. Without it, SpacetimeDB tries to preserve data when
        // the schema diff looks compatible — but our upstream's migration
        // semantics are opaque to us, so we never want partial preservation.
        // See invariant #4 in CLAUDE.md.
        let status = Command::new(&self.cfg.spacetime_bin)
            .args([
                "publish",
                "-s",
                &self.cfg.stdb_server,
                "-y",
                "--delete-data",
                &self.cfg.database_name,
            ])
            .current_dir(&self.cfg.workdir)
            .status()
            .await
            .context("spawn spacetime publish")?;
        if !status.success() {
            return Err(anyhow!("spacetime publish failed: {status}"));
        }
        Ok(())
    }
}

fn fingerprint_hex(schema_json: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(schema_json);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_stable() {
        let a = fingerprint_hex(b"hello");
        let b = fingerprint_hex(b"hello");
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn fingerprint_changes_with_input() {
        assert_ne!(fingerprint_hex(b"a"), fingerprint_hex(b"b"));
    }

    #[test]
    fn sidecar_roundtrip() -> Result<()> {
        let dir = tempdir_for_test();
        let cfg = PublisherConfig {
            workdir: dir.clone(),
            template_dir: dir.clone(),
            codegen_script: dir.join("nope"),
            spacetime_bin: PathBuf::from("nope"),
            stdb_server: "x".into(),
            database_name: "y".into(),
        };
        let p = Publisher::new(cfg);
        assert!(p.read_sidecar()?.is_none());
        p.write_sidecar("abc")?;
        let s = p.read_sidecar()?.unwrap();
        assert_eq!(s.fingerprint, "abc");
        assert_eq!(s.database_name, "y");
        Ok(())
    }

    fn tempdir_for_test() -> PathBuf {
        let p = std::env::temp_dir().join(format!("relay-publisher-test-{}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}
