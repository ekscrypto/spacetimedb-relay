// Smoke-test the Publisher against the cached BitCraft 14 schema.
// Requires a running local SpacetimeDB and a server alias `spike-local`.
//
//   cargo run -p relay-publisher --example smoke -- /tmp/bitcraft-14-schema.json

use std::path::PathBuf;

use relay_publisher::{Publisher, PublisherConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let schema_path = std::env::args()
        .nth(1)
        .expect("usage: smoke <schema.json>");
    let schema = std::fs::read(&schema_path)?;

    let repo_root: PathBuf = env!("CARGO_MANIFEST_DIR").into();
    let repo_root = repo_root
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root")
        .to_path_buf();

    let cfg = PublisherConfig {
        workdir: PathBuf::from("/tmp/relay-publisher-smoke-workdir"),
        template_dir: repo_root.join("tools/mirror-template"),
        codegen_script: repo_root.join("tools/codegen.py"),
        spacetime_bin: PathBuf::from("/Users/ekscrypto/.local/bin/spacetime"),
        stdb_server: "spike-local".to_string(),
        database_name: "relay-mirror-smoke".to_string(),
    };
    let pub1 = Publisher::new(cfg.clone());

    let r1 = pub1.publish_if_drifted(&schema).await?;
    println!("first call: republished={} fingerprint={}", r1.republished, r1.fingerprint);

    let r2 = pub1.publish_if_drifted(&schema).await?;
    println!("second call (same schema): republished={}", r2.republished);
    assert!(!r2.republished, "second call should be a no-op");

    Ok(())
}
