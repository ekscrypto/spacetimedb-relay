// SPDX-License-Identifier: MIT

//! CLI args + env vars. Mirrors the convention used by the relay binary
//! and relay-coordinator: clap derive, every flag dual-wired to a
//! `RELAY_CACHE_*` env var, sensible production defaults so an operator
//! can run with no flags on the relay host.

use std::path::PathBuf;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "relay-cache",
    version,
    about = "Same-host in-memory read cache over the relay fleet"
)]
pub struct Args {
    /// Loopback bind for the HTTP read API. Empty string disables the
    /// listener (the crate still maintains subscriptions but serves no
    /// queries — useful for soak testing the ingest path).
    #[arg(long, env = "RELAY_CACHE_BIND", default_value = "127.0.0.1:8089")]
    pub bind: String,

    /// Systemd unit directory, walked once at startup to discover the
    /// `relay-bc<N>` fleet. Defaults to the system path; the relay tests
    /// override this to a tmpdir.
    #[arg(
        long,
        env = "RELAY_CACHE_UNIT_DIR",
        default_value = "/etc/systemd/system"
    )]
    pub unit_dir: PathBuf,

    /// `host:port` of any one region frontend, used to fetch the shared
    /// module schema once at startup. All regions serve byte-identical
    /// schemas; the choice is arbitrary. Must be a frontend port
    /// (`3000+region`), not the public `:443` health site.
    #[arg(
        long,
        env = "RELAY_CACHE_SCHEMA_HOST",
        default_value = "127.0.0.1:3014"
    )]
    pub schema_host: String,

    /// Database name on the schema host. Defaults to the production
    /// mirror; override for staging.
    #[arg(
        long,
        env = "RELAY_CACHE_SCHEMA_DB",
        default_value = "relay-mirror-bc14"
    )]
    pub schema_db: String,

    /// Soft memory ceiling in bytes. On approach: log at `warn`, flip the
    /// `/cache-health` ready flag to false, keep serving with whatever data we
    /// have. Never a load shedder — see the README "Memory policy" section.
    #[arg(
        long,
        env = "RELAY_CACHE_MEM_CEILING_BYTES",
        default_value_t = 4 * 1024 * 1024 * 1024
    )]
    pub mem_ceiling_bytes: u64,
}
