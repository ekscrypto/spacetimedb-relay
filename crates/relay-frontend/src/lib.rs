// SPDX-License-Identifier: MIT

//! Public-facing WebSocket proxy that fronts the local SpacetimeDB.
//!
//! Downstream clients connect here (not directly to local stdb). The
//! frontend pairs each connection with a fresh ws to local stdb,
//! tracks per-client metrics, and — for `v1.bsatn.spacetimedb` clients —
//! rewrites local-stdb v1 `TransactionUpdate`s so they look like the
//! original upstream's TransactionUpdate (reducer name, args, caller
//! identity, timestamp all lifted out of `relay_apply_<table>`'s args).
//!
//! For `v2.bsatn.spacetimedb` clients the proxy is pure passthrough plus
//! metrics — v2's wire strips reducer info from broadcasts, so there is
//! nothing to inject.

pub mod codec;
pub mod metrics;
pub mod rewrite;
pub mod state;

mod client;
mod http;
mod listener;

pub use listener::{run, Config};
pub use metrics::{ClientSnapshot, ClientStats, FrontendMetrics, FrontendSnapshot};
pub use state::{ActiveClients, ClientHandle, ClientId};

/// WebSocket subprotocol the proxy is willing to negotiate with both
/// downstream clients and the local SpacetimeDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Subprotocol {
    V1,
    V2,
}

impl Subprotocol {
    pub const V1_NAME: &'static str = "v1.bsatn.spacetimedb";
    pub const V2_NAME: &'static str = "v2.bsatn.spacetimedb";

    pub fn name(self) -> &'static str {
        match self {
            Subprotocol::V1 => Self::V1_NAME,
            Subprotocol::V2 => Self::V2_NAME,
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            Self::V1_NAME => Some(Subprotocol::V1),
            Self::V2_NAME => Some(Subprotocol::V2),
            _ => None,
        }
    }
}
