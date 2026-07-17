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
//!
//! `v1.json.spacetimedb` clients are also accepted: they send and receive
//! JSON-encoded messages as WebSocket **text** frames, which local stdb
//! services directly. The relay forwards those text frames opaquely (no
//! BSATN rewrite applies) but still enforces the read-only guardrails —
//! `OneOffQuery`, `CallReducer`, and `CallProcedure` are rejected before
//! they reach local stdb, with a JSON reply echoing the caller's
//! `request_id` / `message_id`.

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
///
/// Variants pair a protocol *family* (v1 / v2) with a wire *encoding*
/// (BSATN binary frames / JSON text frames). The relay connects to local
/// stdb using whichever subprotocol the downstream client negotiated, so
/// the encoding is consistent end-to-end; local stdb services both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Subprotocol {
    /// `v1.bsatn.spacetimedb` — binary WS frames, BSATN-encoded.
    /// The relay rewrites local-stdb `TransactionUpdate`s to lift
    /// upstream meta into them (see `rewrite`).
    V1,
    /// `v2.bsatn.spacetimedb` — binary WS frames, BSATN-encoded,
    /// pure passthrough (v2 strips reducer info from broadcasts).
    V2,
    /// `v1.json.spacetimedb` — text WS frames, JSON-encoded. Local
    /// stdb services the JSON directly; the relay forwards text
    /// opaquely but still rejects write-path client messages.
    V1Json,
}

impl Subprotocol {
    pub const V1_NAME: &'static str = "v1.bsatn.spacetimedb";
    pub const V2_NAME: &'static str = "v2.bsatn.spacetimedb";
    pub const V1_JSON_NAME: &'static str = "v1.json.spacetimedb";

    pub fn name(self) -> &'static str {
        match self {
            Subprotocol::V1 => Self::V1_NAME,
            Subprotocol::V2 => Self::V2_NAME,
            Subprotocol::V1Json => Self::V1_JSON_NAME,
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            Self::V1_NAME => Some(Subprotocol::V1),
            Self::V2_NAME => Some(Subprotocol::V2),
            Self::V1_JSON_NAME => Some(Subprotocol::V1Json),
            _ => None,
        }
    }

    /// Whether this subprotocol carries BSATN in binary WS frames.
    /// The BSATN paths (tag-based message inspection, v1
    /// `TransactionUpdate` rewrite) only apply when this is true;
    /// `V1Json` sends text frames and takes the passthrough path.
    pub fn is_bsatn(self) -> bool {
        match self {
            Subprotocol::V1 | Subprotocol::V2 => true,
            Subprotocol::V1Json => false,
        }
    }

    /// Preference rank used by the subprotocol negotiator when a client
    /// offers several: higher wins. V2 (bsatn) > V1 (bsatn) > V1Json —
    /// BSATN is more compact and avoids JSON (de)serialization, so when
    /// a client offers both we prefer the binary encoding.
    pub fn rank(self) -> u8 {
        match self {
            Subprotocol::V2 => 3,
            Subprotocol::V1 => 2,
            Subprotocol::V1Json => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subprotocol_name_round_trip() {
        for sp in [Subprotocol::V1, Subprotocol::V2, Subprotocol::V1Json] {
            assert_eq!(Subprotocol::from_name(sp.name()), Some(sp));
        }
    }

    #[test]
    fn from_name_rejects_unknown() {
        assert_eq!(Subprotocol::from_name("v3.bsatn.spacetimedb"), None);
        assert_eq!(Subprotocol::from_name("v2.json.spacetimedb"), None);
        assert_eq!(Subprotocol::from_name("nonsense"), None);
    }

    #[test]
    fn from_name_recognizes_v1_json() {
        assert_eq!(
            Subprotocol::from_name("v1.json.spacetimedb"),
            Some(Subprotocol::V1Json)
        );
    }

    #[test]
    fn is_bsatn_only_for_binary_encodings() {
        assert!(Subprotocol::V1.is_bsatn());
        assert!(Subprotocol::V2.is_bsatn());
        assert!(!Subprotocol::V1Json.is_bsatn());
    }

    #[test]
    fn rank_orders_v2_above_v1_above_v1json() {
        // Drives the negotiator's "prefer BSATN when both are offered" rule.
        assert!(Subprotocol::V2.rank() > Subprotocol::V1.rank());
        assert!(Subprotocol::V1.rank() > Subprotocol::V1Json.rank());
    }
}
