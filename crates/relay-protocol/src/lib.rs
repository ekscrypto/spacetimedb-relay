// SPDX-License-Identifier: MIT

//! Wire types and BSATN helpers shared across the relay.
//!
//! This crate is pure data — no I/O, no async. It re-exports the
//! upstream `spacetimedb-sats` and `spacetimedb-client-api-messages`
//! types under stable module paths so the rest of the workspace can
//! depend on `relay-protocol` instead of pinning the upstream crate
//! names directly.

pub use spacetimedb_client_api_messages as api_messages;
pub use spacetimedb_lib as lib;
pub use spacetimedb_sats as sats;

pub mod bsatn;
pub mod schema;
pub use bsatn::{decode_row, field_byte_ranges, BsatnError, Cell, DecodedRow};
pub use schema::{
    parse_schema, MirroredField, MirroredSchema, MirroredTable, MirroredType, MirroredVariant,
    SchemaParseError, TableAccess, TableKind,
};

/// Upstream reducer provenance threaded through `relay_apply_<table>`.
///
/// When we receive a v1 `TransactionUpdate` from upstream, the
/// caller_identity / timestamp / reducer name / args are stamped here
/// and forwarded as the second argument to the local mirror's apply
/// reducer. Downstream subscribers reading the local SpacetimeDB then
/// see this struct in `TransactionUpdate.reducer_call.args`, which
/// lets them recover the original upstream provenance.
///
/// The wasm-side mirror crate has a structurally-identical struct
/// emitted by `tools/codegen.py`; both sides BSATN-encode via
/// `SpacetimeType` so the wire layout matches.
///
/// Sentinel values (used when the upstream protocol can't supply
/// the field — e.g. v2 upstream, v1 `TransactionUpdateLight`):
/// * `reducer_name = ""`
/// * `caller_identity = Identity::ZERO`
/// * `caller_connection_id = ConnectionId::ZERO`
/// * `timestamp = Timestamp::UNIX_EPOCH`
/// * `request_id = 0`
/// * `args = []`
///
/// `SubscribeApplied` rows pass `None` (no upstream cause at all).
#[derive(Clone, Debug, sats::SpacetimeType)]
#[sats(crate = spacetimedb_lib)]
pub struct UpstreamReducerMeta {
    pub reducer_name: String,
    pub caller_identity: lib::Identity,
    pub caller_connection_id: lib::ConnectionId,
    pub timestamp: lib::Timestamp,
    pub request_id: u32,
    pub args: Vec<u8>,
}

pub mod tags {
    pub const CLIENT_SUBSCRIBE: u8 = 0x00;
    pub const CLIENT_UNSUBSCRIBE: u8 = 0x01;
    pub const CLIENT_ONE_OFF_QUERY: u8 = 0x02;
    pub const CLIENT_CALL_REDUCER: u8 = 0x03;
    pub const CLIENT_CALL_PROCEDURE: u8 = 0x04;

    pub const SERVER_INITIAL_CONNECTION: u8 = 0x00;
    pub const SERVER_SUBSCRIBE_APPLIED: u8 = 0x01;
    pub const SERVER_UNSUBSCRIBE_APPLIED: u8 = 0x02;
    pub const SERVER_SUBSCRIPTION_ERROR: u8 = 0x03;
    pub const SERVER_TRANSACTION_UPDATE: u8 = 0x04;
    pub const SERVER_ONE_OFF_QUERY_RESULT: u8 = 0x05;
    pub const SERVER_REDUCER_RESULT: u8 = 0x06;
    pub const SERVER_PROCEDURE_RESULT: u8 = 0x07;
}
