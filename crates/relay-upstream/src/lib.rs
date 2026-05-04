// SPDX-License-Identifier: MIT

// `tokio_tungstenite::tungstenite::Error` is a large-ish enum (~120B) which
// clippy 1.93 flags as `result_large_err` everywhere it propagates. The
// canonical fix is `Box<TungsteniteError>`, but threading that through every
// call site for what is already a cold path is gratuitous.
#![allow(clippy::result_large_err)]

//! Upstream SpacetimeDB client.
//!
//! Owns the single WebSocket connection to the upstream server and
//! exposes a stream of decoded `ServerMessage` events. Designed for
//! exactly one instance per (host, database) — never one per
//! downstream client.

mod client;
mod schema;
mod v1_compat;

pub use client::{
    connect_and_run, server_tag_name, Compression, ProtocolVersion, UpstreamCommand,
    UpstreamConfig, UpstreamError, UpstreamEvent, UpstreamFrame,
};
pub use schema::{fetch_schema, SchemaFetchError};
