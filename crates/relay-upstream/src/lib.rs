// SPDX-License-Identifier: MIT

//! Upstream SpacetimeDB client.
//!
//! Owns the single WebSocket connection to the upstream server and
//! exposes a stream of decoded `ServerMessage` events. Designed for
//! exactly one instance per (host, database) — never one per
//! downstream client.

mod client;
mod schema;

pub use client::{
    connect_and_run, server_tag_name, Compression, UpstreamCommand, UpstreamConfig, UpstreamError,
    UpstreamEvent, UpstreamFrame,
};
pub use schema::{fetch_schema, SchemaFetchError};
