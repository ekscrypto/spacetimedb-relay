// SPDX-License-Identifier: MIT

//! Rewrites v1 `TransactionUpdate`s emitted by the local SpacetimeDB so
//! they look like the upstream's original `TransactionUpdate`.
//!
//! The local stdb's v1 broadcast says
//!
//! ```text
//! TransactionUpdate {
//!   caller_identity      = <relay's local-stdb identity>,
//!   caller_connection_id = <relay's local-stdb conn id>,
//!   timestamp            = <when the local reducer ran>,
//!   reducer_call = ReducerCallInfo {
//!     reducer_name = "relay_apply_<table>",
//!     args = BSATN([ Some(UpstreamReducerMeta), deletes, inserts ]),
//!     ...
//!   },
//!   status: Committed(DatabaseUpdate { tables: [...] }),
//!   ...
//! }
//! ```
//!
//! and we rewrite it to
//!
//! ```text
//! TransactionUpdate {
//!   caller_identity      = meta.caller_identity,
//!   caller_connection_id = meta.caller_connection_id,
//!   timestamp            = meta.timestamp,
//!   reducer_call = ReducerCallInfo {
//!     reducer_name = meta.reducer_name,
//!     args         = meta.args,
//!     request_id   = meta.request_id,
//!     reducer_id   = unchanged (we don't have the upstream's id),
//!   },
//!   status: <unchanged: still the rows from upstream>,
//!   ...
//! }
//! ```
//!
//! so a downstream v1 client sees a TransactionUpdate effectively
//! identical to the one the upstream would have sent.
//!
//! The rewrite is a no-op for any other message tag, for non-Committed
//! transactions, and for `reducer_name`s that aren't `relay_apply_*`
//! (e.g. `relay_bind_writer` calls during the proxy's own startup).

use bytes::Bytes;
use relay_protocol::UpstreamReducerMeta;
use spacetimedb_client_api_messages_v1::websocket as v1;
use spacetimedb_lib_v1::bsatn as v1_bsatn;
use spacetimedb_sats::bsatn as sats_bsatn;
use thiserror::Error;

use crate::codec::{self, FrameError};

/// Reducer-name prefix the relay-mirror-driver uses for every per-table
/// apply. We rewrite only frames whose reducer matches this prefix —
/// other reducer calls (e.g. `relay_bind_writer`) on the local stdb are
/// internal and pass through untouched.
const APPLY_PREFIX: &str = "relay_apply_";

#[derive(Debug, Error)]
pub enum RewriteError {
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("v1 ServerMessage decode: {0}")]
    Decode(String),
    #[error("v1 ServerMessage encode: {0}")]
    Encode(String),
}

/// Outcome of [`rewrite_local_to_v1_client`]: either we synthesised a
/// new frame (returned in `Owned`), or we have nothing to rewrite and
/// the caller can forward the original bytes verbatim.
pub enum Rewritten {
    /// Replace the frame with these bytes (compression-byte already
    /// prepended).
    Owned(Vec<u8>),
    /// Forward the original frame bytes unchanged.
    Passthrough,
}

/// Inspect a v1 frame coming from local stdb and rewrite it if it's a
/// `relay_apply_<table>` `TransactionUpdate(Committed)`.
pub fn rewrite_local_to_v1_client(frame: &[u8]) -> Result<Rewritten, RewriteError> {
    let body = codec::body(frame)?;

    let mut msg: v1::ServerMessage<v1::BsatnFormat> =
        v1_bsatn::from_slice(body).map_err(|e| RewriteError::Decode(e.to_string()))?;

    let v1::ServerMessage::TransactionUpdate(ref mut tu) = msg else {
        return Ok(Rewritten::Passthrough);
    };
    if !matches!(tu.status, v1::UpdateStatus::Committed(_)) {
        return Ok(Rewritten::Passthrough);
    }
    if !tu.reducer_call.reducer_name.starts_with(APPLY_PREFIX) {
        return Ok(Rewritten::Passthrough);
    }

    let Some(meta) = extract_upstream_meta(&tu.reducer_call.args)? else {
        // The `relay_apply_<table>` reducer was called with `None`
        // upstream meta — typically only happens for our own writer
        // bind / housekeeping. Pass through.
        return Ok(Rewritten::Passthrough);
    };

    apply_meta(tu, meta);

    let body = v1_bsatn::to_vec(&msg).map_err(|e| RewriteError::Encode(e.to_string()))?;
    Ok(Rewritten::Owned(codec::wrap_uncompressed(body)))
}

fn apply_meta(tu: &mut v1::TransactionUpdate<v1::BsatnFormat>, meta: UpstreamReducerMeta) {
    tu.caller_identity =
        spacetimedb_lib_v1::Identity::from_byte_array(meta.caller_identity.to_byte_array());
    tu.caller_connection_id = spacetimedb_lib_v1::ConnectionId::from_be_byte_array(
        meta.caller_connection_id.as_be_byte_array(),
    );
    tu.timestamp = spacetimedb_lib_v1::Timestamp::from_micros_since_unix_epoch(
        meta.timestamp.to_micros_since_unix_epoch(),
    );
    tu.reducer_call.reducer_name = meta.reducer_name.into_boxed_str();
    tu.reducer_call.args = meta.args.into_boxed_slice();
    tu.reducer_call.request_id = meta.request_id;
}

/// Decode the leading `Option<UpstreamReducerMeta>` from a
/// `relay_apply_<table>` reducer's args. The trailing
/// `Vec<Vec<u8>>` deletes + inserts are left untouched (we don't need
/// them for the rewrite). Returns `None` when the leading `Option` is
/// `None`.
///
/// Wire shape, per `relay-mirror-driver::encode_apply_args`:
/// ```text
/// [u8 0=Some, 1=None]
/// (if Some) BSATN(UpstreamReducerMeta)
/// [u32 deletes_count][per-delete: u32 len, bytes]
/// [u32 inserts_count][per-insert: u32 len, bytes]
/// ```
pub fn extract_upstream_meta(args: &[u8]) -> Result<Option<UpstreamReducerMeta>, RewriteError> {
    let tag = *args.first().ok_or_else(|| {
        RewriteError::Decode("relay_apply args empty (missing Option tag)".into())
    })?;
    match tag {
        0 => {
            let rest = &args[1..];
            let len = meta_byte_len(rest)
                .ok_or_else(|| RewriteError::Decode("UpstreamReducerMeta truncated".into()))?;
            let m: UpstreamReducerMeta = sats_bsatn::from_slice(&rest[..len])
                .map_err(|e| RewriteError::Decode(e.to_string()))?;
            Ok(Some(m))
        }
        1 => Ok(None),
        other => Err(RewriteError::Decode(format!(
            "unexpected Option tag {other}"
        ))),
    }
}

/// Walk the BSATN bytes of an `UpstreamReducerMeta` value to find its
/// total length. Mirrors the layout that `#[derive(SpacetimeType)]`
/// produces for the struct: u32-prefixed `String`, then 32 + 16 + 8 + 4
/// bytes of fixed-size primitives, then a u32-prefixed `Vec<u8>`.
///
/// Returns `None` when the input is too short to contain a valid meta —
/// callers should treat that as a decode error.
fn meta_byte_len(input: &[u8]) -> Option<usize> {
    let name_len = u32::from_le_bytes(input.get(0..4)?.try_into().ok()?) as usize;
    let mut p = 4usize.checked_add(name_len)?;
    p = p.checked_add(32 + 16 + 8 + 4)?;
    let args_len = u32::from_le_bytes(input.get(p..p + 4)?.try_into().ok()?) as usize;
    p = p.checked_add(4)?.checked_add(args_len)?;
    if input.len() < p {
        return None;
    }
    Some(p)
}

/// Convenience for forwarding `Bytes` chunks: returns the rewritten
/// bytes if a rewrite happened, otherwise `original` unchanged. Errors
/// surface so callers can log + drop the frame instead of forwarding
/// garbage.
pub fn rewrite_or_pass_v1(original: Bytes) -> Result<Bytes, RewriteError> {
    match rewrite_local_to_v1_client(&original)? {
        Rewritten::Owned(v) => Ok(Bytes::from(v)),
        Rewritten::Passthrough => Ok(original),
    }
}

/// Build a full v1 [`v1::TransactionUpdate`] frame from a
/// [`v1::TransactionUpdateLight`] + the upstream meta the relay
/// recorded when sending the corresponding `relay_apply_<table>`
/// CallReducer.
///
/// Used by the proxy when local SpacetimeDB (V2) emits TUL on the v1
/// subprotocol — TUL has rows but no caller info, so we construct the
/// full TU shape that v1 SDKs expect.
pub fn synthesize_v1_tu_from_tul(
    tul: v1::TransactionUpdateLight<v1::BsatnFormat>,
    meta: UpstreamReducerMeta,
) -> Vec<u8> {
    let tu = v1::ServerMessage::<v1::BsatnFormat>::TransactionUpdate(v1::TransactionUpdate {
        status: v1::UpdateStatus::Committed(tul.update),
        timestamp: spacetimedb_lib_v1::Timestamp::from_micros_since_unix_epoch(
            meta.timestamp.to_micros_since_unix_epoch(),
        ),
        caller_identity: spacetimedb_lib_v1::Identity::from_byte_array(
            meta.caller_identity.to_byte_array(),
        ),
        caller_connection_id: spacetimedb_lib_v1::ConnectionId::from_be_byte_array(
            meta.caller_connection_id.as_be_byte_array(),
        ),
        reducer_call: v1::ReducerCallInfo {
            reducer_name: meta.reducer_name.into_boxed_str(),
            // We don't know the upstream's reducer_id (it's a numeric
            // id local to the upstream's module). Zero is a safe
            // sentinel — clients keying off `reducer_name` ignore it.
            reducer_id: 0,
            args: meta.args.into_boxed_slice(),
            request_id: meta.request_id,
        },
        // Diagnostics SpacetimeDB clients rarely surface; zeroes are
        // acceptable and consistent with relay-upstream's translation
        // path (v1 TU → v2 TU drops these).
        energy_quanta_used: spacetimedb_client_api_messages_v1::energy::EnergyQuanta { quanta: 0 },
        total_host_execution_duration: spacetimedb_lib_v1::TimeDuration::ZERO,
    });
    let body = v1_bsatn::to_vec(&tu).expect("v1 TU encode infallible");
    codec::wrap_uncompressed(body)
}

#[cfg(test)]
mod synthesis_tests {
    use super::*;
    use spacetimedb_client_api_messages_v1::websocket::{
        BsatnRowList, CompressableQueryUpdate, DatabaseUpdate, QueryUpdate, RowSizeHint,
        TableUpdate, TransactionUpdateLight,
    };

    fn meta() -> UpstreamReducerMeta {
        UpstreamReducerMeta {
            reducer_name: "send_chat".into(),
            caller_identity: relay_protocol::lib::Identity::from_byte_array([0x77; 32]),
            caller_connection_id: relay_protocol::lib::ConnectionId::from_u128(0xABCD),
            timestamp: relay_protocol::lib::Timestamp::from_micros_since_unix_epoch(
                1_700_000_000_000_000,
            ),
            request_id: 555,
            args: vec![1, 2, 3, 4],
        }
    }

    fn tul() -> TransactionUpdateLight<v1::BsatnFormat> {
        TransactionUpdateLight {
            request_id: 11,
            update: DatabaseUpdate {
                tables: vec![TableUpdate {
                    table_id: 0.into(),
                    table_name: "chat_message_state".to_string().into_boxed_str(),
                    num_rows: 1,
                    updates: smallvec::smallvec![CompressableQueryUpdate::Uncompressed(
                        QueryUpdate {
                            deletes: BsatnRowList::new(
                                RowSizeHint::FixedSize(0),
                                Default::default(),
                            ),
                            inserts: BsatnRowList::new(
                                RowSizeHint::RowOffsets(vec![0].into()),
                                bytes::Bytes::from_static(b"hello"),
                            ),
                        }
                    )],
                }],
            },
        }
    }

    #[test]
    fn synthesizes_full_tu_from_tul_plus_meta() {
        let frame = synthesize_v1_tu_from_tul(tul(), meta());
        let body = codec::body(&frame).unwrap();
        let decoded: v1::ServerMessage<v1::BsatnFormat> = v1_bsatn::from_slice(body).unwrap();
        let v1::ServerMessage::TransactionUpdate(tu) = decoded else {
            panic!("expected TransactionUpdate, got something else");
        };
        assert_eq!(tu.reducer_call.reducer_name.as_ref(), "send_chat");
        assert_eq!(tu.reducer_call.request_id, 555);
        assert_eq!(tu.reducer_call.args.as_ref(), &[1, 2, 3, 4]);
        assert_eq!(tu.caller_identity.to_byte_array(), [0x77u8; 32]);
        assert_eq!(
            tu.timestamp.to_micros_since_unix_epoch(),
            1_700_000_000_000_000
        );
        let v1::UpdateStatus::Committed(db) = tu.status else {
            panic!("synthesised TU must be Committed");
        };
        assert_eq!(db.tables.len(), 1);
        assert_eq!(db.tables[0].table_name.as_ref(), "chat_message_state");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes as ApiBytes;
    use relay_protocol::lib::{ConnectionId, Identity, Timestamp};

    fn sample_meta() -> UpstreamReducerMeta {
        UpstreamReducerMeta {
            reducer_name: "send_message".into(),
            caller_identity: Identity::from_byte_array([0x42u8; 32]),
            caller_connection_id: ConnectionId::from_u128(0xCAFE_BABE_DEAD_BEEFu128),
            timestamp: Timestamp::from_micros_since_unix_epoch(1_700_000_000_000_000),
            request_id: 99,
            args: b"hello-args".to_vec(),
        }
    }

    fn build_apply_args(
        meta: Option<&UpstreamReducerMeta>,
        deletes: &[Vec<u8>],
        inserts: &[Vec<u8>],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        match meta {
            Some(m) => {
                buf.push(0);
                buf.extend_from_slice(&sats_bsatn::to_vec(m).expect("meta encode infallible"));
            }
            None => buf.push(1),
        }
        buf.extend_from_slice(&(deletes.len() as u32).to_le_bytes());
        for d in deletes {
            buf.extend_from_slice(&(d.len() as u32).to_le_bytes());
            buf.extend_from_slice(d);
        }
        buf.extend_from_slice(&(inserts.len() as u32).to_le_bytes());
        for i in inserts {
            buf.extend_from_slice(&(i.len() as u32).to_le_bytes());
            buf.extend_from_slice(i);
        }
        buf
    }

    fn build_v1_tu_frame(reducer_name: &str, args: Vec<u8>) -> Vec<u8> {
        let msg = v1::ServerMessage::<v1::BsatnFormat>::TransactionUpdate(v1::TransactionUpdate {
            status: v1::UpdateStatus::Committed(v1::DatabaseUpdate { tables: vec![] }),
            timestamp: spacetimedb_lib_v1::Timestamp::from_micros_since_unix_epoch(1),
            caller_identity: spacetimedb_lib_v1::Identity::from_byte_array([0u8; 32]),
            caller_connection_id: spacetimedb_lib_v1::ConnectionId::from_be_byte_array([0u8; 16]),
            reducer_call: v1::ReducerCallInfo {
                reducer_name: reducer_name.to_string().into_boxed_str(),
                reducer_id: 7,
                args: args.into_boxed_slice(),
                request_id: 0,
            },
            energy_quanta_used: spacetimedb_client_api_messages_v1::energy::EnergyQuanta {
                quanta: 0,
            },
            total_host_execution_duration: spacetimedb_lib_v1::TimeDuration::ZERO,
        });
        let body = v1_bsatn::to_vec(&msg).expect("encode v1 TU");
        codec::wrap_uncompressed(body)
    }

    #[test]
    fn extract_meta_some_round_trip() {
        let meta = sample_meta();
        let args = build_apply_args(Some(&meta), &[vec![0xAA]], &[vec![0xBB, 0xCC]]);
        let extracted = extract_upstream_meta(&args).unwrap().unwrap();
        assert_eq!(extracted.reducer_name, "send_message");
        assert_eq!(extracted.request_id, 99);
        assert_eq!(extracted.args, b"hello-args");
        assert_eq!(extracted.caller_identity.to_byte_array(), [0x42u8; 32]);
    }

    #[test]
    fn extract_meta_none() {
        let args = build_apply_args(None, &[], &[]);
        assert!(extract_upstream_meta(&args).unwrap().is_none());
    }

    #[test]
    fn rewrite_swaps_caller_and_reducer_fields() {
        let meta = sample_meta();
        let args = build_apply_args(Some(&meta), &[], &[]);
        let frame = build_v1_tu_frame("relay_apply_message", args);

        let out = rewrite_local_to_v1_client(&frame).unwrap();
        let bytes = match out {
            Rewritten::Owned(v) => v,
            Rewritten::Passthrough => panic!("expected rewrite"),
        };
        let body = codec::body(&bytes).unwrap();
        let decoded: v1::ServerMessage<v1::BsatnFormat> = v1_bsatn::from_slice(body).unwrap();
        let v1::ServerMessage::TransactionUpdate(tu) = decoded else {
            panic!("expected TransactionUpdate");
        };
        assert_eq!(tu.reducer_call.reducer_name.as_ref(), "send_message");
        assert_eq!(tu.reducer_call.request_id, 99);
        assert_eq!(tu.reducer_call.args.as_ref(), b"hello-args");
        assert_eq!(tu.caller_identity.to_byte_array(), [0x42u8; 32]);
        assert_eq!(
            tu.timestamp.to_micros_since_unix_epoch(),
            1_700_000_000_000_000
        );
    }

    #[test]
    fn rewrite_passthrough_for_non_apply_reducer() {
        let args = vec![0u8]; // would-be Option<None>; never decoded for non-apply
        let frame = build_v1_tu_frame("relay_bind_writer", args);
        let out = rewrite_local_to_v1_client(&frame).unwrap();
        assert!(matches!(out, Rewritten::Passthrough));
    }

    #[test]
    fn rewrite_passthrough_for_none_meta() {
        let args = build_apply_args(None, &[], &[]);
        let frame = build_v1_tu_frame("relay_apply_message", args);
        let out = rewrite_local_to_v1_client(&frame).unwrap();
        assert!(matches!(out, Rewritten::Passthrough));
    }

    #[test]
    fn rewrite_passthrough_for_non_transaction_update() {
        // IdentityToken ≠ TransactionUpdate ⇒ passthrough.
        let msg = v1::ServerMessage::<v1::BsatnFormat>::IdentityToken(v1::IdentityToken {
            identity: spacetimedb_lib_v1::Identity::from_byte_array([0u8; 32]),
            connection_id: spacetimedb_lib_v1::ConnectionId::from_be_byte_array([0u8; 16]),
            token: "tok".to_string().into_boxed_str(),
        });
        let frame = codec::wrap_uncompressed(v1_bsatn::to_vec(&msg).unwrap());
        let out = rewrite_local_to_v1_client(&frame).unwrap();
        assert!(matches!(out, Rewritten::Passthrough));
    }

    #[test]
    fn rewrite_or_pass_returns_original_when_no_change() {
        let frame = build_v1_tu_frame("relay_bind_writer", vec![0u8]);
        let original = ApiBytes::from(frame.clone());
        let out = rewrite_or_pass_v1(original.clone()).unwrap();
        // Same byte content, but Bytes equality is enough.
        assert_eq!(out, ApiBytes::from(frame));
    }
}
