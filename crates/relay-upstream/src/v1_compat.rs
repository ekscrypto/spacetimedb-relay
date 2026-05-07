// SPDX-License-Identifier: MIT

//! v1 → v2 wire-format translation.
//!
//! When the relay connects to a SpacetimeDB instance that still speaks
//! `v1.bsatn.spacetimedb`, BSATN bodies arrive in the v1
//! `ServerMessage` shape. This module decodes them with the v1 type
//! crate (pinned at 1.12.0) and rebuilds the equivalent v2 messages
//! the rest of the relay already consumes.
//!
//! Outbound `Subscribe` is also re-encoded for v1, since v1 used a
//! set-replace `Subscribe { query_strings, request_id }` instead of
//! v2's `QuerySetId`-keyed variant.
//!
//! The v1 → v2 mapping the relay relies on:
//!
//! - `IdentityToken` ↔ `InitialConnection` (rename + field reorder).
//! - `InitialSubscription` → `SubscribeApplied` (synthesise the
//!   request/query-set ids that v1 doesn't carry on this message;
//!   flatten the `DatabaseUpdate` into `QueryRows`).
//! - `TransactionUpdate` (`Committed` only) and `TransactionUpdateLight`
//!   → v2 `TransactionUpdate { query_sets: [QuerySetUpdate] }`.
//!   `Failed` / `OutOfEnergy` reducer outcomes are dropped (the relay
//!   doesn't surface reducer status to downstream).
//! - `SubscriptionError` ↔ `SubscriptionError`.
//!
//! Anything we don't expect — `SubscribeApplied`, `UnsubscribeApplied`,
//! `OneOffQueryResponse`, `ProcedureResult`, the `Multi*` variants — is
//! reported as a decode error rather than silently dropped.

use bytes::{Bytes, BytesMut};

use relay_protocol::api_messages::websocket::common as v2_common;
use relay_protocol::api_messages::websocket::v2;
use relay_protocol::lib::{ConnectionId, Identity, Timestamp};
use relay_protocol::UpstreamReducerMeta;
use spacetimedb_client_api_messages_v1::websocket as v1;

use crate::client::UpstreamError;

const TRANSLATED_QUERY_SET_ID: u32 = 1;
const TRANSLATED_REQUEST_ID: u32 = 1;

/// Decode a v1 ServerMessage and translate it into the v2 shape, also
/// returning any upstream reducer provenance recovered along the way.
///
/// The relay forwards `meta` as the second arg of `relay_apply_<table>`
/// so downstream subscribers can read upstream caller / timestamp /
/// reducer name out of the local TransactionUpdate's `reducer.args`.
/// `meta` is `Some` only for v1 `TransactionUpdate(Committed)` since
/// that's the only variant carrying full caller info; everything else
/// (initial subscription, lightweight transaction updates,
/// subscription errors) returns `None`.
pub fn decode_and_translate(
    bsatn_bytes: &[u8],
) -> Result<(v2::ServerMessage, Option<UpstreamReducerMeta>), UpstreamError> {
    let v1_msg =
        spacetimedb_lib_v1::bsatn::from_slice::<v1::ServerMessage<v1::BsatnFormat>>(bsatn_bytes)
            .map_err(|e| UpstreamError::Decode(format!("v1 ServerMessage: {e}")))?;
    translate_server_message(v1_msg)
}

pub fn encode_subscribe(request_id: u32, queries: &[String]) -> Result<Vec<u8>, UpstreamError> {
    let msg = v1::ClientMessage::<Box<[u8]>>::Subscribe(v1::Subscribe {
        query_strings: queries
            .iter()
            .map(|s| s.clone().into_boxed_str())
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        request_id,
    });
    spacetimedb_lib_v1::bsatn::to_vec(&msg).map_err(|e| UpstreamError::Encode(e.to_string()))
}

/// Encode a v1 `SubscribeMulti` carrying a single query, additively
/// adding it to the connection's subscription set.
pub fn encode_subscribe_multi(
    request_id: u32,
    query_id: u32,
    query: &str,
) -> Result<Vec<u8>, UpstreamError> {
    let msg = v1::ClientMessage::<Box<[u8]>>::SubscribeMulti(v1::SubscribeMulti {
        query_strings: vec![query.to_string().into_boxed_str()].into_boxed_slice(),
        request_id,
        query_id: v1::QueryId::new(query_id),
    });
    spacetimedb_lib_v1::bsatn::to_vec(&msg).map_err(|e| UpstreamError::Encode(e.to_string()))
}

fn translate_server_message(
    msg: v1::ServerMessage<v1::BsatnFormat>,
) -> Result<(v2::ServerMessage, Option<UpstreamReducerMeta>), UpstreamError> {
    match msg {
        v1::ServerMessage::IdentityToken(it) => Ok((
            v2::ServerMessage::InitialConnection(v2::InitialConnection {
                identity: convert_identity(it.identity),
                connection_id: convert_connection_id(it.connection_id),
                token: it.token,
            }),
            None,
        )),
        v1::ServerMessage::InitialSubscription(is) => {
            let tables = single_table_rows_from_database_update(is.database_update);
            Ok((
                v2::ServerMessage::SubscribeApplied(v2::SubscribeApplied {
                    request_id: TRANSLATED_REQUEST_ID,
                    query_set_id: v2_common::QuerySetId::new(TRANSLATED_QUERY_SET_ID),
                    rows: v2::QueryRows {
                        tables: tables.into_boxed_slice(),
                    },
                }),
                None,
            ))
        }
        v1::ServerMessage::TransactionUpdate(tu) => {
            let meta = upstream_meta_from_v1(&tu);
            let database_update = match tu.status {
                v1::UpdateStatus::Committed(d) => d,
                v1::UpdateStatus::Failed(_) | v1::UpdateStatus::OutOfEnergy => {
                    return Ok((
                        v2::ServerMessage::TransactionUpdate(v2::TransactionUpdate {
                            query_sets: Box::new([]),
                        }),
                        None,
                    ));
                }
            };
            Ok((
                v2::ServerMessage::TransactionUpdate(transaction_update_from_database_update(
                    database_update,
                )),
                Some(meta),
            ))
        }
        v1::ServerMessage::TransactionUpdateLight(tul) => Ok((
            v2::ServerMessage::TransactionUpdate(transaction_update_from_database_update(
                tul.update,
            )),
            None,
        )),
        v1::ServerMessage::SubscriptionError(err) => Ok((
            v2::ServerMessage::SubscriptionError(v2::SubscriptionError {
                request_id: err.request_id,
                query_set_id: v2_common::QuerySetId::new(
                    err.query_id.unwrap_or(TRANSLATED_QUERY_SET_ID),
                ),
                error: err.error,
            }),
            None,
        )),
        v1::ServerMessage::SubscribeMultiApplied(sma) => {
            // Same translation as v1 InitialSubscription, but the rows
            // belong to the single query identified by `sma.query_id`.
            // Forward as v2 SubscribeApplied so stdb_mode can apply
            // the rows via its existing path; downstream uses the
            // per-table breakdown rather than the query_id.
            let tables = single_table_rows_from_database_update(sma.update);
            Ok((
                v2::ServerMessage::SubscribeApplied(v2::SubscribeApplied {
                    request_id: sma.request_id,
                    query_set_id: v2_common::QuerySetId::new(sma.query_id.id),
                    rows: v2::QueryRows {
                        tables: tables.into_boxed_slice(),
                    },
                }),
                None,
            ))
        }
        v1::ServerMessage::SubscribeApplied(_)
        | v1::ServerMessage::UnsubscribeApplied(_)
        | v1::ServerMessage::UnsubscribeMultiApplied(_)
        | v1::ServerMessage::OneOffQueryResponse(_)
        | v1::ServerMessage::ProcedureResult(_) => Err(UpstreamError::Decode(format!(
            "unexpected v1 ServerMessage variant: {}",
            v1_variant_name(&msg)
        ))),
    }
}

fn upstream_meta_from_v1(
    tu: &v1::TransactionUpdate<v1::BsatnFormat>,
) -> UpstreamReducerMeta {
    UpstreamReducerMeta {
        reducer_name: tu.reducer_call.reducer_name.to_string(),
        caller_identity: convert_identity(tu.caller_identity),
        caller_connection_id: convert_connection_id(tu.caller_connection_id),
        timestamp: Timestamp::from_micros_since_unix_epoch(
            tu.timestamp.to_micros_since_unix_epoch(),
        ),
        request_id: tu.reducer_call.request_id,
        args: tu.reducer_call.args.to_vec(),
    }
}

fn v1_variant_name(msg: &v1::ServerMessage<v1::BsatnFormat>) -> &'static str {
    match msg {
        v1::ServerMessage::InitialSubscription(_) => "InitialSubscription",
        v1::ServerMessage::TransactionUpdate(_) => "TransactionUpdate",
        v1::ServerMessage::TransactionUpdateLight(_) => "TransactionUpdateLight",
        v1::ServerMessage::IdentityToken(_) => "IdentityToken",
        v1::ServerMessage::OneOffQueryResponse(_) => "OneOffQueryResponse",
        v1::ServerMessage::SubscribeApplied(_) => "SubscribeApplied",
        v1::ServerMessage::UnsubscribeApplied(_) => "UnsubscribeApplied",
        v1::ServerMessage::SubscriptionError(_) => "SubscriptionError",
        v1::ServerMessage::SubscribeMultiApplied(_) => "SubscribeMultiApplied",
        v1::ServerMessage::UnsubscribeMultiApplied(_) => "UnsubscribeMultiApplied",
        v1::ServerMessage::ProcedureResult(_) => "ProcedureResult",
    }
}

fn convert_identity(v1_id: spacetimedb_lib_v1::Identity) -> Identity {
    Identity::from_byte_array(v1_id.to_byte_array())
}

fn convert_connection_id(v1_cid: spacetimedb_lib_v1::ConnectionId) -> ConnectionId {
    ConnectionId::from_u128(v1_cid.to_u128())
}

fn single_table_rows_from_database_update(
    db: v1::DatabaseUpdate<v1::BsatnFormat>,
) -> Vec<v2::SingleTableRows> {
    db.tables
        .into_iter()
        .map(|t| {
            let inserts =
                merge_inserts_for_initial_subscription(t.updates.into_iter().collect::<Vec<_>>());
            v2::SingleTableRows {
                table: String::from(t.table_name).into(),
                rows: inserts,
            }
        })
        .collect()
}

fn transaction_update_from_database_update(
    db: v1::DatabaseUpdate<v1::BsatnFormat>,
) -> v2::TransactionUpdate {
    let tables: Vec<v2::TableUpdate> = db
        .tables
        .into_iter()
        .map(|t| {
            let (inserts, deletes) =
                merge_inserts_and_deletes(t.updates.into_iter().collect::<Vec<_>>());
            v2::TableUpdate {
                table_name: String::from(t.table_name).into(),
                rows: Box::new([v2::TableUpdateRows::PersistentTable(
                    v2::PersistentTableRows { inserts, deletes },
                )]),
            }
        })
        .collect();
    v2::TransactionUpdate {
        query_sets: Box::new([v2::QuerySetUpdate {
            query_set_id: v2_common::QuerySetId::new(TRANSLATED_QUERY_SET_ID),
            tables: tables.into_boxed_slice(),
        }]),
    }
}

fn merge_inserts_for_initial_subscription(
    updates: Vec<v1::CompressableQueryUpdate<v1::BsatnFormat>>,
) -> v2_common::BsatnRowList {
    let mut all_inserts: Vec<v1::BsatnRowList> = Vec::with_capacity(updates.len());
    for u in updates {
        if let Some(qu) = uncompressed_query_update(u) {
            all_inserts.push(qu.inserts);
        }
    }
    merge_v1_lists(&all_inserts)
}

fn merge_inserts_and_deletes(
    updates: Vec<v1::CompressableQueryUpdate<v1::BsatnFormat>>,
) -> (v2_common::BsatnRowList, v2_common::BsatnRowList) {
    let mut all_inserts: Vec<v1::BsatnRowList> = Vec::with_capacity(updates.len());
    let mut all_deletes: Vec<v1::BsatnRowList> = Vec::with_capacity(updates.len());
    for u in updates {
        if let Some(qu) = uncompressed_query_update(u) {
            all_inserts.push(qu.inserts);
            all_deletes.push(qu.deletes);
        }
    }
    (merge_v1_lists(&all_inserts), merge_v1_lists(&all_deletes))
}

fn uncompressed_query_update(
    u: v1::CompressableQueryUpdate<v1::BsatnFormat>,
) -> Option<v1::QueryUpdate<v1::BsatnFormat>> {
    match u {
        v1::CompressableQueryUpdate::Uncompressed(qu) => Some(qu),
        v1::CompressableQueryUpdate::Brotli(_) | v1::CompressableQueryUpdate::Gzip(_) => {
            tracing::warn!(
                target: "relay::upstream::v1",
                "dropped per-table compressed update; relay only handles Uncompressed (request `?compression=None`)"
            );
            None
        }
    }
}

/// Concatenate several v1 [`BsatnRowList`]s into a single v2 row list.
///
/// We always produce a `RowOffsets`-form list. That's wasteful when
/// every input was `FixedSize` with the same width — but the relay's
/// row decoders read both shapes equivalently, and the simplification
/// saves an entire branch of subtle merging logic that would only
/// matter for a marginal byte saving on a deprecated protocol.
fn merge_v1_lists(lists: &[v1::BsatnRowList]) -> v2_common::BsatnRowList {
    use v1::RowListLen;
    use v1::RowSizeHint as V1Hint;
    use v2_common::RowSizeHint as V2Hint;

    let mut data = BytesMut::new();
    let mut offsets: Vec<u64> = Vec::new();
    for list in lists {
        let n = list.len();
        for i in 0..n {
            let row = match list.get(i) {
                Some(r) => r,
                None => continue,
            };
            offsets.push(data.len() as u64);
            data.extend_from_slice(&row);
        }
        // Touch the variant explicitly so the compiler tells us if a
        // future v1 release adds another RowSizeHint shape.
        let (hint, _) = list.clone().into_inner();
        match hint {
            V1Hint::FixedSize(_) | V1Hint::RowOffsets(_) => {}
        }
    }
    v2_common::BsatnRowList::new(V2Hint::RowOffsets(offsets.into()), Bytes::from(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use v2_common::RowListLen as _;

    #[test]
    fn identity_token_round_trip_through_v1_decode() {
        let id_bytes = [0xABu8; 32];
        let cid_bytes_be = 0xCAFEBABE_DEADBEEF_CAFEBABE_DEADBEEFu128.to_be_bytes();
        let v1_msg = v1::ServerMessage::<v1::BsatnFormat>::IdentityToken(v1::IdentityToken {
            identity: spacetimedb_lib_v1::Identity::from_byte_array(id_bytes),
            connection_id: spacetimedb_lib_v1::ConnectionId::from_be_byte_array(cid_bytes_be),
            token: "tok-abc".to_string().into_boxed_str(),
        });
        let bsatn = spacetimedb_lib_v1::bsatn::to_vec(&v1_msg).unwrap();

        let (translated, meta) = decode_and_translate(&bsatn).unwrap();
        assert!(meta.is_none(), "IdentityToken does not carry reducer meta");
        match translated {
            v2::ServerMessage::InitialConnection(ic) => {
                assert_eq!(ic.identity.to_byte_array(), id_bytes);
                assert_eq!(ic.connection_id.as_be_byte_array(), cid_bytes_be);
                assert_eq!(&*ic.token, "tok-abc");
            }
            other => panic!("expected InitialConnection, got {other:?}"),
        }
    }

    #[test]
    fn transaction_update_carries_caller_meta() {
        // Build a v1 TransactionUpdate with all caller fields populated;
        // verify the translated v2 message comes paired with an
        // UpstreamReducerMeta whose fields match.
        let id_bytes = [0x11u8; 32];
        let cid_be = 0x1234567890ABCDEF_FEEDFACECAFEBEEFu128.to_be_bytes();
        let v1_msg = v1::ServerMessage::<v1::BsatnFormat>::TransactionUpdate(
            v1::TransactionUpdate {
                status: v1::UpdateStatus::Committed(v1::DatabaseUpdate { tables: vec![] }),
                timestamp: spacetimedb_lib_v1::Timestamp::from_micros_since_unix_epoch(
                    1_700_000_000_000_000,
                ),
                caller_identity: spacetimedb_lib_v1::Identity::from_byte_array(id_bytes),
                caller_connection_id: spacetimedb_lib_v1::ConnectionId::from_be_byte_array(cid_be),
                reducer_call: v1::ReducerCallInfo {
                    reducer_name: "send_message".to_string().into_boxed_str(),
                    reducer_id: 7,
                    args: vec![1, 2, 3].into_boxed_slice(),
                    request_id: 42,
                },
                energy_quanta_used: spacetimedb_client_api_messages_v1::energy::EnergyQuanta {
                    quanta: 0,
                },
                total_host_execution_duration: spacetimedb_lib_v1::TimeDuration::ZERO,
            },
        );
        let bsatn = spacetimedb_lib_v1::bsatn::to_vec(&v1_msg).unwrap();
        let (msg, meta) = decode_and_translate(&bsatn).unwrap();
        assert!(matches!(msg, v2::ServerMessage::TransactionUpdate(_)));
        let meta = meta.expect("Committed v1 TransactionUpdate must carry meta");
        assert_eq!(meta.reducer_name, "send_message");
        assert_eq!(meta.request_id, 42);
        assert_eq!(meta.args, vec![1, 2, 3]);
        assert_eq!(meta.caller_identity.to_byte_array(), id_bytes);
        assert_eq!(meta.caller_connection_id.as_be_byte_array(), cid_be);
        assert_eq!(meta.timestamp.to_micros_since_unix_epoch(), 1_700_000_000_000_000);
    }

    #[test]
    fn merge_v1_lists_concatenates_two_fixed_size_lists() {
        let l1 = v1::BsatnRowList::new(
            v1::RowSizeHint::FixedSize(4),
            Bytes::from_static(&[1, 2, 3, 4, 5, 6, 7, 8]),
        );
        let l2 = v1::BsatnRowList::new(
            v1::RowSizeHint::FixedSize(4),
            Bytes::from_static(&[9, 10, 11, 12]),
        );
        let merged = merge_v1_lists(&[l1, l2]);
        assert_eq!(merged.len(), 3);
        let row0 = merged.get(0).unwrap();
        let row1 = merged.get(1).unwrap();
        let row2 = merged.get(2).unwrap();
        assert_eq!(&row0[..], &[1, 2, 3, 4]);
        assert_eq!(&row1[..], &[5, 6, 7, 8]);
        assert_eq!(&row2[..], &[9, 10, 11, 12]);
    }

    #[test]
    fn unexpected_v1_variant_is_an_error() {
        // SubscribeMultiApplied is not on the relay's expected path
        // because the relay only sends set-replace `Subscribe`. If a
        // server sent it anyway we'd want a loud error, not a silent drop.
        let v1_msg = v1::ServerMessage::<v1::BsatnFormat>::SubscribeMultiApplied(
            v1::SubscribeMultiApplied {
                request_id: 1,
                total_host_execution_duration_micros: 0,
                query_id: v1::QueryId::new(1),
                update: v1::DatabaseUpdate { tables: vec![] },
            },
        );
        let bsatn = spacetimedb_lib_v1::bsatn::to_vec(&v1_msg).unwrap();
        let err = decode_and_translate(&bsatn).expect_err("must reject unexpected variants");
        assert!(matches!(err, UpstreamError::Decode(_)));
    }
}


