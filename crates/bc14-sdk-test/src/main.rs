//! Empirical test: does the SpacetimeDB v1.12.0 SDK's WebSocket layer
//! survive a 250-table subscription on `bitcraft-live-14` where our
//! relay's reimplementation fails at ~90 s with a TCP RST?
//!
//! This binary vendors `crates/sdk/src/{websocket,compression,metrics}.rs`
//! from the v1.12.0 SpacetimeDB tag verbatim (only `pub(crate)` →
//! `pub` and a stubbed metrics module). It uses `WsConnection::connect`
//! directly to subscribe to all 250 public-user tables on BitCraft
//! and reports per-second progress.
//!
//! If this test runs to completion (or wedges/RSTs at the same point as
//! our relay), it tells us whether the wedge is in the SDK pattern
//! itself or in something else our relay does.

mod sdk_compression;
mod sdk_metrics;
mod sdk_websocket;

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt;
use http::Uri;
use spacetimedb_client_api_messages::websocket::{
    BsatnFormat, ByteListLen, ClientMessage, Compression, ServerMessage, Subscribe,
};
use tracing_subscriber::EnvFilter;

use crate::sdk_websocket::{WsConnection, WsParams};

const TABLES: &[&str] = &[
    "a_i_debug_state",
    "ability_custom_desc",
    "ability_state",
    "ability_unlock_desc",
    "achievement_desc",
    "action_bar_state",
    "action_state",
    "active_buff_state",
    "admin_broadcast",
    "alert_desc",
    "alert_state",
    "attached_herds_state",
    "attack_impact_timer_migrated",
    "attack_outcome_state",
    "attack_timer",
    "bank_state",
    "barter_stall_state",
    "biome_desc",
    "buff_desc",
    "buff_type_desc",
    "building_buff_desc",
    "building_claim_desc",
    "building_desc",
    "building_function_type_mapping_desc",
    "building_nickname_state",
    "building_portal_desc",
    "building_repairs_desc",
    "building_state",
    "building_type_desc",
    "buy_order_state",
    "cargo_desc",
    "character_stat_desc",
    "character_stats_state",
    "chat_message_state",
    "claim_local_state",
    "claim_local_supply_security_threshold_state",
    "claim_lowercase_name_state",
    "claim_member_state",
    "claim_recruitment_state",
    "claim_state",
    "claim_tech_desc",
    "claim_tech_state",
    "claim_tile_cost",
    "claim_tile_state",
    "climb_requirement_desc",
    "closed_listing_state",
    "clothing_desc",
    "collectible_desc",
    "combat_action_desc",
    "combat_action_multi_hit_desc",
    "combat_dimension_state",
    "combat_immunity_state",
    "combat_state",
    "construction_recipe_desc",
    "contribution_loot_desc",
    "contribution_state",
    "crafting_recipe_desc",
    "crumb_trail_contribution_lock_state",
    "crumb_trail_contribution_spent_state",
    "crumb_trail_exposed_state",
    "deconstruction_recipe_desc",
    "deployable_collectible_state",
    "deployable_desc",
    "deployable_state",
    "dimension_description_state",
    "dimension_network_state",
    "distant_visible_entity",
    "distant_visible_entity_desc",
    "dropped_inventory_despawn_timer",
    "dropped_inventory_ownership_timer",
    "dropped_inventory_state",
    "duel_state",
    "dungeon_state",
    "elevator_desc",
    "emote_desc",
    "empire_chunk_state",
    "empire_color_desc",
    "empire_icon_desc",
    "empire_lowercase_name_state",
    "empire_node_siege_state",
    "empire_node_state",
    "empire_notification_desc",
    "empire_player_data_state",
    "empire_rank_desc",
    "empire_rank_state",
    "empire_settlement_state",
    "empire_state",
    "empire_supplies_desc",
    "empire_territory_desc",
    "enemy_ai_params_desc",
    "enemy_desc",
    "enemy_mob_monitor_state",
    "enemy_scaling_desc",
    "enemy_state",
    "environment_debuff_desc",
    "equipment_desc",
    "equipment_preset_knowledge_desc",
    "equipment_preset_state",
    "equipment_state",
    "experience_state",
    "exploration_chunks_state",
    "extract_outcome_state",
    "extract_outcome_state_v1",
    "extraction_recipe_desc",
    "food_desc",
    "footprint_tile_state",
    "gate_desc",
    "global_search_state",
    "globals",
    "growth_state",
    "health_state",
    "herd_state",
    "hexite_exchange_entry_desc",
    "identity_role",
    "inter_module_message",
    "inter_module_message_v2",
    "interior_collapse_trigger_state",
    "interior_environment_desc",
    "interior_instance_desc",
    "interior_network_desc",
    "interior_player_count_state",
    "interior_portal_connections_desc",
    "interior_shape_desc",
    "interior_spawn_desc",
    "inventory_state",
    "item_conversion_recipe_desc",
    "item_desc",
    "item_list_desc",
    "knowledge_achievement_state",
    "knowledge_battle_action_state",
    "knowledge_building_state",
    "knowledge_cargo_state",
    "knowledge_claim_state",
    "knowledge_construction_state",
    "knowledge_craft_state",
    "knowledge_deployable_state",
    "knowledge_enemy_state",
    "knowledge_extract_state",
    "knowledge_item_state",
    "knowledge_lore_state",
    "knowledge_npc_state",
    "knowledge_paving_state",
    "knowledge_pillar_shaping_state",
    "knowledge_resource_placement_state",
    "knowledge_resource_state",
    "knowledge_ruins_state",
    "knowledge_scroll_desc",
    "knowledge_secondary_state",
    "knowledge_stat_modifier_desc",
    "knowledge_vault_state",
    "light_source_state",
    "location_state",
    "loot_chest_desc",
    "loot_chest_state",
    "loot_table_desc",
    "lost_items_state",
    "marketplace_state",
    "mobile_entity_state",
    "mounting_state",
    "npc_desc",
    "npc_state",
    "on_durability_zero_timer",
    "onboarding_state",
    "parameters_desc",
    "passive_craft_state",
    "pathfinding_desc",
    "paved_tile_state",
    "paving_tile_desc",
    "permission_state",
    "pillar_shaping_desc",
    "pillar_shaping_state",
    "player_action_desc",
    "player_action_state",
    "player_housing_customization_state",
    "player_housing_desc",
    "player_housing_evict_player_timer",
    "player_housing_moving_cost_state",
    "player_housing_state",
    "player_lowercase_username_state",
    "player_note_state",
    "player_notification_event",
    "player_prefs_state",
    "player_queue_state",
    "player_region_transfer_event",
    "player_set_name_outcome_event",
    "player_settings_state",
    "player_state",
    "player_use_elevator_timer",
    "player_username_state",
    "player_vote_state",
    "portal_state",
    "premium_item_desc",
    "premium_service_desc",
    "progressive_action_state",
    "project_site_state",
    "prospecting_desc",
    "prospecting_state",
    "public_progressive_action_state",
    "quest_chain_desc",
    "quest_chain_state",
    "quest_drop_desc",
    "quest_stage_desc",
    "region_connection_info",
    "region_control_info",
    "region_population_info",
    "region_sign_in_parameters",
    "rent_state",
    "reserved_name_desc",
    "resource_clump_desc",
    "resource_desc",
    "resource_health_state",
    "resource_placement_recipe_desc",
    "resource_state",
    "satiation_state",
    "secondary_knowledge_desc",
    "sell_order_state",
    "signed_in_player_state",
    "skill_desc",
    "stamina_state",
    "storage_log_state",
    "target_state",
    "targetable_state",
    "targeting_matrix_desc",
    "teleport_item_desc",
    "teleportation_energy_state",
    "terraform_progress_state",
    "terraform_recipe_desc",
    "terrain_chunk_state",
    "the_great_placeholder_table",
    "threat_state",
    "tool_desc",
    "tool_type_desc",
    "toolbar_state",
    "trade_order_state",
    "trade_session_state",
    "traveler_task_desc",
    "traveler_task_knowledge_requirement_desc",
    "traveler_task_loop_timer",
    "traveler_task_state",
    "traveler_trade_order_desc",
    "user_state",
    "vault_state",
    "wall_desc",
    "waystone_state",
    "weapon_desc",
    "weapon_type_desc",
    "wind_dbg_desc",
    "wind_params_desc",
    "world_region_name_state",
    "world_region_state"
];

const HOST: &str = "wss://bitcraft-early-access.spacetimedb.com";
const DATABASE: &str = "bitcraft-live-14";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let token = std::fs::read_to_string(".bitcraft-token")?.trim().to_string();
    let uri: Uri = format!("{HOST}/").parse()?;
    let params = WsParams { compression: Compression::None, light: false, confirmed: None };

    tracing::info!(?HOST, ?DATABASE, n_tables = TABLES.len(), "connecting via SDK WsConnection");
    let t0 = Instant::now();
    let conn = WsConnection::connect(uri, DATABASE, Some(&token), None, params).await?;
    tracing::info!(elapsed_ms = t0.elapsed().as_millis() as u64, "WS connected");

    let rt_handle = tokio::runtime::Handle::current();
    let (_join, mut incoming, outgoing) = conn.spawn_message_loop(&rt_handle);

    // Build & send Subscribe with all 250 queries.
    let queries: Vec<Box<str>> = TABLES
        .iter()
        .map(|t| format!("SELECT * FROM {t}").into_boxed_str())
        .collect();
    let subscribe = ClientMessage::Subscribe(Subscribe {
        query_strings: queries.into_boxed_slice(),
        request_id: 1,
    });
    outgoing
        .unbounded_send(subscribe)
        .map_err(|e| anyhow::anyhow!("subscribe send failed: {e}"))?;
    tracing::info!(n_tables = TABLES.len(), "subscribe sent");

    // Drain incoming messages, log progress every second.
    let started = Instant::now();
    let mut frames = 0u64;
    let mut last_log = started;
    let mut got_initial_subscription = false;
    let mut bytes_in_messages: u64 = 0;

    while let Some(msg) = incoming.next().await {
        frames += 1;
        match &msg {
            ServerMessage::IdentityToken(it) => {
                tracing::info!(
                    identity = %it.identity.to_hex().as_str(),
                    token_len = it.token.len(),
                    "IdentityToken"
                );
            }
            ServerMessage::InitialSubscription(is) => {
                let n_tables = is.database_update.tables.len();
                let total_bytes: u64 = is
                    .database_update
                    .tables
                    .iter()
                    .flat_map(|t| t.updates.iter())
                    .map(|cqu| match cqu {
                        spacetimedb_client_api_messages::websocket::CompressableQueryUpdate::Uncompressed(qu) => {
                            qu.inserts.num_bytes() as u64 + qu.deletes.num_bytes() as u64
                        }
                        _ => 0,
                    })
                    .sum();
                bytes_in_messages += total_bytes;
                got_initial_subscription = true;
                tracing::info!(
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    n_tables,
                    initial_subscription_bytes = total_bytes,
                    "InitialSubscription RECEIVED"
                );
            }
            ServerMessage::TransactionUpdate(_) => {
                tracing::info!(frames, elapsed_ms = started.elapsed().as_millis() as u64, "TransactionUpdate");
            }
            other => {
                let kind = match other { ServerMessage::SubscribeApplied(_)=>"SubscribeApplied", ServerMessage::TransactionUpdateLight(_)=>"TransactionUpdateLight", ServerMessage::OneOffQueryResponse(_)=>"OneOffQueryResponse", ServerMessage::SubscribeMultiApplied(_)=>"SubscribeMultiApplied", ServerMessage::UnsubscribeApplied(_)=>"UnsubscribeApplied", ServerMessage::UnsubscribeMultiApplied(_)=>"UnsubscribeMultiApplied", ServerMessage::SubscriptionError(_)=>"SubscriptionError", _ => "?" }; tracing::info!(kind, "other ServerMessage");
            }
        }
        if last_log.elapsed() > Duration::from_secs(2) {
            tracing::info!(
                frames,
                elapsed_s = started.elapsed().as_secs(),
                got_initial = got_initial_subscription,
                "progress"
            );
            last_log = Instant::now();
        }
        // After we receive InitialSubscription, we have answered the question.
        if got_initial_subscription {
            tracing::info!("SUCCESS: full InitialSubscription decoded; exiting");
            return Ok(());
        }
    }

    tracing::error!("incoming channel closed without InitialSubscription");
    Err(anyhow::anyhow!("connection ended before InitialSubscription"))
}
