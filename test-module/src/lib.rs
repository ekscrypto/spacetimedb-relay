// SPDX-License-Identifier: MIT

use spacetimedb::{reducer, table, Identity, ReducerContext, Table, Timestamp};

#[table(name = user_account, public)]
pub struct UserAccount {
    #[primary_key]
    identity: Identity,
    name: String,
    online: bool,
}

#[table(name = message, public)]
pub struct Message {
    #[primary_key]
    #[auto_inc]
    id: u64,
    sender: Identity,
    room: String,
    body: String,
    sent_at: Timestamp,
}

#[table(name = item, public)]
pub struct Item {
    #[primary_key]
    #[auto_inc]
    id: u64,
    owner: Identity,
    kind: String,
    qty: i32,
    rarity: u8,
}

#[table(name = event_log, public)]
pub struct EventLog {
    #[primary_key]
    #[auto_inc]
    id: u64,
    kind: String,
    payload: String,
    ts: Timestamp,
}

#[reducer(init)]
fn init(_ctx: &ReducerContext) {}

#[reducer(client_connected)]
fn on_connect(ctx: &ReducerContext) {
    if let Some(mut u) = ctx.db.user_account().identity().find(ctx.sender) {
        u.online = true;
        ctx.db.user_account().identity().update(u);
    } else {
        ctx.db.user_account().insert(UserAccount {
            identity: ctx.sender,
            name: String::new(),
            online: true,
        });
    }
}

#[reducer(client_disconnected)]
fn on_disconnect(ctx: &ReducerContext) {
    if let Some(mut u) = ctx.db.user_account().identity().find(ctx.sender) {
        u.online = false;
        ctx.db.user_account().identity().update(u);
    }
}

#[reducer]
pub fn set_name(ctx: &ReducerContext, name: String) {
    if let Some(mut u) = ctx.db.user_account().identity().find(ctx.sender) {
        u.name = name;
        ctx.db.user_account().identity().update(u);
    } else {
        ctx.db.user_account().insert(UserAccount {
            identity: ctx.sender,
            name,
            online: true,
        });
    }
}

#[reducer]
pub fn send_message(ctx: &ReducerContext, room: String, body: String) {
    ctx.db.message().insert(Message {
        id: 0,
        sender: ctx.sender,
        room,
        body,
        sent_at: ctx.timestamp,
    });
}

#[reducer]
pub fn add_item(ctx: &ReducerContext, kind: String, qty: i32, rarity: u8) {
    ctx.db.item().insert(Item {
        id: 0,
        owner: ctx.sender,
        kind,
        qty,
        rarity,
    });
}

#[reducer]
pub fn log_event(ctx: &ReducerContext, kind: String, payload: String) {
    ctx.db.event_log().insert(EventLog {
        id: 0,
        kind,
        payload,
        ts: ctx.timestamp,
    });
}

#[reducer]
pub fn seed_demo_data(ctx: &ReducerContext) {
    let synthetic = [
        synthetic_identity(0xA1),
        synthetic_identity(0xA2),
        synthetic_identity(0xA3),
        synthetic_identity(0xA4),
    ];
    let names = ["alice", "bob", "carol", "dave"];
    for (id, name) in synthetic.iter().zip(names.iter()) {
        if ctx.db.user_account().identity().find(*id).is_none() {
            ctx.db.user_account().insert(UserAccount {
                identity: *id,
                name: (*name).to_string(),
                online: false,
            });
        }
    }

    let rooms = ["lobby", "trade", "raid", "guild"];
    for sender in synthetic.iter() {
        for (i, room) in rooms.iter().enumerate() {
            for j in 0..3 {
                ctx.db.message().insert(Message {
                    id: 0,
                    sender: *sender,
                    room: (*room).to_string(),
                    body: format!("hello from {} #{}", room, j),
                    sent_at: ctx.timestamp,
                });
                let _ = i;
            }
        }
    }

    let kinds = ["sword", "shield", "potion", "gem"];
    for owner in synthetic.iter() {
        for (i, kind) in kinds.iter().enumerate() {
            ctx.db.item().insert(Item {
                id: 0,
                owner: *owner,
                kind: (*kind).to_string(),
                qty: (i as i32 + 1) * 2,
                rarity: (i as u8) % 5,
            });
        }
    }

    for i in 0..10u64 {
        ctx.db.event_log().insert(EventLog {
            id: 0,
            kind: if i % 2 == 0 { "info".to_string() } else { "warn".to_string() },
            payload: format!("event #{i}"),
            ts: ctx.timestamp,
        });
    }
}

#[reducer]
pub fn clear_all(ctx: &ReducerContext) {
    let ids: Vec<u64> = ctx.db.message().iter().map(|m| m.id).collect();
    for id in ids {
        ctx.db.message().id().delete(id);
    }
    let ids: Vec<u64> = ctx.db.item().iter().map(|m| m.id).collect();
    for id in ids {
        ctx.db.item().id().delete(id);
    }
    let ids: Vec<u64> = ctx.db.event_log().iter().map(|m| m.id).collect();
    for id in ids {
        ctx.db.event_log().id().delete(id);
    }
    let ids: Vec<Identity> = ctx.db.user_account().iter().map(|u| u.identity).collect();
    for id in ids {
        ctx.db.user_account().identity().delete(id);
    }
}

fn synthetic_identity(seed: u8) -> Identity {
    let mut bytes = [0u8; 32];
    bytes[0] = 0xC0;
    bytes[1] = 0xDE;
    bytes[31] = seed;
    Identity::from_byte_array(bytes)
}
