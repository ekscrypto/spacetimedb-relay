// SPDX-License-Identifier: MIT

//! Catalog store for `crafting_recipe_desc`.

use hashbrown::HashMap;

use crate::decode::{CraftedItemStack, CraftingRecipeDescRow};

pub struct CraftingRecipeDescStore {
    by_id: HashMap<i32, CraftingRecipeDescEntry>,
}

pub struct CraftingRecipeDescEntry {
    pub actions_required: i32,
    pub crafted_item: Box<[CraftedItemStack]>,
}

impl CraftingRecipeDescStore {
    pub fn new() -> Self {
        Self {
            by_id: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn get(&self, id: i32) -> Option<&CraftingRecipeDescEntry> {
        self.by_id.get(&id)
    }

    pub fn upsert(&mut self, row: CraftingRecipeDescRow) {
        self.by_id.insert(
            row.id,
            CraftingRecipeDescEntry {
                actions_required: row.actions_required,
                crafted_item: row.crafted_item,
            },
        );
    }

    pub fn delete(&mut self, id: i32) {
        self.by_id.remove(&id);
    }
}
