#!/usr/bin/env python3
"""Unit tests for tools/codegen.py.

Run: python3 tools/test_codegen.py
     (or: python3 -m unittest tools.test_codegen)

These guard the generated mirror-module reducers against regressions
that have historically caused production incidents — in particular the
relay_apply_<table> panic on "row not found" (errno 15) that killed
mirror modules during resubscribe.
"""

import os
import sys
import unittest

# Make codegen.py importable regardless of cwd.
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import codegen  # noqa: E402


def _typespace_with_product(elements: list) -> dict:
    """Build a typespace dict carrying one Product type at index 0.

    Mirrors the shape SpacetimeDB returns: {"types": [ {type}, ... ]}.
    Tables reference their row layout via product_type_ref = <index>.
    """
    return {"types": [{"Product": {"elements": elements}}]}


def _table(name: str, elements: list, pk_cols: list[int], product_ref: int = 0) -> dict:
    """A minimal public table with the given row elements and PK columns."""
    return {
        "name": name,
        "product_type_ref": product_ref,
        "primary_key": pk_cols,
        "indexes": [],
        "constraints": [],
        "sequences": [],
        "schedule": None,
        "table_type": "User",
        "table_access": {"Public": None},
    }


class ApplyReducerToleranceTests(unittest.TestCase):
    """The relay_apply_<table> reducer must tolerate out-of-order updates.

    Background: .update() on a unique PK column panics (errno 15 "row
    was not found") when the row is absent from the local mirror.
    During resubscribe after a republish, upstream can stream an update
    for a row whose insert hasn't landed yet, which used to kill the
    entire module and loop the self-healing republish path. The reducer
    must use delete+insert (idempotent whether or not the row exists)
    instead of update().

    The same idempotence requirement applies to the insert arm: a bare
    insert() panics (errno 12 "Value with given unique identifier
    already exists") when the row is already present locally. This
    arises during resubscribe (upstream replays inserts for rows the
    mirror still holds) and when an update for one entity arrives split
    across TableUpdates with no upstream delete. Every insert — paired
    or not — must therefore delete()+insert().
    """

    def _generate(self) -> str:
        elements = [
            {"name": {"some": "id"}, "algebraic_type": {"U64": []}},
            {"name": {"some": "label"}, "algebraic_type": {"String": []}},
        ]
        schema = {
            "typespace": _typespace_with_product(elements),
            "tables": [_table("widget", elements, pk_cols=[0])],
            "reducers": [],
            "types": [],
            "misc_exports": [],
            "row_level_security": [],
        }
        return codegen.Codegen(schema).run()

    def _apply_rows_helper_body(self, src: str) -> str:
        start = src.index("fn apply_widget_rows")
        # helper body ends at the line starting with "}" at col 0.
        end = src.index("\n}\n", start) + 3
        return src[start:end]

    def _apply_reducer_body(self, src: str) -> str:
        start = src.index("pub fn relay_apply_widget")
        end = src.index("\n}\n", start) + 3
        return src[start:end]

    def test_apply_reducer_does_not_call_update(self):
        """No .update() in the apply helper — it panics on missing rows."""
        body = self._apply_rows_helper_body(self._generate())
        self.assertNotIn(".update(", body, "apply_*_rows must not use .update()")

    def test_apply_reducer_insert_is_always_delete_plus_insert(self):
        """Every insert must be delete()+insert() — there is a single,
        unconditional insert path covering both paired and unpaired
        rows. This is panic-proof for any PK type: delete() returns
        bool and never panics whether or not the row existed, and
        insert() then sees a free slot (no errno 12 duplicate)."""
        body = self._apply_rows_helper_body(self._generate())
        # The insert path: delete immediately followed by insert.
        self.assertIn(
            "ctx.db.widget().id().delete(&new.id);\n"
            "        ctx.db.widget().insert(new);",
            body,
        )
        # No bare insert() that could panic on a duplicate PK. The only
        # insert(new) call must be the one preceded by the delete.
        insert_count = body.count("ctx.db.widget().insert(new);")
        self.assertEqual(insert_count, 1, "exactly one insert path expected")
        delete_count = body.count("ctx.db.widget().id().delete(&new.id);")
        self.assertEqual(delete_count, 1, "exactly one pre-insert delete expected")

    def test_apply_reducer_unpaired_insert_does_not_bare_insert(self):
        """Regression: the unpaired-insert arm must NOT call insert()
        without a preceding delete(). A bare insert panics with errno 12
        when the row already exists locally — observed killing the module
        ~223x/day on relay-global for ability_state / attack_outcome_state
        / action_bar_state etc., which looped the self-healing republish
        path and surfaced downstream as 'no such reducer'."""
        body = self._apply_rows_helper_body(self._generate())
        # The buggy form was an `else { insert(new) }` with no delete.
        # There must be no insert() reachable without a delete() above it
        # in the same block. Concretely: no `else` insert arm.
        self.assertNotIn("else {", body)
        self.assertNotIn("else{{", body)

    def test_apply_reducer_trailing_delete_is_unguarded(self):
        """Unpaired deletes call .delete() directly — it returns bool and
        cannot panic, so no guard is wanted. Regression guard against
        someone over-correcting by adding a find() (which would break
        on custom/inline PK types lacking FilterableValue)."""
        body = self._apply_rows_helper_body(self._generate())
        self.assertIn(".delete(&old.id);", body)
        self.assertNotIn(".find(", body)

    def test_apply_reducer_delegates_to_rows_helper(self):
        """Per-table relay_apply_* is a thin writer-gated wrapper."""
        body = self._apply_reducer_body(self._generate())
        self.assertIn("apply_widget_rows(ctx, deletes, inserts)", body)


class ReducerNameManglingTests(unittest.TestCase):
    """Reducers must pin their registered name via #[reducer(name = ...)].

    Background: SpacetimeDB applies a SnakeCase naming policy by default
    that mangles a reducer's registered name by inserting an underscore
    between a lowercase letter and a digit (e.g. a fn
    `relay_apply_foo_v4` is registered as `relay_apply_foo_v_4`). The
    relay driver calls `relay_apply_<table>` using the raw, unmangled
    upstream table name, so any table whose name contains a letter+digit
    pair (observed on _v1/_v2/_v3/_v4 versioned tables: inter_module_message_v4,
    deployable_state_v2, etc.) hit "no such reducer" on every call — ~286
    errors/min on relay-global, which kept the module in a death loop.
    Fix: emit an explicit `name = "relay_*_<table>"` on every per-table
    reducer so the canonical name is the raw table name, bypassing the
    SnakeCase policy.
    """

    def _generate_versioned(self) -> str:
        """Generate a module with a table whose name ends in a letter+digit."""
        elements = [
            {"name": {"some": "id"}, "algebraic_type": {"U64": []}},
        ]
        schema = {
            "typespace": _typespace_with_product(elements),
            "tables": [_table("widget_v2", elements, pk_cols=[0])],
            "reducers": [],
            "types": [],
            "misc_exports": [],
            "row_level_security": [],
        }
        return codegen.Codegen(schema).run()

    def test_per_table_reducers_have_explicit_name_attribute(self):
        """Every per-table reducer must carry #[reducer(name = "...")]."""
        src = self._generate_versioned()
        # The fn name is relay_apply_widget_v2; the registered (canonical)
        # name must ALSO be relay_apply_widget_v2 (unmangled), via the
        # explicit name= override.
        self.assertIn(
            '#[spacetimedb::reducer(name = "relay_apply_widget_v2")]',
            src,
        )
        self.assertIn(
            '#[spacetimedb::reducer(name = "relay_insert_widget_v2")]',
            src,
        )
        self.assertIn(
            '#[spacetimedb::reducer(name = "relay_delete_widget_v2")]',
            src,
        )
        self.assertIn(
            '#[spacetimedb::reducer(name = "relay_update_widget_v2")]',
            src,
        )

    def test_explicit_name_uses_raw_table_name_not_mangled(self):
        """The name= value must be the raw table name (widget_v2), not the
        SnakeCase-mangled form (widget_v_2). This is the regression: the
        driver calls relay_apply_widget_v2, so the registered name must be
        exactly that."""
        src = self._generate_versioned()
        self.assertIn('"relay_apply_widget_v2"', src)
        self.assertNotIn('"relay_apply_widget_v_2"', src)
        self.assertNotIn("widget_v_2", src)


class InsertOnlyApplyReducerTests(unittest.TestCase):
    """Tables without a single-column PK still need a relay_apply_*
    reducer.

    Background: the driver calls relay_apply_<table> for EVERY table
    that receives an upstream change (stdb_mode::dispatch_message
    sends an ApplyJob unconditionally). Tables with 0 or composite PK
    columns used to get only relay_insert_* and no apply reducer, so
    any upstream update/delete for them failed with "no such reducer"
    — observed firing ~90x/min on relay-global for blocked_player_state
    and official_translators. The apply reducer for these tables is
    insert-only (no PK index to delete/update by), which is reduced
    fidelity but stops the error storm.
    """

    def _generate_no_pk(self) -> str:
        elements = [
            {"name": {"some": "label"}, "algebraic_type": {"String": []}},
            {"name": {"some": "value"}, "algebraic_type": {"I32": []}},
        ]
        schema = {
            "typespace": _typespace_with_product(elements),
            "tables": [_table("tag", elements, pk_cols=[])],  # no PK
            "reducers": [],
            "types": [],
            "misc_exports": [],
            "row_level_security": [],
        }
        return codegen.Codegen(schema).run()

    def test_no_pk_table_emits_apply_reducer(self):
        """A 0-PK table must still get relay_apply_* so the driver's
        unconditional apply call doesn't hit 'no such reducer'."""
        src = self._generate_no_pk()
        self.assertIn("pub fn relay_apply_tag(", src)

    def test_no_pk_apply_reducer_is_insert_only(self):
        """The insert-only apply helper must insert all inserts and
        drop deletes (no safe delete path without a PK index)."""
        src = self._generate_no_pk()
        start = src.index("fn apply_tag_rows")
        body = src[start:start + 600]
        self.assertIn("deletes: Vec<Vec<u8>>", body)
        self.assertIn("inserts: Vec<Vec<u8>>", body)
        self.assertIn("for b in &inserts", body)
        self.assertIn("ctx.db.tag().insert(r);", body)
        self.assertIn("let _ = deletes;", body)
        self.assertNotIn(".update(", body)
        self.assertNotIn(".delete(&", body)


class ApplyTxReducerTests(unittest.TestCase):
    """Live multi-table TransactionUpdates go through relay_apply_tx."""

    def _generate_two_tables(self) -> str:
        elements_a = [
            {"name": {"some": "id"}, "algebraic_type": {"U64": []}},
        ]
        elements_b = [
            {"name": {"some": "label"}, "algebraic_type": {"String": []}},
        ]
        # Two product types so each table has its own row layout.
        typespace = {
            "types": [
                {"Product": {"elements": elements_a}},
                {"Product": {"elements": elements_b}},
            ]
        }
        schema = {
            "typespace": typespace,
            "tables": [
                _table("sell_order_state", elements_a, pk_cols=[0], product_ref=0),
                _table("closed_listing_state", elements_b, pk_cols=[], product_ref=1),
            ],
            "reducers": [],
            "types": [],
            "misc_exports": [],
            "row_level_security": [],
        }
        return codegen.Codegen(schema).run()

    def test_emits_table_op_struct(self):
        src = self._generate_two_tables()
        self.assertIn("pub struct TableOp {", src)
        self.assertIn("pub table: String,", src)
        self.assertIn("pub deletes: Vec<Vec<u8>>,", src)
        self.assertIn("pub inserts: Vec<Vec<u8>>,", src)

    def test_emits_relay_apply_tx(self):
        src = self._generate_two_tables()
        self.assertIn(
            '#[spacetimedb::reducer(name = "relay_apply_tx")]',
            src,
        )
        self.assertIn("pub fn relay_apply_tx(", src)
        self.assertIn("ops: Vec<TableOp>,", src)

    def test_apply_tx_match_arms_cover_all_tables(self):
        src = self._generate_two_tables()
        start = src.index("pub fn relay_apply_tx")
        body = src[start:]
        self.assertIn(
            '"sell_order_state" => apply_sell_order_state_rows(ctx, op.deletes, op.inserts)?,',
            body,
        )
        self.assertIn(
            '"closed_listing_state" => apply_closed_listing_state_rows(ctx, op.deletes, op.inserts)?,',
            body,
        )
        self.assertIn("relay_apply_tx: unknown table", body)


if __name__ == "__main__":
    unittest.main(verbosity=2)
