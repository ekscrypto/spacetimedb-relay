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

    def _apply_reducer_body(self, src: str) -> str:
        start = src.index("pub fn relay_apply_widget")
        # reducer body ends at the line starting with "}" at col 0.
        end = src.index("\n}\n", start) + 3
        return src[start:end]

    def test_apply_reducer_does_not_call_update(self):
        """No .update() in the apply reducer — it panics on missing rows."""
        body = self._apply_reducer_body(self._generate())
        self.assertNotIn(".update(", body, "relay_apply must not use .update()")

    def test_apply_reducer_insert_is_always_delete_plus_insert(self):
        """Every insert must be delete()+insert() — there is a single,
        unconditional insert path covering both paired and unpaired
        rows. This is panic-proof for any PK type: delete() returns
        bool and never panics whether or not the row existed, and
        insert() then sees a free slot (no errno 12 duplicate)."""
        body = self._apply_reducer_body(self._generate())
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
        body = self._apply_reducer_body(self._generate())
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
        body = self._apply_reducer_body(self._generate())
        self.assertIn(".delete(&old.id);", body)
        self.assertNotIn(".find(", body)


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
        """The insert-only apply reducer must insert all inserts and
        drop deletes (no safe delete path without a PK index)."""
        start = self._generate_no_pk().index("pub fn relay_apply_tag")
        body = self._generate_no_pk()[start:start + 600]
        # accepts the same (upstream, deletes, inserts) signature as PK tables
        self.assertIn("deletes: Vec<Vec<u8>>", body)
        self.assertIn("inserts: Vec<Vec<u8>>", body)
        # inserts everything, drops deletes
        self.assertIn("for b in &inserts", body)
        self.assertIn("ctx.db.tag().insert(r);", body)
        self.assertIn("let _ = (upstream, deletes);", body)
        # no update/delete-by-pk (none possible without PK index)
        self.assertNotIn(".update(", body)
        self.assertNotIn(".delete(&", body)


if __name__ == "__main__":
    unittest.main(verbosity=2)
