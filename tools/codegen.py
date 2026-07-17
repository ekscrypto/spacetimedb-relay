#!/usr/bin/env python3
"""Generate a SpacetimeDB module crate that mirrors a foreign schema.

Input:  schema JSON as returned by `/v1/database/<name>/schema?version=9`.
Output: a Rust source file with one #[table] per public upstream table
        plus a `relay_insert_<table>(row: Vec<u8>)` reducer per table
        that BSATN-decodes and inserts.

Usage:  codegen.py <schema.json> [--only TABLE [TABLE ...]] > lib.rs
"""

import argparse
import json
import sys
from typing import Any

AUTH_SCAFFOLD = """\
/// Singleton table tracking which Identity is allowed to call the
/// `relay_insert_*` reducers. Empty after `init`; populated by the
/// first caller of `relay_bind_writer` (the relay process on its
/// first WS connection after publish).
#[spacetimedb::table(name = "_relay_meta", accessor = relay_meta)]
pub struct RelayMetaRow {
    #[primary_key]
    pub id: u8,
    pub writer: Identity,
}

#[spacetimedb::reducer(init)]
fn relay_init(_ctx: &ReducerContext) {
    // Intentionally empty. The writer slot is claimed by the first
    // caller of `relay_bind_writer` — typically the relay's runtime
    // WS connection moments after publish. We avoid auto-binding to
    // `ctx.sender()` here because that's the publishing identity (the
    // spacetime CLI), which the relay process can't easily replay
    // without sharing its login token.
}

/// Idempotent: succeeds if no writer is bound yet, or if the caller
/// already IS the bound writer. Errors otherwise. The relay calls this
/// once at startup as a belt-and-suspenders fallback for cases where
/// `init` ran before the relay's first connection (e.g. cold-started
/// SpacetimeDB host that already had the module published).
#[spacetimedb::reducer]
pub fn relay_bind_writer(ctx: &ReducerContext) -> Result<(), Box<str>> {
    if let Some(existing) = ctx.db.relay_meta().id().find(&0u8) {
        if existing.writer == ctx.sender() {
            return Ok(());
        }
        return Err("relay writer already bound to a different identity".into());
    }
    ctx.db.relay_meta().insert(RelayMetaRow {
        id: 0,
        writer: ctx.sender(),
    });
    Ok(())
}

fn assert_writer(ctx: &ReducerContext) -> Result<(), Box<str>> {
    let m = ctx
        .db
        .relay_meta()
        .id()
        .find(&0u8)
        .ok_or_else(|| Box::<str>::from("relay writer not bound"))?;
    if ctx.sender() != m.writer {
        return Err("unauthorized: only the relay writer may call this reducer".into());
    }
    Ok(())
}

/// Upstream reducer provenance, threaded through every `relay_*_<table>`
/// reducer as the second argument. The relay populates this from the
/// upstream `TransactionUpdate` (or passes `None` for initial subscribe
/// rows). Downstream clients read it back out of
/// `ctx.event.reducer.args.upstream` to recover the original caller /
/// timestamp / reducer name / request_id that triggered the change.
#[derive(spacetimedb::SpacetimeType, Clone, Debug)]
pub struct UpstreamReducerInfo {
    pub reducer_name: String,
    pub caller_identity: spacetimedb::Identity,
    pub caller_connection_id: spacetimedb::ConnectionId,
    pub timestamp: spacetimedb::Timestamp,
    pub request_id: u32,
    pub args: Vec<u8>,
}
"""

RUST_KEYWORDS = {
    "as", "break", "const", "continue", "crate", "else", "enum", "extern",
    "false", "fn", "for", "if", "impl", "in", "let", "loop", "match", "mod",
    "move", "mut", "pub", "ref", "return", "self", "Self", "static", "struct",
    "super", "trait", "true", "type", "unsafe", "use", "where", "while",
    "async", "await", "dyn", "abstract", "become", "box", "do", "final",
    "macro", "override", "priv", "typeof", "unsized", "virtual", "yield",
    "try",
}


def sanitize_ident(name: str) -> str:
    out = []
    for c in name:
        if c.isalnum() or c == "_":
            out.append(c)
        else:
            out.append("_")
    s = "".join(out) or "_"
    if s[0].isdigit():
        s = "_" + s
    return s


def rust_field_name(name: str) -> str:
    s = sanitize_ident(name)
    if s in RUST_KEYWORDS:
        return f"r#{s}"
    return s


def to_pascal(name: str) -> str:
    return "".join(p[:1].upper() + p[1:] for p in sanitize_ident(name).split("_") if p)


class Codegen:
    def __init__(self, schema: dict[str, Any]):
        self.schema = schema
        self.typespace: list[dict[str, Any]] = schema["typespace"]["types"]
        self.tables: list[dict[str, Any]] = schema["tables"]
        # typespace index -> rust type name; emitted at top level.
        self.type_names: dict[int, str] = {}
        # Order in which top-level types must appear (deps-first not needed
        # since Rust forward-resolves, but we still preserve insertion order).
        self.type_emit_order: list[int] = []
        # Source for each top-level type.
        self.type_src: dict[int, str] = {}
        # Synthetic anonymous type counter (inline Product/Sum at field level).
        self.inline_counter = 0
        self.inline_src: list[str] = []
        # Per-table struct + reducer source.
        self.table_src: list[str] = []
        # Track whether a table's primary_key indices reference fields that
        # we successfully mapped (otherwise drop the #[primary_key] attr).
        self.warnings: list[str] = []

    # ------------------------------------------------------------ types ----

    def name_for_typespace(self, idx: int) -> str:
        if idx in self.type_names:
            return self.type_names[idx]
        # Pre-claim the name so cycles terminate.
        name = f"Type{idx}"
        self.type_names[idx] = name
        self.type_emit_order.append(idx)
        self.type_src[idx] = self._emit_typespace(idx, name)
        return name

    def _emit_typespace(self, idx: int, name: str) -> str:
        node = self.typespace[idx]
        tag = next(iter(node.keys()))
        body = node[tag]
        if tag == "Product":
            return self._emit_product(name, body["elements"])
        if tag == "Sum":
            return self._emit_sum(name, body["variants"])
        return f"// TODO typespace[{idx}] kind={tag}\npub struct {name}();\n"

    def _emit_product(self, name: str, elements: list[dict[str, Any]]) -> str:
        # Empty product → unit struct.
        if not elements:
            return (
                f"#[derive(spacetimedb::SpacetimeType, Clone, Debug)]\n"
                f"pub struct {name} {{}}\n"
            )
        used_field_names: set[str] = set()
        lines = [
            "#[derive(spacetimedb::SpacetimeType, Clone, Debug, PartialEq)]",
            f"pub struct {name} {{",
        ]
        for i, e in enumerate(elements):
            raw = (e.get("name") or {}).get("some") or f"f{i}"
            fname = rust_field_name(raw)
            base = fname
            n = 1
            while fname in used_field_names:
                fname = f"{base}_{n}"
                n += 1
            used_field_names.add(fname)
            ty = self._algebraic_to_rust(e["algebraic_type"], ctx=f"{name}_{fname}")
            lines.append(f"    pub {fname}: {ty},")
        lines.append("}\n")
        return "\n".join(lines)

    def _emit_sum(self, name: str, variants: list[dict[str, Any]]) -> str:
        # Optional pattern collapses to Option<T> at the use site, never as
        # a top-level enum — the only way we get here for an Optional is if
        # it was reached via Ref, which is rare. Handle it anyway.
        if self._is_optional(variants):
            inner = self._algebraic_to_rust(self._optional_inner(variants), ctx=name)
            return f"pub type {name} = Option<{inner}>;\n"
        used: set[str] = set()
        lines = [
            "#[derive(spacetimedb::SpacetimeType, Clone, Debug, PartialEq)]",
            f"pub enum {name} {{",
        ]
        for i, v in enumerate(variants):
            raw = (v.get("name") or {}).get("some") or f"V{i}"
            vname = to_pascal(raw) or f"V{i}"
            base = vname
            n = 1
            while vname in used:
                vname = f"{base}{n}"
                n += 1
            used.add(vname)
            inner = self._algebraic_to_rust(v["algebraic_type"], ctx=f"{name}_{vname}")
            # Empty product → unit variant; otherwise tuple variant.
            if inner == "()":
                lines.append(f"    {vname},")
            else:
                lines.append(f"    {vname}({inner}),")
        lines.append("}\n")
        return "\n".join(lines)

    @staticmethod
    def _is_optional(variants: list[dict[str, Any]]) -> bool:
        names = [((v.get("name") or {}).get("some") or "") for v in variants]
        return sorted(names) == ["none", "some"]

    @staticmethod
    def _optional_inner(variants: list[dict[str, Any]]) -> dict[str, Any]:
        for v in variants:
            if (v.get("name") or {}).get("some") == "some":
                return v["algebraic_type"]
        raise AssertionError("optional sum without 'some' variant")

    # --------------------------------------------------------- algebraic ---

    def _algebraic_to_rust(self, at: dict[str, Any], ctx: str) -> str:
        tag = next(iter(at.keys()))
        body = at[tag]
        prim = {
            "Bool": "bool", "I8": "i8", "I16": "i16", "I32": "i32",
            "I64": "i64", "I128": "i128", "U8": "u8", "U16": "u16",
            "U32": "u32", "U64": "u64", "U128": "u128",
            "F32": "f32", "F64": "f64", "String": "String",
        }
        if tag in prim:
            return prim[tag]
        if tag == "I256":
            return "spacetimedb::sats::i256"
        if tag == "U256":
            return "spacetimedb::sats::u256"
        if tag == "Ref":
            return self.name_for_typespace(int(body))
        if tag == "Array":
            return f"Vec<{self._algebraic_to_rust(body, ctx + '_item')}>"
        if tag == "Sum":
            variants = body["variants"]
            if self._is_optional(variants):
                inner_at = self._optional_inner(variants)
                inner = self._algebraic_to_rust(inner_at, ctx)
                return f"Option<{inner}>"
            return self._hoist_inline_sum(variants, ctx)
        if tag == "Product":
            elements = body["elements"]
            if not elements:
                return "()"
            return self._hoist_inline_product(elements, ctx)
        self.warnings.append(f"unknown algebraic tag {tag} at {ctx}")
        return "Vec<u8>"

    def _hoist_inline_product(self, elements: list[dict[str, Any]], ctx: str) -> str:
        self.inline_counter += 1
        name = f"Inline{self.inline_counter}_{ctx}"
        # Sanitize hoisted type name.
        name = sanitize_ident(name)
        # Rust types should be PascalCase; force first char upper.
        if name and not name[0].isupper():
            name = name[0].upper() + name[1:]
        self.inline_src.append(self._emit_product(name, elements))
        return name

    def _hoist_inline_sum(self, variants: list[dict[str, Any]], ctx: str) -> str:
        self.inline_counter += 1
        name = f"Inline{self.inline_counter}_{ctx}"
        name = sanitize_ident(name)
        if name and not name[0].isupper():
            name = name[0].upper() + name[1:]
        self.inline_src.append(self._emit_sum(name, variants))
        return name

    # ------------------------------------------------------------ tables ---

    def emit_table(self, tbl: dict[str, Any]) -> str:
        upstream_name = tbl["name"]
        rust_name = sanitize_ident(upstream_name)
        struct_name = to_pascal(upstream_name)
        pt_idx = tbl["product_type_ref"]
        pt = self.typespace[pt_idx]
        if "Product" not in pt:
            self.warnings.append(f"table {upstream_name} product_type_ref={pt_idx} not Product")
            return ""
        elements = pt["Product"]["elements"]
        pk_col_indices: list[int] = list(tbl.get("primary_key") or [])
        used_field_names: set[str] = set()
        pk_field_name: str | None = None

        lines = [
            f"#[spacetimedb::table(name = \"{rust_name}\", accessor = {rust_name}, public)]",
            f"pub struct {struct_name} {{",
        ]
        for i, e in enumerate(elements):
            raw = (e.get("name") or {}).get("some") or f"f{i}"
            fname = rust_field_name(raw)
            base = fname
            n = 1
            while fname in used_field_names:
                fname = f"{base}_{n}"
                n += 1
            used_field_names.add(fname)
            ty = self._algebraic_to_rust(e["algebraic_type"], ctx=f"{struct_name}_{fname}")
            if len(pk_col_indices) == 1 and i == pk_col_indices[0]:
                lines.append("    #[primary_key]")
                pk_field_name = fname
            lines.append(f"    pub {fname}: {ty},")
        lines.append("}\n")

        # The `upstream` arg carries the provenance (caller/timestamp/
        # reducer-name/args) of the upstream TransactionUpdate that
        # triggered this call. The wasm body ignores it — its only job
        # is to ride along in the local TransactionUpdate.reducer_call
        # so downstream subscribers can recover upstream context as
        # `ctx.event.reducer.args.upstream`.
        insert_reducer = "\n".join([
            f"#[spacetimedb::reducer(name = \"relay_insert_{rust_name}\")]",
            f"pub fn relay_insert_{rust_name}(",
            f"    ctx: &ReducerContext,",
            f"    upstream: Option<UpstreamReducerInfo>,",
            f"    row: Vec<u8>,",
            f") -> Result<(), Box<str>> {{",
            f"    let _ = upstream;",
            f"    assert_writer(ctx)?;",
            f"    let r: {struct_name} = spacetimedb::sats::bsatn::from_slice(&row)",
            f"        .map_err(|e| format!(\"bsatn decode {rust_name}: {{e}}\").into_boxed_str())?;",
            f"    ctx.db.{rust_name}().insert(r);",
            f"    Ok(())",
            f"}}\n",
        ])

        delete_update_apply = ""
        if pk_field_name is not None:
            delete_update_apply = "\n".join([
                "",
                f"#[spacetimedb::reducer(name = \"relay_delete_{rust_name}\")]",
                f"pub fn relay_delete_{rust_name}(",
                f"    ctx: &ReducerContext,",
                f"    upstream: Option<UpstreamReducerInfo>,",
                f"    row: Vec<u8>,",
                f") -> Result<(), Box<str>> {{",
                f"    let _ = upstream;",
                f"    assert_writer(ctx)?;",
                f"    let r: {struct_name} = spacetimedb::sats::bsatn::from_slice(&row)",
                f"        .map_err(|e| format!(\"bsatn decode {rust_name} delete: {{e}}\").into_boxed_str())?;",
                f"    ctx.db.{rust_name}().{pk_field_name}().delete(&r.{pk_field_name});",
                f"    Ok(())",
                f"}}",
                "",
                f"#[spacetimedb::reducer(name = \"relay_update_{rust_name}\")]",
                f"pub fn relay_update_{rust_name}(",
                f"    ctx: &ReducerContext,",
                f"    upstream: Option<UpstreamReducerInfo>,",
                f"    old_row: Vec<u8>,",
                f"    new_row: Vec<u8>,",
                f") -> Result<(), Box<str>> {{",
                f"    let _ = upstream;",
                f"    assert_writer(ctx)?;",
                f"    let old: {struct_name} = spacetimedb::sats::bsatn::from_slice(&old_row)",
                f"        .map_err(|e| format!(\"bsatn decode {rust_name} old: {{e}}\").into_boxed_str())?;",
                f"    let new: {struct_name} = spacetimedb::sats::bsatn::from_slice(&new_row)",
                f"        .map_err(|e| format!(\"bsatn decode {rust_name} new: {{e}}\").into_boxed_str())?;",
                f"    ctx.db.{rust_name}().{pk_field_name}().delete(&old.{pk_field_name});",
                f"    ctx.db.{rust_name}().insert(new);",
                f"    Ok(())",
                f"}}",
                "",
                # Batched apply: pairs deletes and inserts by primary key
                # in a single transaction, so downstream subscribers see one
                # atomic notification per upstream TableUpdate. Linear scan
                # over deletes is fine here — TableUpdates are typically
                # small (initial subscribe-applied is pure-insert, no
                # pairing work).
                f"#[spacetimedb::reducer(name = \"relay_apply_{rust_name}\")]",
                f"pub fn relay_apply_{rust_name}(",
                f"    ctx: &ReducerContext,",
                f"    upstream: Option<UpstreamReducerInfo>,",
                f"    deletes: Vec<Vec<u8>>,",
                f"    inserts: Vec<Vec<u8>>,",
                f") -> Result<(), Box<str>> {{",
                f"    let _ = upstream;",
                f"    assert_writer(ctx)?;",
                f"    let mut old_rows: Vec<{struct_name}> = Vec::with_capacity(deletes.len());",
                f"    for b in &deletes {{",
                f"        old_rows.push(",
                f"            spacetimedb::sats::bsatn::from_slice(b)",
                f"                .map_err(|e| format!(\"bsatn decode {rust_name} delete: {{e}}\").into_boxed_str())?,",
                f"        );",
                f"    }}",
                f"    // Track which old rows have been consumed by a paired insert",
                f"    // so the trailing delete loop doesn't re-delete them.",
                f"    let mut consumed = vec![false; old_rows.len()];",
                f"    for b in &inserts {{",
                f"        let new: {struct_name} = spacetimedb::sats::bsatn::from_slice(b)",
                f"            .map_err(|e| format!(\"bsatn decode {rust_name} insert: {{e}}\").into_boxed_str())?;",
                f"        for (i, old) in old_rows.iter().enumerate() {{",
                f"            if consumed[i] {{ continue; }}",
                f"            if old.{pk_field_name} == new.{pk_field_name} {{",
                f"                consumed[i] = true;",
                f"                break;",
                f"            }}",
                f"        }}",
                # delete + insert, not .update(): update() panics
                # (errno 15 "row was not found") when the paired row
                # isn't actually present in the local mirror. A bare
                # insert() on the unpaired path panics (errno 12 "Value
                # with given unique identifier already exists") when the
                # row is already present locally. Both conditions arise
                # during resubscribe after a republish: upstream may
                # replay inserts for rows the mirror still holds, and
                # stream an update for a not-yet-inserted row whose
                # delete hasn't landed. delete() returns bool and never
                # panics, so delete+insert is idempotent whether or not
                # the row existed, for either the paired or unpaired
                # case. Works for every PK type (find() would require a
                # FilterableValue bound that custom/inline PK types lack).
                f"        ctx.db.{rust_name}().{pk_field_name}().delete(&new.{pk_field_name});",
                f"        ctx.db.{rust_name}().insert(new);",
                f"    }}",
                f"    for (i, old) in old_rows.into_iter().enumerate() {{",
                f"        if consumed[i] {{ continue; }}",
                f"        ctx.db.{rust_name}().{pk_field_name}().delete(&old.{pk_field_name});",
                f"    }}",
                f"    Ok(())",
                f"}}\n",
            ])
        else:
            self.warnings.append(
                f"table {upstream_name}: {len(pk_col_indices)} PK columns — emitting insert-only "
                f"(no delete/update reducer; apply is insert-only)"
            )
            # Emit a relay_apply_* even for multi-/no-PK tables: the driver
            # calls relay_apply_<table> for every table that receives an
            # upstream change, and a missing reducer fails with "no such
            # reducer". Without a single-column PK index we can't pair
            # deletes with inserts or safely delete by key, so we insert
            # all inserts and drop deletes (reduced fidelity for these
            # tables, but the module no longer errors). Such tables are
            # typically low-churn descriptors/state.
            delete_update_apply += "\n".join([
                "",
                f"#[spacetimedb::reducer(name = \"relay_apply_{rust_name}\")]",
                f"pub fn relay_apply_{rust_name}(",
                f"    ctx: &ReducerContext,",
                f"    upstream: Option<UpstreamReducerInfo>,",
                f"    deletes: Vec<Vec<u8>>,",
                f"    inserts: Vec<Vec<u8>>,",
                f") -> Result<(), Box<str>> {{",
                f"    let _ = (upstream, deletes);",
                f"    assert_writer(ctx)?;",
                f"    for b in &inserts {{",
                f"        let r: {struct_name} = spacetimedb::sats::bsatn::from_slice(b)",
                f"            .map_err(|e| format!(\"bsatn decode {rust_name} insert: {{e}}\").into_boxed_str())?;",
                f"        ctx.db.{rust_name}().insert(r);",
                f"    }}",
                f"    Ok(())",
                f"}}\n",
            ])

        return "\n".join(lines) + "\n" + insert_reducer + delete_update_apply

    def run(self, only: list[str] | None = None) -> str:
        wanted = set(only) if only else None
        for tbl in self.tables:
            if "Public" not in (tbl.get("table_access") or {}):
                continue
            if wanted is not None and tbl["name"] not in wanted:
                continue
            src = self.emit_table(tbl)
            if src:
                self.table_src.append(src)

        out: list[str] = [
            "// SPDX-License-Identifier: MIT",
            "// Generated by spike/codegen.py — do not edit by hand.",
            "#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals, unused)]",
            "",
            "use spacetimedb::{Identity, ReducerContext, Table};",
            "",
            AUTH_SCAFFOLD,
            "",
        ]
        # Top-level typespace types in emit order.
        for idx in self.type_emit_order:
            out.append(self.type_src[idx])
        # Inline-hoisted anonymous types.
        out.extend(self.inline_src)
        # Tables + reducers.
        out.extend(self.table_src)
        return "\n".join(out)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("schema", help="path to schema JSON")
    p.add_argument("--only", nargs="*", help="restrict to these table names")
    p.add_argument("-o", "--output", help="write to this path instead of stdout")
    args = p.parse_args()
    schema = json.load(open(args.schema))
    cg = Codegen(schema)
    src = cg.run(args.only)
    if args.output:
        with open(args.output, "w") as f:
            f.write(src)
    else:
        sys.stdout.write(src)
    for w in cg.warnings:
        print(f"warn: {w}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
