// SPDX-License-Identifier: MIT

//! Compiled WHERE-clause representation.
//!
//! `Predicate` is the schema-resolved form of a SQL filter:
//! column refs are resolved to indices into the row's `Cell` vector,
//! literals are validated against the column type, and any
//! schema-relative state is baked in at compile time.
//!
//! Operator coverage:
//!   * **PR2:** `=` only.
//!   * **PR3 (this commit):** `=`, `<>`, `<`, `>`, `<=`, `>=`, plus
//!     `AND` / `OR`. `:sender` is resolved at compile time using the
//!     downstream client's identity.

use std::cmp::Ordering;

use relay_protocol::Cell;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredicateOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Lte,
    Gte,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicOp {
    And,
    Or,
}

#[derive(Debug, Clone)]
pub enum Predicate {
    /// `column[col_idx] OP literal`.
    Cmp {
        col_idx: usize,
        op: PredicateOp,
        literal: Literal,
    },
    /// `lhs <AND|OR> rhs`.
    Logic {
        op: LogicOp,
        lhs: Box<Predicate>,
        rhs: Box<Predicate>,
    },
}

/// A literal already coerced to a representation that pairs naturally
/// with the row `Cell` it will be compared against.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Bool(bool),
    Smallint(i16),
    Integer(i32),
    Bigint(i64),
    Real(f32),
    DoublePrecision(f64),
    /// Generic byte string. Used for Identity / ConnectionId / U128 /
    /// U256 — all "fixed-size opaque blob" columns.
    Bytea(Vec<u8>),
    Text(String),
    /// Decimal `123` or hex `0x...` literal compared against a u64
    /// column. The relay stores u64 as 8 little-endian bytes in
    /// `Cell::Bytea`; ordering is u64-numeric, not byte-lexicographic.
    U64(u64),
}

impl Predicate {
    pub fn matches(&self, cells: &[Cell]) -> bool {
        match self {
            Self::Cmp {
                col_idx,
                op,
                literal,
            } => {
                let Some(cell) = cells.get(*col_idx) else {
                    return false;
                };
                eval_cmp(cell, *op, literal)
            }
            Self::Logic { op, lhs, rhs } => match op {
                LogicOp::And => lhs.matches(cells) && rhs.matches(cells),
                LogicOp::Or => lhs.matches(cells) || rhs.matches(cells),
            },
        }
    }
}

fn eval_cmp(cell: &Cell, op: PredicateOp, lit: &Literal) -> bool {
    let Some(ord) = cell_cmp(cell, lit) else {
        // Mismatched type / null cell / unsupported pair: a non-Eq
        // operator never matches; Eq returns false.
        return false;
    };
    match op {
        PredicateOp::Eq => matches!(ord, Ordering::Equal),
        PredicateOp::Ne => !matches!(ord, Ordering::Equal),
        PredicateOp::Lt => matches!(ord, Ordering::Less),
        PredicateOp::Gt => matches!(ord, Ordering::Greater),
        PredicateOp::Lte => matches!(ord, Ordering::Less | Ordering::Equal),
        PredicateOp::Gte => matches!(ord, Ordering::Greater | Ordering::Equal),
    }
}

fn cell_cmp(cell: &Cell, lit: &Literal) -> Option<Ordering> {
    match (cell, lit) {
        (Cell::Bool(Some(a)), Literal::Bool(b)) => Some(a.cmp(b)),
        (Cell::Smallint(Some(a)), Literal::Smallint(b)) => Some(a.cmp(b)),
        (Cell::Integer(Some(a)), Literal::Integer(b)) => Some(a.cmp(b)),
        (Cell::Bigint(Some(a)), Literal::Bigint(b)) => Some(a.cmp(b)),
        (Cell::Real(Some(a)), Literal::Real(b)) => a.partial_cmp(b),
        (Cell::DoublePrecision(Some(a)), Literal::DoublePrecision(b)) => a.partial_cmp(b),
        (Cell::Text(Some(a)), Literal::Text(b)) => Some(a.as_str().cmp(b.as_str())),
        (Cell::Bytea(Some(a)), Literal::U64(b)) if a.len() == 8 => {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&a[..8]);
            Some(u64::from_le_bytes(buf).cmp(b))
        }
        (Cell::Bytea(Some(a)), Literal::Bytea(b)) => Some(a.as_slice().cmp(b.as_slice())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_protocol::Cell;

    fn pred_eq(col: usize, lit: Literal) -> Predicate {
        Predicate::Cmp {
            col_idx: col,
            op: PredicateOp::Eq,
            literal: lit,
        }
    }

    fn pred_op(col: usize, op: PredicateOp, lit: Literal) -> Predicate {
        Predicate::Cmp {
            col_idx: col,
            op,
            literal: lit,
        }
    }

    #[test]
    fn eq_string() {
        let p = pred_eq(0, Literal::Text("alice".into()));
        assert!(p.matches(&[Cell::Text(Some("alice".into()))]));
        assert!(!p.matches(&[Cell::Text(Some("bob".into()))]));
    }

    #[test]
    fn ne_int() {
        let p = pred_op(0, PredicateOp::Ne, Literal::Integer(7));
        assert!(p.matches(&[Cell::Integer(Some(8))]));
        assert!(!p.matches(&[Cell::Integer(Some(7))]));
    }

    #[test]
    fn lt_gt_int() {
        let lt = pred_op(0, PredicateOp::Lt, Literal::Integer(5));
        let gt = pred_op(0, PredicateOp::Gt, Literal::Integer(5));
        let lte = pred_op(0, PredicateOp::Lte, Literal::Integer(5));
        let gte = pred_op(0, PredicateOp::Gte, Literal::Integer(5));
        assert!(lt.matches(&[Cell::Integer(Some(4))]));
        assert!(!lt.matches(&[Cell::Integer(Some(5))]));
        assert!(gt.matches(&[Cell::Integer(Some(6))]));
        assert!(!gt.matches(&[Cell::Integer(Some(5))]));
        assert!(lte.matches(&[Cell::Integer(Some(5))]));
        assert!(gte.matches(&[Cell::Integer(Some(5))]));
        assert!(!gte.matches(&[Cell::Integer(Some(4))]));
    }

    #[test]
    fn u64_le_bytes_compare_numerically() {
        // 0x0100000000000000 (LE) = 1; 0xFF00000000000000 (LE) = 255.
        // Byte-wise, [0xFF, 0,..] > [0x01, 0,..]. As u64-numeric, also true.
        // But [0x00, 0x01, 0,..] (=256) byte-wise < [0xFF, 0,..] (=255) — so
        // the byte-wise compare gives the wrong answer here. Our code reads
        // back the LE bytes as u64.
        let p = pred_op(0, PredicateOp::Gt, Literal::U64(255));
        let cell_256 = Cell::Bytea(Some(vec![0, 1, 0, 0, 0, 0, 0, 0]));
        assert!(p.matches(std::slice::from_ref(&cell_256)));
        let cell_255 = Cell::Bytea(Some(vec![0xff, 0, 0, 0, 0, 0, 0, 0]));
        assert!(!p.matches(&[cell_255]));
        let p_eq = pred_eq(0, Literal::U64(256));
        assert!(p_eq.matches(&[cell_256]));
    }

    #[test]
    fn and_short_circuit() {
        let p = Predicate::Logic {
            op: LogicOp::And,
            lhs: Box::new(pred_eq(0, Literal::Integer(1))),
            rhs: Box::new(pred_eq(1, Literal::Text("a".into()))),
        };
        assert!(p.matches(&[Cell::Integer(Some(1)), Cell::Text(Some("a".into()))]));
        assert!(!p.matches(&[Cell::Integer(Some(2)), Cell::Text(Some("a".into()))]));
        assert!(!p.matches(&[Cell::Integer(Some(1)), Cell::Text(Some("b".into()))]));
    }

    #[test]
    fn or_either_branch() {
        let p = Predicate::Logic {
            op: LogicOp::Or,
            lhs: Box::new(pred_eq(0, Literal::Integer(1))),
            rhs: Box::new(pred_eq(0, Literal::Integer(2))),
        };
        assert!(p.matches(&[Cell::Integer(Some(1))]));
        assert!(p.matches(&[Cell::Integer(Some(2))]));
        assert!(!p.matches(&[Cell::Integer(Some(3))]));
    }

    #[test]
    fn null_cell_never_matches() {
        let p = pred_eq(0, Literal::Integer(1));
        assert!(!p.matches(&[Cell::Integer(None)]));
    }

    #[test]
    fn missing_column_never_matches() {
        let p = pred_eq(5, Literal::Integer(1));
        assert!(!p.matches(&[Cell::Integer(Some(1))]));
    }
}
