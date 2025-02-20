// Copyright (c) 2023-2025 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use crate::api::index::{fieldname_typoid, FieldName};
use crate::api::operator::{attname_from_var, searchqueryinput_typoid};
use crate::nodecast;
use crate::postgres::customscan::operator_oid;
use crate::postgres::customscan::pdbscan::qual_inspect::Qual;
use crate::schema::{SearchFieldName, SearchIndexSchema};
use pgrx::{direct_function_call, pg_sys, IntoDatum, PgList};
use rustc_hash::FxHashMap;
use std::sync::OnceLock;

macro_rules! pushdown {
    ($attname:expr, $opexpr:expr, $operator:expr, $rhs:ident) => {
        let funcexpr = make_opexpr($attname, $opexpr, $operator, $rhs);

        if !is_complex(funcexpr.cast()) {
            return Some(Qual::PushdownExpr { funcexpr });
        } else {
            return Some(Qual::Expr {
                node: funcexpr.cast(),
                expr_state: std::ptr::null_mut(),
            });
        }
    };
}

type PostgresOperatorOid = pg_sys::Oid;
type TantivyOperator = &'static str;

unsafe fn initialize_equality_operator_lookup() -> FxHashMap<PostgresOperatorOid, TantivyOperator> {
    const OPERATORS: [&str; 6] = ["=", ">", "<", ">=", "<=", "<>"];
    const TYPE_PAIRS: &[[&str; 2]] = &[
        // integers
        ["int2", "int2"],
        ["int4", "int4"],
        ["int8", "int8"],
        ["int2", "int4"],
        ["int2", "int8"],
        ["int4", "int8"],
        // floats
        ["float4", "float4"],
        ["float8", "float8"],
        ["float4", "float8"],
        // dates
        ["date", "date"],
        ["time", "time"],
        ["timetz", "timetz"],
        ["timestamp", "timestamp"],
        ["timestamptz", "timestamptz"],
        // text
        ["text", "text"],
        ["uuid", "uuid"],
    ];

    let mut lookup = FxHashMap::default();

    // tantivy doesn't support range operators on bools, so we can only support the equality operator
    lookup.insert(operator_oid("=(bool,bool)"), "=");

    for o in OPERATORS {
        for [l, r] in TYPE_PAIRS {
            lookup.insert(operator_oid(&format!("{o}({l},{r})")), o);
            if l != r {
                // types can be reversed too
                lookup.insert(operator_oid(&format!("{o}({r},{l})")), o);
            }
        }
    }

    lookup
}

/// Take a Postgres [`pg_sys::OpExpr`] pointer that is **not** of our `@@@` operator and try  to
/// convert it into one that is.
///
/// Returns `Some(Qual)` if we were able to convert it, `None` if not.
#[rustfmt::skip]
pub unsafe fn try_pushdown(
    root: *mut pg_sys::PlannerInfo,
    opexpr: *mut pg_sys::OpExpr,
    schema: &SearchIndexSchema
) -> Option<Qual> {
    let args = PgList::<pg_sys::Node>::from_pg((*opexpr).args);
    let var = nodecast!(Var, T_Var, args.get_ptr(0)?)?;
    let rhs = args.get_ptr(1)?;

    let (typeoid, attname) = attname_from_var(root, var);
    let attname = attname?;

    let search_field = schema.get_search_field(&SearchFieldName(attname.clone()))?;
    
    if search_field.is_text() && !search_field.is_raw() {
        return None;
    }
    

    static EQUALITY_OPERATOR_LOOKUP: OnceLock<FxHashMap<pg_sys::Oid, &str>> = OnceLock::new();
    match EQUALITY_OPERATOR_LOOKUP.get_or_init(|| initialize_equality_operator_lookup()).get(&(*opexpr).opno) {
        Some(pgsearch_operator) => { pushdown!(&attname, opexpr, pgsearch_operator, rhs); },
        None => {
            // TODO:  support other types of OpExprs
            None
        }
    }
}

unsafe fn term_with_operator_procid() -> pg_sys::Oid {
    direct_function_call::<pg_sys::Oid>(
            pg_sys::regprocedurein,
            // NB:  the SQL signature here needs to match our Rust implementation
            &[c"paradedb.term_with_operator(paradedb.fieldname, text, anyelement)".into_datum()],
        )
            .expect("the `paradedb.term_with_operator(paradedb.fieldname, text, anyelement)` function should exist")
}

unsafe fn make_opexpr(
    attname: &str,
    orig_opexor: *mut pg_sys::OpExpr,
    operator: &str,
    value: *mut pg_sys::Node,
) -> *mut pg_sys::FuncExpr {
    let paradedb_funcexpr: *mut pg_sys::FuncExpr =
        pg_sys::palloc0(size_of::<pg_sys::FuncExpr>()).cast();
    (*paradedb_funcexpr).xpr.type_ = pg_sys::NodeTag::T_FuncExpr;
    (*paradedb_funcexpr).funcid = term_with_operator_procid();
    (*paradedb_funcexpr).funcresulttype = searchqueryinput_typoid();
    (*paradedb_funcexpr).funcretset = false;
    (*paradedb_funcexpr).funcvariadic = false;
    (*paradedb_funcexpr).funcformat = pg_sys::CoercionForm::COERCE_EXPLICIT_CALL;
    (*paradedb_funcexpr).funccollid = pg_sys::InvalidOid;
    (*paradedb_funcexpr).inputcollid = (*orig_opexor).inputcollid;
    (*paradedb_funcexpr).location = (*orig_opexor).location;
    (*paradedb_funcexpr).args = {
        let fieldname = pg_sys::makeConst(
            fieldname_typoid(),
            -1,
            pg_sys::InvalidOid,
            -1,
            FieldName::from(attname).into_datum().unwrap(),
            false,
            false,
        );
        let operator = pg_sys::makeConst(
            pg_sys::TEXTOID,
            -1,
            pg_sys::DEFAULT_COLLATION_OID,
            -1,
            operator.into_datum().unwrap(),
            false,
            false,
        );

        let mut args = PgList::<pg_sys::Node>::new();
        args.push(fieldname.cast());
        args.push(operator.cast());
        args.push(value.cast());

        args.into_pg()
    };

    paradedb_funcexpr
}

pub unsafe fn is_complex(root: *mut pg_sys::Node) -> bool {
    unsafe extern "C" fn walker(node: *mut pg_sys::Node, _: *mut core::ffi::c_void) -> bool {
        nodecast!(Var, T_Var, node).is_some()
            || nodecast!(Param, T_Param, node).is_some()
            || pg_sys::contain_volatile_functions(node)
            || pg_sys::expression_tree_walker(node, Some(walker), std::ptr::null_mut())
    }

    if root.is_null() {
        return false;
    }

    walker(root, std::ptr::null_mut())
}
