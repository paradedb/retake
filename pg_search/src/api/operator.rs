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

mod searchqueryinput;
mod text;

use crate::api::index::{fieldname_typoid, FieldName};
use crate::index::reader::index::SearchIndexReader;
use crate::index::BlockDirectoryType;
use crate::nodecast;
use crate::postgres::utils::locate_bm25_index;
use crate::query::SearchQueryInput;
use pgrx::callconv::{BoxRet, FcInfo};
use pgrx::datum::Datum;
use pgrx::pg_sys::expression_tree_walker;
use pgrx::pgrx_sql_entity_graph::metadata::{
    ArgumentError, Returns, ReturnsError, SqlMapping, SqlTranslatable,
};
use pgrx::*;
use std::ffi::CStr;
use std::ptr::{addr_of_mut, NonNull};

#[derive(Debug)]
#[repr(transparent)]
pub struct ReturnedNodePointer(pub Option<NonNull<pg_sys::Node>>);

unsafe impl BoxRet for ReturnedNodePointer {
    unsafe fn box_into<'fcx>(self, fcinfo: &mut FcInfo<'fcx>) -> Datum<'fcx> {
        self.0
            .map(|nonnull| {
                let datum = pg_sys::Datum::from(nonnull.as_ptr());
                fcinfo.return_raw_datum(datum)
            })
            .unwrap_or_else(Datum::null)
    }
}

unsafe impl SqlTranslatable for ReturnedNodePointer {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As("internal".into()))
    }

    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As("internal".into())))
    }
}

pub fn parse_with_field_procoid() -> pg_sys::Oid {
    unsafe {
        direct_function_call::<pg_sys::Oid>(
            pg_sys::regprocedurein,
            // NB:  the SQL signature here needs to match our Rust implementation
            &[c"paradedb.parse_with_field(paradedb.fieldname, text, bool, bool)".into_datum()],
        )
        .expect("the `paradedb.parse_with_field(paradedb.fieldname, text, bool, bool)` function should exist")
    }
}

pub fn anyelement_query_input_procoid() -> pg_sys::Oid {
    unsafe {
        direct_function_call::<pg_sys::Oid>(
            pg_sys::regprocedurein,
            &[c"paradedb.search_with_query_input(anyelement, paradedb.searchqueryinput)".into_datum()],
        )
        .expect("the `paradedb.search_with_query_input(anyelement, paradedb.searchqueryinput) function should exist")
    }
}

fn anyelement_text_procoid() -> pg_sys::Oid {
    unsafe {
        direct_function_call::<pg_sys::Oid>(
            pg_sys::regprocedurein,
            &[c"paradedb.search_with_text(anyelement, text)".into_datum()],
        )
        .expect("the `paradedb.search_with_text(anyelement, text) function should exist")
    }
}

fn anyelement_text_opoid() -> pg_sys::Oid {
    unsafe {
        direct_function_call::<pg_sys::Oid>(
            pg_sys::regoperatorin,
            &[c"@@@(anyelement, text)".into_datum()],
        )
        .expect("the `@@@(anyelement, text)` operator should exist")
    }
}

pub fn anyelement_query_input_opoid() -> pg_sys::Oid {
    unsafe {
        direct_function_call::<pg_sys::Oid>(
            pg_sys::regoperatorin,
            &[c"@@@(anyelement, paradedb.searchqueryinput)".into_datum()],
        )
        .expect("the `@@@(anyelement, paradedb.searchqueryinput)` operator should exist")
    }
}

pub fn searchqueryinput_typoid() -> pg_sys::Oid {
    unsafe {
        let oid = direct_function_call::<pg_sys::Oid>(
            pg_sys::regtypein,
            &[c"paradedb.SearchQueryInput".into_datum()],
        )
        .expect("type `paradedb.SearchQueryInput` should exist");
        if oid == pg_sys::Oid::INVALID {
            panic!("type `paradedb.SearchQueryInput` should exist");
        }
        oid
    }
}

pub(crate) fn estimate_selectivity(
    indexrel: &PgRelation,
    search_query_input: &SearchQueryInput,
) -> Option<f64> {
    let reltuples = indexrel
        .heap_relation()
        .expect("indexrel should be an index")
        .reltuples()
        .unwrap_or(1.0) as f64;
    if !reltuples.is_normal() || reltuples.is_sign_negative() {
        // we can't estimate against a non-normal or negative estimate of heap tuples
        return None;
    }

    let search_reader = SearchIndexReader::open(indexrel, BlockDirectoryType::Mvcc, false)
        .expect("estimate_selectivity: should be able to open a SearchIndexReader");
    let estimate = search_reader.estimate_docs(search_query_input).unwrap_or(1) as f64;
    let mut selectivity = estimate / reltuples;
    if selectivity > 1.0 {
        selectivity = 1.0;
    }

    Some(selectivity)
}

unsafe fn make_search_query_input_opexpr_node(
    srs: *mut pg_sys::SupportRequestSimplify,
    input_args: &mut PgList<pg_sys::Node>,
    var: *mut pg_sys::Var,
    query: Option<SearchQueryInput>,
    parse_with_field: Option<(*mut pg_sys::Node, String)>,
    opoid: pg_sys::Oid,
    procoid: pg_sys::Oid,
) -> ReturnedNodePointer {
    let (relid, _varattno, targetlist) = find_var_relation(var, (*srs).root);
    if relid == pg_sys::Oid::INVALID {
        panic!("could not determine relation for var");
    }

    // we need to use what should be the only `USING bm25` index on the table
    let heaprel = PgRelation::open(relid);
    let indexrel = locate_bm25_index(relid).unwrap_or_else(|| {
        panic!(
            "relation `{}.{}` must have a `USING bm25` index",
            heaprel.namespace(),
            heaprel.name()
        )
    });

    let keys = &(*indexrel.rd_index).indkey;
    let keys = keys.values.as_slice(keys.dim1 as usize);
    let tupdesc = PgTupleDesc::from_pg_unchecked(indexrel.rd_att);
    let att = tupdesc
        .get(0)
        .unwrap_or_else(|| panic!("attribute `{}` not found", keys[0]));

    if let Some(targetlist) = &targetlist {
        // if we have a targetlist, find the first field of the index definition in it -- its location
        // in the target list becomes the var's attno
        let mut found = false;
        for (i, te) in targetlist.iter_ptr().enumerate() {
            if te.is_null() {
                continue;
            }
            if (*te).resorigcol == keys[0] {
                (*var).varattno = (i + 1) as _;
                (*var).varattnosyn = (*var).varattno;
                found = true;
                break;
            }
        }

        if !found {
            panic!("index's first column is not in the var's targetlist");
        }
    } else {
        // the Var must look like the first attribute from the index definition
        (*var).varattno = keys[0];
        (*var).varattnosyn = (*var).varattno;
    }

    // the Var must also assume the type of the first attribute from the index definition,
    // regardless of where we found the Var
    (*var).vartype = att.atttypid;
    (*var).vartypmod = att.atttypmod;
    (*var).varcollid = att.attcollation;

    // we're about to fabricate a new pg_sys::OpExpr node to return
    // that represents the `@@@(anyelement, paradedb.searchqueryinput)` operator
    let mut newopexpr = PgBox::<pg_sys::OpExpr>::alloc_node(pg_sys::NodeTag::T_OpExpr);

    if let Some(query) = query {
        // In case a sequential scan gets triggered, we need a way to pass the index oid
        // to the scan function. It otherwise will not know which index to use.
        let wrapped_query = SearchQueryInput::WithIndex {
            oid: indexrel.oid(),
            query: Box::new(query),
        };

        // create a new pg_sys::Const node
        let search_query_input_const = pg_sys::makeConst(
            searchqueryinput_typoid(),
            -1,
            pg_sys::Oid::INVALID,
            -1,
            wrapped_query.into_datum().unwrap(),
            false,
            false,
        );

        // and assign it to the original argument list
        input_args.replace_ptr(1, search_query_input_const.cast());

        newopexpr.opno = anyelement_query_input_opoid();
        newopexpr.opfuncid = anyelement_query_input_procoid();
    } else if let Some((param, attname)) = parse_with_field {
        // rewrite the rhs to be a function call to our `paradedb.parse_with_field(...)` function
        let mut parse_with_field_args = PgList::<pg_sys::Node>::new();

        parse_with_field_args.push(
            pg_sys::makeConst(
                fieldname_typoid(),
                -1,
                pg_sys::Oid::INVALID,
                -1,
                FieldName::from(attname).into_datum().unwrap(),
                false,
                false,
            )
            .cast(),
        );
        parse_with_field_args.push(param.cast());
        parse_with_field_args.push(
            pg_sys::makeConst(
                pg_sys::BOOLOID,
                -1,
                pg_sys::Oid::INVALID,
                size_of::<bool>() as _,
                pg_sys::Datum::from(false),
                false,
                true,
            )
            .cast(),
        );
        parse_with_field_args.push(
            pg_sys::makeConst(
                pg_sys::BOOLOID,
                -1,
                pg_sys::Oid::INVALID,
                size_of::<bool>() as _,
                pg_sys::Datum::from(false),
                false,
                true,
            )
            .cast(),
        );

        let funcexpr = pg_sys::makeFuncExpr(
            parse_with_field_procoid(),
            searchqueryinput_typoid(),
            parse_with_field_args.into_pg(),
            pg_sys::Oid::INVALID,
            pg_sys::DEFAULT_COLLATION_OID,
            pg_sys::CoercionForm::COERCE_EXPLICIT_CALL,
        );

        input_args.replace_ptr(1, funcexpr.cast());
        newopexpr.opno = anyelement_query_input_opoid();
        newopexpr.opfuncid = anyelement_query_input_procoid();
    } else {
        newopexpr.opno = opoid;
        newopexpr.opfuncid = procoid;
    }

    newopexpr.opresulttype = pg_sys::BOOLOID;
    newopexpr.opcollid = pg_sys::Oid::INVALID;
    newopexpr.inputcollid = pg_sys::DEFAULT_COLLATION_OID;
    newopexpr.location = (*(*srs).fcall).location;

    // then assign that list to our new OpExpr node
    newopexpr.args = input_args.as_ptr();

    let newopexpr = newopexpr.into_pg();

    ReturnedNodePointer(NonNull::new(newopexpr.cast()))
}

/// Given a [`pg_sys::Var`] and a [`pg_sys::PlannerInfo`], attempt to find the relation Oid that
/// contains the var.
///
/// It's possible the returned Oid will be [`pg_sys::Oid::INVALID`] if the Var doesn't eventually
/// come from a relation.
///
/// The returned [`pg_sys::AttrNumber`] is the physical attribute number in the relation the Var
/// is from.
pub unsafe fn find_var_relation(
    var: *mut pg_sys::Var,
    root: *mut pg_sys::PlannerInfo,
) -> (
    pg_sys::Oid,
    pg_sys::AttrNumber,
    Option<PgList<pg_sys::TargetEntry>>,
) {
    let query = (*root).parse;
    let rte = pg_sys::rt_fetch((*var).varno as pg_sys::Index, (*query).rtable);

    match (*rte).rtekind {
        // the Var comes from a relation
        pg_sys::RTEKind::RTE_RELATION => ((*rte).relid, (*var).varattno, None),

        // the Var comes from a subquery, so dig into its target list and find the original
        // table it comes from along with its original column AttributeNumber
        pg_sys::RTEKind::RTE_SUBQUERY => {
            let targetlist = PgList::<pg_sys::TargetEntry>::from_pg((*(*rte).subquery).targetList);
            let te = targetlist
                .get_ptr((*var).varattno as usize - 1)
                .expect("var should exist in subquery TargetList");
            ((*te).resorigtbl, (*te).resorigcol, Some(targetlist))
        }

        // the Var comes from a CTE, so lookup that CTE and find it in the CTE's target list
        pg_sys::RTEKind::RTE_CTE => {
            let mut levelsup = (*rte).ctelevelsup;
            let mut cteroot = root;
            while levelsup > 0 {
                cteroot = (*cteroot).parent_root;
                if cteroot.is_null() {
                    // shouldn't happen
                    panic!(
                        "bad levelsup for CTE \"{}\"",
                        CStr::from_ptr((*rte).ctename).to_string_lossy()
                    )
                }
                levelsup -= 1;
            }

            let rte_ctename = CStr::from_ptr((*rte).ctename);
            let ctelist = PgList::<pg_sys::CommonTableExpr>::from_pg((*(*cteroot).parse).cteList);
            let mut matching_cte = None;
            for cte in ctelist.iter_ptr() {
                let ctename = CStr::from_ptr((*cte).ctename);

                if ctename == rte_ctename {
                    matching_cte = Some(cte);
                    break;
                }
            }

            let cte = matching_cte.unwrap_or_else(|| {
                panic!(
                    "unable to find cte named \"{}\"",
                    rte_ctename.to_string_lossy()
                )
            });

            if !is_a((*cte).ctequery, pg_sys::NodeTag::T_Query) {
                panic!("CTE is not a query")
            }
            let query = (*cte).ctequery.cast::<pg_sys::Query>();
            let targetlist = if !(*query).returningList.is_null() {
                PgList::<pg_sys::TargetEntry>::from_pg((*query).returningList)
            } else {
                PgList::<pg_sys::TargetEntry>::from_pg((*query).targetList)
            };
            let te = targetlist
                .get_ptr((*var).varattno as usize - 1)
                .expect("var should exist in cte TargetList");

            ((*te).resorigtbl, (*te).resorigcol, Some(targetlist))
        }
        _ => panic!("unsupported RTEKind: {}", (*rte).rtekind),
    }
}

/// Find all the Vars referenced in the specified node
pub unsafe fn find_vars(node: *mut pg_sys::Node) -> Vec<*mut pg_sys::Var> {
    #[pg_guard]
    unsafe extern "C" fn walker(node: *mut pg_sys::Node, data: *mut core::ffi::c_void) -> bool {
        if node.is_null() {
            return false;
        }

        if let Some(var) = nodecast!(Var, T_Var, node) {
            let data = data.cast::<Data>();
            (*data).vars.push(var);
        }

        expression_tree_walker(node, Some(walker), data)
    }

    struct Data {
        vars: Vec<*mut pg_sys::Var>,
    }

    let mut data = Data { vars: Vec::new() };

    walker(node, addr_of_mut!(data).cast());
    data.vars
}

/// Given a [`pg_sys::PlannerInfo`] and a [`pg_sys::Var`] from it, figure out the name of the `Var`
///
/// # Return
///
/// Returns the heap relation [`pg_sys::Oid`] that contains the `Var` along with its name.
pub unsafe fn attname_from_var(
    root: *mut pg_sys::PlannerInfo,
    var: *mut pg_sys::Var,
) -> (pg_sys::Oid, Option<String>) {
    let (heaprelid, varattno, _) = find_var_relation(var, root);
    if (*var).varattno == 0 {
        return (heaprelid, None);
    }
    let heaprel = PgRelation::open(heaprelid);
    let tupdesc = heaprel.tuple_desc();
    let attname = if varattno == pg_sys::SelfItemPointerAttributeNumber as pg_sys::AttrNumber {
        Some("ctid".into())
    } else {
        tupdesc
            .get(varattno as usize - 1)
            .map(|attribute| attribute.name().to_string())
    };
    (heaprelid, attname)
}

extension_sql!(
    r#"
ALTER FUNCTION paradedb.search_with_text SUPPORT paradedb.text_support;
ALTER FUNCTION paradedb.search_with_query_input SUPPORT paradedb.query_input_support;

CREATE OPERATOR pg_catalog.@@@ (
    PROCEDURE = search_with_text,
    LEFTARG = anyelement,
    RIGHTARG = text,
    RESTRICT = text_restrict
);

CREATE OPERATOR pg_catalog.@@@ (
    PROCEDURE = search_with_query_input,
    LEFTARG = anyelement,
    RIGHTARG = paradedb.searchqueryinput,
    RESTRICT = query_input_restrict
);

CREATE OPERATOR CLASS anyelement_bm25_ops DEFAULT FOR TYPE anyelement USING bm25 AS
    OPERATOR 1 pg_catalog.@@@(anyelement, text),                         /* for querying with a tantivy-compatible text query */
    OPERATOR 2 pg_catalog.@@@(anyelement, paradedb.searchqueryinput),    /* for querying with a paradedb.searchqueryinput structure */
    STORAGE anyelement;
"#,
    name = "bm25_ops_anyelement_operator",
    requires = [
        // for using plain text on the rhs
        text::search_with_text,
        text::text_restrict,
        text::text_support,
        // for using SearchQueryInput on the rhs
        searchqueryinput::search_with_query_input,
        searchqueryinput::query_input_restrict,
        searchqueryinput::query_input_support,
    ]
);
