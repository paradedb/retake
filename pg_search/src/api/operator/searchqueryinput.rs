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
use super::{
    anyelement_query_input_opoid, anyelement_query_input_procoid,
    make_search_query_input_opexpr_node,
};
use crate::api::operator::{estimate_selectivity, find_var_relation, ReturnedNodePointer};
use crate::gucs::per_tuple_cost;
use crate::index::fast_fields_helper::FFHelper;
use crate::index::reader::index::SearchIndexReader;
use crate::index::BlockDirectoryType;
use crate::postgres::types::TantivyValue;
use crate::postgres::utils::locate_bm25_index;
use crate::query::SearchQueryInput;
use crate::{nodecast, UNKNOWN_SELECTIVITY};
use parking_lot::Mutex;
use pgrx::{
    check_for_interrupts, pg_extern, pg_func_extra, pg_sys, AnyElement, FromDatum, Internal,
    PgList, PgOid, PgRelation,
};
use rustc_hash::{FxHashMap, FxHashSet};
use std::ptr::NonNull;

/// SQL API for allowing the user to specify the index to query.
///
/// This is useful (required, even) in cases where a query must be planned a sequential scan.
///
/// An example might be a query like this, that reads "find everything from `t` where the `body` field
/// contains a term from the `keywords` field.
///
/// ```sql
/// SELECT * FROM t WHERE key_field @@@ paradedb.term('body', keywords);
/// ```
///
/// In order for pg_search to execute this, we need to know the index to use, so it would need to be written
/// as:
///
/// ```sql
/// SELECT * FROM t WHERE key_field @@@ paradedb.with_index('bm25_idxt', paradedb.term('body', keywords));
/// ```
#[pg_extern(immutable, parallel_safe)]
pub fn with_index(index: PgRelation, query: SearchQueryInput) -> SearchQueryInput {
    SearchQueryInput::WithIndex {
        oid: index.oid(),
        query: Box::new(query),
    }
}

#[derive(Default)]
struct Cache {
    search_readers: Mutex<FxHashMap<pg_sys::Oid, (SearchIndexReader, FFHelper)>>,
    matches: Mutex<FxHashMap<(pg_sys::Oid, String), FxHashSet<TantivyValue>>>,
}

#[pg_extern(immutable, parallel_safe, cost = 1000000000)]
pub fn search_with_query_input(
    element: AnyElement,
    query: SearchQueryInput,
    fcinfo: pg_sys::FunctionCallInfo,
) -> bool {
    let index_oid = query
        .index_oid()
        .unwrap_or_else(|| panic!("the query argument must be wrapped in a `SearchQueryInput::WithIndex` variant.  Try using `paradedb.with_index('<index name>', <original expression>)`"));

    // get the Cache attached to this instance of the function
    let cache = unsafe { pg_func_extra(fcinfo, Cache::default) };

    // and get/initialize the SearchReader and FFHelper for this index_oid
    let mut search_readers = cache.search_readers.lock();
    let (search_reader, ff_helper) = search_readers.entry(index_oid).or_insert_with(|| {
        let index_relation = unsafe {
            PgRelation::with_lock(index_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE)
        };
        let search_reader =
            SearchIndexReader::open(&index_relation, BlockDirectoryType::Mvcc, false)
                .expect("search_with_query_input: should be able to open a SearchIndexReader");
        let key_field = search_reader.key_field();
        let key_field_name = key_field.name.0;
        let key_field_type = key_field.type_.into();
        let ff_helper =
            FFHelper::with_fields(&search_reader, &[(key_field_name, key_field_type).into()]);

        (search_reader, ff_helper)
    });

    // now, query the SearchReader and collect up the docs that match our query.
    // the matches are cached so that the same input query will return the same results
    // throughout the duration of the scan
    let mut matches = cache.matches.lock();
    let matches_key = (index_oid, format!("{query:?}")); // NB:  ideally, `SearchQueryInput` would `#[derive(Hash)]`, but it can't (easily)
    let matches = matches.entry(matches_key).or_insert_with(|| {
        search_reader
            .search(query.need_scores(), false, &query, None)
            .map(|(_, doc_address)| {
                check_for_interrupts!();
                ff_helper
                    .value(0, doc_address)
                    .expect("key_field value should not be null")
            })
            .collect()
    });

    // finally, see if the value on the lhs of the @@@ operator (which should always be our "key_field")
    // is contained in the matches set
    unsafe {
        let user_value =
            TantivyValue::try_from_datum(element.datum(), PgOid::from_untagged(element.oid()))
                .expect("no value present");

        matches.contains(&user_value)
    }
}

#[pg_extern(immutable, parallel_safe)]
pub unsafe fn query_input_support(arg: Internal) -> ReturnedNodePointer {
    let datum = match arg.unwrap() {
        Some(d) => d,
        None => return ReturnedNodePointer(None),
    };

    if let Some(node) = query_input_support_request_simplify(datum) {
        return node;
    }

    if let Some(node) = search_query_input_request_cost(datum) {
        return node;
    }

    ReturnedNodePointer(None)
}

fn query_input_support_request_simplify(arg: pg_sys::Datum) -> Option<ReturnedNodePointer> {
    unsafe {
        let srs = nodecast!(
            SupportRequestSimplify,
            T_SupportRequestSimplify,
            arg.cast_mut_ptr::<pg_sys::Node>()
        )?;

        // Rewrite this node touse the @@@(key_field, paradedb.searchqueryinput) operator.
        // This involves converting the rhs of the operator into a SearchQueryInput.
        let mut input_args = PgList::<pg_sys::Node>::from_pg((*(*srs).fcall).args);
        let var = nodecast!(Var, T_Var, input_args.get_ptr(0)?)?;

        // NB:  there was a point where we only allowed a relation reference on the left of @@@
        // when the right side uses a builder function, but we've decided that also allowing a field
        // name is materially better as the relation reference might be an artificial ROW(...) whereas
        // a field will be a legitimate Var from which we can derive the physical table.
        // if (*var).varattno != 0 {
        //     panic!("the left side of the `@@@` operator must be a relation reference when the right side uses a builder function");
        // }

        let rhs = input_args.get_ptr(1)?;
        let query = nodecast!(Const, T_Const, rhs)
            .map(|const_| SearchQueryInput::from_datum((*const_).constvalue, (*const_).constisnull))
            .flatten();

        Some(make_search_query_input_opexpr_node(
            srs,
            &mut input_args,
            var,
            query,
            None,
            anyelement_query_input_opoid(),
            anyelement_query_input_procoid(),
        ))
    }
}

#[pg_extern(immutable, parallel_safe)]
pub fn query_input_restrict(
    planner_info: Internal, // <pg_sys::PlannerInfo>,
    operator_oid: pg_sys::Oid,
    args: Internal, // <pg_sys::List>,
    _var_relid: i32,
) -> f64 {
    fn inner_query_input(
        planner_info: Internal, // <pg_sys::PlannerInfo>,
        args: Internal,         // <pg_sys::List>,
    ) -> Option<f64> {
        unsafe {
            let info = planner_info.unwrap()?.cast_mut_ptr::<pg_sys::PlannerInfo>();
            let args =
                PgList::<pg_sys::Node>::from_pg(args.unwrap()?.cast_mut_ptr::<pg_sys::List>());

            let var = nodecast!(Var, T_Var, args.get_ptr(0)?)?;
            let const_ = nodecast!(Const, T_Const, args.get_ptr(1)?)?;

            let (heaprelid, _, _) = find_var_relation(var, info);
            let indexrel = locate_bm25_index(heaprelid)?;

            // create the search query from the rhs Const node
            let search_query_input =
                SearchQueryInput::from_datum((*const_).constvalue, (*const_).constisnull)?;

            estimate_selectivity(&indexrel, &search_query_input)
        }
    }

    assert!(operator_oid == anyelement_query_input_opoid());

    let mut selectivity = inner_query_input(planner_info, args).unwrap_or(UNKNOWN_SELECTIVITY);
    if selectivity > 1.0 {
        selectivity = UNKNOWN_SELECTIVITY;
    }

    selectivity
}

fn search_query_input_request_cost(arg: pg_sys::Datum) -> Option<ReturnedNodePointer> {
    unsafe {
        let src = nodecast!(
            SupportRequestCost,
            T_SupportRequestCost,
            arg.cast_mut_ptr::<pg_sys::Node>()
        )?;
        // our `search_with_*` functions are *incredibly* expensive.  So much so that
        // we really don't ever want Postgres to prefer them.
        //
        // The higher the `per_tuple` cost is here, the better.
        //
        // it can cost a lot to startup the `@@@` operator outside of an IndexScan because
        // ultimately we have to hash all the resulting ctids in memory.  For lack of a better
        // value, we say it costs as much as the `GUCS.per_tuple_cost()`.  This is an arbitrary
        // number that we've documented as needing to be big.
        (*src).startup = per_tuple_cost();

        // similarly, use the same GUC here.  Postgres will then add this into its per-tuple
        // cost evaluations for whatever scan it's considering using for the `@@@` operator.
        // our IAM provides more intelligent costs for the IndexScan situation.
        (*src).per_tuple = per_tuple_cost();

        Some(ReturnedNodePointer(NonNull::new(src.cast())))
    }
}
