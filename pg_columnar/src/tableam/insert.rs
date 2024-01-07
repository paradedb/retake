use async_std::task;
use core::ffi::c_int;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::arrow::array::ArrayRef;
use datafusion::common::arrow::datatypes::Schema;
use datafusion::common::DFSchema;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use lazy_static::lazy_static;
use parking_lot::RwLock;
use pgrx::*;
use std::sync::Arc;

use crate::datafusion::context::DatafusionContext;
use crate::datafusion::substrait::{DatafusionMap, DatafusionMapProducer, SubstraitTranslator};
use crate::datafusion::table::DatafusionTable;

pub struct BulkInsertState {
    pub batches: Vec<RecordBatch>,
    pub schema: Option<DFSchema>,
    pub nslots: usize,
}

impl BulkInsertState {
    pub const fn new() -> Self {
        BulkInsertState {
            batches: vec![],
            schema: None,
            nslots: 0,
        }
    }
}

lazy_static! {
    pub static ref BULK_INSERT_STATE: RwLock<BulkInsertState> = RwLock::new(BulkInsertState::new());
}

// Number of slots to hold in memory before flushing to disk
static MAX_SLOTS: usize = 5_000_000;

#[pg_guard]
pub unsafe extern "C" fn memam_slot_callbacks(
    _rel: pg_sys::Relation,
) -> *const pg_sys::TupleTableSlotOps {
    &pg_sys::TTSOpsVirtual
}

#[pg_guard]
pub unsafe extern "C" fn memam_multi_insert(
    rel: pg_sys::Relation,
    slots: *mut *mut pg_sys::TupleTableSlot,
    nslots: c_int,
    _cid: pg_sys::CommandId,
    _options: c_int,
    _bistate: *mut pg_sys::BulkInsertStateData,
) {
    let pg_relation = PgRelation::from_pg(rel);
    let tuple_desc = pg_relation.tuple_desc();
    let oids = tuple_desc
        .iter()
        .map(|attr| PgOid::from(attr.atttypid))
        .collect::<Vec<PgOid>>();

    let natts = tuple_desc.len();
    let pg_relation = unsafe { PgRelation::from_pg(rel) };
    let table = DatafusionTable::from_pg(&pg_relation).expect("Failed to get Datafusion table");
    let mut values: Vec<ArrayRef> = vec![];

    set_schema_if_needed(&table.name().unwrap(), &pg_relation);

    for (col_idx, oid) in oids.iter().enumerate().take(natts) {
        DatafusionMapProducer::map(oid.to_substrait().unwrap(), |df_map: DatafusionMap| {
            let mut datums = Vec::with_capacity(nslots as usize);
            let mut is_nulls = Vec::with_capacity(nslots as usize);

            for row_idx in 0..nslots {
                let tuple_table_slot = *slots.add(row_idx as usize);
                let datum = (*tuple_table_slot).tts_values.add(col_idx);
                let is_null = *(*tuple_table_slot).tts_isnull.add(col_idx);

                datums.push(datum);
                is_nulls.push(is_null);
            }

            values.push((df_map.array)(datums, is_nulls));
        })
        .expect("Could not map array");
    }

    let mut bulk_insert_state = BULK_INSERT_STATE.write();
    bulk_insert_state.nslots += nslots as usize;

    if let Some(schema) = &bulk_insert_state.schema {
        let binding = schema.into();
        bulk_insert_state
            .batches
            .push(RecordBatch::try_new(Arc::new(binding), values).expect("Could not create batch"));
    }

    if bulk_insert_state.nslots > MAX_SLOTS {
        drop(bulk_insert_state);
        flush_batches(rel);
    }
}

#[pg_guard]
pub unsafe extern "C" fn memam_finish_bulk_insert(rel: pg_sys::Relation, _options: c_int) {
    flush_batches(rel);
}

#[inline]
unsafe fn flush_batches(rel: pg_sys::Relation) {
    let pg_relation = PgRelation::from_pg(rel);
    let table = DatafusionTable::from_pg(&pg_relation).expect("Failed to get Datafusion table");
    let mut bulk_insert_state = BULK_INSERT_STATE.write();

    if bulk_insert_state.batches.is_empty() {
        return;
    }

    if let Some(schema) = &bulk_insert_state.schema {
        DatafusionContext::with_read(|context| {
            let memtable = Arc::new(
                MemTable::try_new(
                    Arc::new(Schema::from(schema)),
                    vec![bulk_insert_state.batches.clone()],
                )
                .expect("Could not create MemTable"),
            );
            let df = context
                .read_table(memtable)
                .expect("Could not create dataframe");
            let _ = task::block_on(
                df.write_table(&table.name().unwrap(), DataFrameWriteOptions::new()),
            );
        });

        bulk_insert_state.batches.clear();
        bulk_insert_state.nslots = 0;
    }
}

#[inline]
unsafe fn set_schema_if_needed(_table_name: &str, pg_relation: &PgRelation) {
    let mut bulk_insert_state = BULK_INSERT_STATE.write();

    if bulk_insert_state.schema.is_none() {
        let table = DatafusionTable::from_pg(pg_relation).expect("Failed to get Datafusion table");
        bulk_insert_state.schema = Some(table.schema().expect("Failed to get schema"));
    }
}
