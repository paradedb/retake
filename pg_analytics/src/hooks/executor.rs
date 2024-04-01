use deltalake::datafusion::logical_expr::{DdlStatement, LogicalPlan};
use pgrx::*;
use std::ffi::CStr;
use thiserror::Error;

use super::handler::IsColumn;
use super::insert::insert;
use super::query::Query;
use super::select::{select, SelectHookError};
use crate::datafusion::commit::{commit_writer, needs_commit};
use crate::datafusion::plan::LogicalPlanDetails;
use crate::datafusion::query::QueryString;
use crate::errors::{NotSupported, ParadeError};
use crate::federation::handler::get_federated_batches;
use crate::federation::{COLUMN_FEDERATION_KEY, ROW_FEDERATION_KEY};
use crate::hooks::handler::TableClassifier;
use crate::hooks::insert::insert;
use crate::hooks::query::Query;
use crate::hooks::select::{get_datafusion_batches, write_batches_to_slots};

pub fn executor_run(
    query_desc: PgBox<pg_sys::QueryDesc>,
    direction: pg_sys::ScanDirection,
    count: u64,
    execute_once: bool,
    prev_hook: fn(
        query_desc: PgBox<pg_sys::QueryDesc>,
        direction: pg_sys::ScanDirection,
        count: u64,
        execute_once: bool,
    ) -> HookResult<()>,
) -> Result<(), ExecutorHookError> {
    unsafe {
        let ps = query_desc.plannedstmt;
        let rtable = (*ps).rtable;
        let pg_plan = query_desc.plannedstmt;
        let query = pg_plan.get_query_string(CStr::from_ptr(query_desc.sourceText))?;

        let classified_tables = rtable.table_lists()?;
        let col_tables =
            classified_tables
                .get(COLUMN_FEDERATION_KEY)
                .ok_or(ParadeError::Generic(
                    "Table classifier did not return a column list".to_string(),
                ))?;
        let row_tables = classified_tables
            .get(ROW_FEDERATION_KEY)
            .ok_or(ParadeError::Generic(
                "Table classifier did not return a column list".to_string(),
            ))?;

        if query_desc.operation == pg_sys::CmdType_CMD_INSERT {
            insert(rtable, query_desc.clone())?;
        }

        // Only use this hook for deltalake tables
        // Allow INSERTs to go through
        if rtable.is_null()
            || query_desc.operation == pg_sys::CmdType_CMD_INSERT
            || col_tables.is_empty()
            // Tech Debt: Find a less hacky way to let COPY go through
            || query.to_lowercase().starts_with("copy")
        {
            prev_hook(query_desc, direction, count, execute_once);
            return Ok(());
        }

        // If tables of different types are both present in the query, federate the query.
        if !row_tables.is_empty() && !col_tables.is_empty() {
            let batches =
                async_std::task::block_on(get_federated_batches(query, classified_tables))?;

            match query_desc.operation {
                pg_sys::CmdType_CMD_SELECT => write_batches_to_slots(query_desc, batches),
                _ => Err(ParadeError::NotSupported(NotSupported::Join(
                    query_desc.operation,
                ))),
            }
        } else {
            // Parse the query into a LogicalPlan
            match LogicalPlanDetails::try_from(QueryString(&query)) {
                Ok(logical_plan_details) => {
                    let logical_plan = logical_plan_details.logical_plan();

                    // CREATE TABLE queries can reach the executor for CREATE TABLE AS SELECT
                    // We should let these queries go through to the table access method
                    if let LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(_)) = logical_plan {
                        prev_hook(query_desc, direction, count, execute_once);
                        return Ok(());
                    }

                    // Execute SELECT, DELETE, UPDATE
                    match query_desc.operation {
                        pg_sys::CmdType_CMD_SELECT => {
                            let single_thread = logical_plan_details.includes_udf();
                            get_datafusion_batches(query_desc, logical_plan, single_thread)?;
                        }
                        pg_sys::CmdType_CMD_UPDATE => return Err(NotSupported::Update.into()),
                        _ => {
                            prev_hook(query_desc, direction, count, execute_once);
                        }
                    }
                }
                Err(_) => {
                    prev_hook(query_desc, direction, count, execute_once);
                }
            }
            pg_sys::CmdType_CMD_UPDATE => return Err(ExecutorHookError::UpdateNotSupported),
            _ => {
                prev_hook(query_desc, direction, count, execute_once);
            }
        };

        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum ExecutorHookError {
    #[error(transparent)]
    ParadeError(#[from] ParadeError),

    #[error(transparent)]
    SelectHookError(#[from] SelectHookError),

    #[error("UPDATE is not supported because Parquet tables are append only.")]
    UpdateNotSupported,
}
