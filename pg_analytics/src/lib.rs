#![allow(non_snake_case)]

mod datafusion;
mod errors;
mod guc;
mod hooks;
mod tableam;
mod types;

use pgrx::*;
use shared::logs::ParadeLogsGlobal;
use shared::telemetry::{setup_telemetry_background_worker, ParadeExtension};

use crate::guc::PARADE_GUC;
use crate::hooks::ParadeHook;

pgrx::pg_module_magic!();
extension_sql_file!("../sql/_bootstrap.sql");

// This is a flag that can be set by the user in a session to enable logs.
// You need to initialize this in every extension that uses `plog!`.
static PARADE_LOGS_GLOBAL: ParadeLogsGlobal = ParadeLogsGlobal::new("pg_bm25");
// These are the hooks that we register with Postgres.
static mut PARADE_HOOK: ParadeHook = ParadeHook;

#[pg_guard]
pub extern "C" fn _PG_init() {
    PARADE_LOGS_GLOBAL.init();
    PARADE_GUC.init();

    #[allow(unknown_lints)]
    #[allow(static_mut_ref)]
    unsafe {
        register_hook(&mut PARADE_HOOK)
    };

    setup_telemetry_background_worker(ParadeExtension::PgAnalytics);
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
