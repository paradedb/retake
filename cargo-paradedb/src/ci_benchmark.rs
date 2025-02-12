// Copyright (c) 2023-2025 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 (AGPLv3) or (at your option)
// any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

#![allow(dead_code)]

use anyhow::{anyhow, bail, Context, Result};
use async_std::task::block_on;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, PgConnection, Row};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;
use sysinfo::{Disks, System};

/// Convert a memory string like `"16GB"` into bytes (e.g. `"16GB" -> 17179869184`).
fn parse_memory_str(s: &str) -> Option<u64> {
    let upper = s.trim().to_uppercase();
    if upper.ends_with("GB") {
        upper
            .trim_end_matches("GB")
            .trim()
            .parse::<f64>()
            .ok()
            .map(|v| (v * 1024.0 * 1024.0 * 1024.0) as u64)
    } else if upper.ends_with("MB") {
        upper
            .trim_end_matches("MB")
            .trim()
            .parse::<f64>()
            .ok()
            .map(|v| (v * 1024.0 * 1024.0) as u64)
    } else if upper.ends_with("KB") {
        upper
            .trim_end_matches("KB")
            .trim()
            .parse::<f64>()
            .ok()
            .map(|v| (v * 1024.0) as u64)
    } else {
        s.trim().parse::<u64>().ok()
    }
}

/// Convert a byte count into a human-friendly string (e.g. `12345678 -> "11.77MB"`).
fn format_bytes(bytes: u64) -> String {
    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;

    if bytes >= GB {
        format!("{:.2}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2}MB", bytes as f64 / MB as f64)
    } else {
        format!("{}B", bytes)
    }
}

/// Statistics (min, max, mean, stddev, p99) for latencies in microseconds.
#[derive(Debug, Serialize, Deserialize)]
pub struct LatencyStatsUs {
    pub min_us: i64,
    pub max_us: i64,
    pub mean_us: i64,
    pub stddev_us: i64,
    pub p99_us: i64,
}

/// Compute min, max, mean, stddev, p99 (in µs) from a slice of microsecond latencies.
fn compute_latency_stats_us(latencies_us: &[i64]) -> Option<LatencyStatsUs> {
    if latencies_us.is_empty() {
        return None;
    }

    let len = latencies_us.len() as i64;
    let min_val = latencies_us.iter().copied().min().unwrap_or(0);
    let max_val = latencies_us.iter().copied().max().unwrap_or(0);

    let sum: i64 = latencies_us.iter().sum();
    let mean_f = sum as f64 / len as f64;

    let mut sum_sq = 0f64;
    for &val in latencies_us {
        let diff = val as f64 - mean_f;
        sum_sq += diff * diff;
    }
    let variance_f = sum_sq / (len as f64);
    let stddev_f = variance_f.sqrt();

    let mut sorted = latencies_us.to_vec();
    sorted.sort_unstable();
    let p99_index = ((len as f64) * 0.99).ceil() as usize - 1;
    let p99_val = sorted[std::cmp::min(p99_index, sorted.len() - 1)];

    Some(LatencyStatsUs {
        min_us: min_val,
        max_us: max_val,
        mean_us: mean_f.round() as i64,
        stddev_us: stddev_f.round() as i64,
        p99_us: p99_val,
    })
}

/// Configuration for the benchmark suite, controlling concurrency, transactions, etc.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchmarkSuiteConfig {
    /// Postgres connection string, e.g. postgres://user@localhost/dbname
    pub db_url: String,

    /// SQL files to test
    pub sql_files: Vec<PathBuf>,

    /// Parallel clients (concurrency)
    pub clients: u32,

    /// Number of transactions per client
    pub transactions: u32,

    /// Whether to build the custom index
    pub index: bool,

    /// e.g. "16GB"
    pub maintenance_work_mem: String,

    /// Table in which to store the final JSON
    pub report_table: String,
}

/// Top-level JSON report structure.
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub extension_version: Option<String>,
    pub extension_sha: Option<String>,
    pub extension_build_mode: Option<String>,

    pub suite_started_at: DateTime<Utc>,
    pub suite_finished_at: Option<DateTime<Utc>>,

    pub config: BenchmarkSuiteConfig,
    pub maintenance_work_mem_info: MaintenanceWorkMemInfo,
    pub pg_stat_statements_available: bool,
    pub connection_ok: bool,

    pub db_size_before: Option<String>,
    pub db_size_after: Option<String>,

    pub index_creation_benchmark: Option<IndexCreationBenchmarkResult>,
    pub pgbench_tests: Vec<PgBenchTestResult>,
    pub reltuples: Option<i64>,
}

/// Info about how we set maintenance_work_mem vs. total memory
#[derive(Debug, Serialize, Deserialize)]
pub struct MaintenanceWorkMemInfo {
    pub configured: String,
    pub total_system_memory: String,
    pub allowed_max_80pct: String,
    pub effective_value: String,
}

/// Metadata about a custom index creation step.
#[derive(Debug, Serialize, Deserialize)]
pub struct IndexCreationBenchmarkResult {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub duration_seconds: f64,

    pub index_settings: HashMap<String, String>,
    pub index_size: Option<String>,
    pub index_info: Option<Vec<serde_json::Value>>,

    pub sysinfo_samples: Vec<SysinfoSample>,
}

/// System resource usage sample (CPU, memory, disk I/O) during indexing.
#[derive(Debug, Serialize, Deserialize)]
pub struct SysinfoSample {
    pub timestamp: DateTime<Utc>,
    pub cpu_usage_percent: f64,
    pub total_memory_bytes: u64,
    pub used_memory_bytes: u64,
    pub total_read_bytes: u64,
    pub total_written_bytes: u64,
}

/// A single pgbench test result for one SQL file
#[derive(Debug, Serialize, Deserialize)]
pub struct PgBenchTestResult {
    pub test_name: String,
    pub sql_file: String,
    pub start_time_utc: DateTime<Utc>,
    pub end_time_utc: DateTime<Utc>,
    pub duration_seconds: f64,

    /// “tps” from pgbench summary
    pub tps: Option<f64>,

    /// “latency average” in ms from pgbench summary
    pub avg_latency_ms: Option<f64>,

    pub db_size_before: Option<String>,
    pub db_size_after: Option<String>,

    /// Top statements from pg_stat_statements
    pub top_statements: Vec<TopStatement>,

    /// “progress: x s, y tps, lat z ms” lines
    pub intervals: Vec<PgBenchLogInterval>,

    /// The raw transaction logs from pgbench’s -l output
    pub transaction_log: Vec<PgBenchTransactionLog>,

    /// Overall stats computed from `transaction_log`
    pub computed_latency_stats: Option<LatencyStatsUs>,

    /// Per-statement aggregated stats
    pub statement_latency_details: Vec<StatementLatencyStatsUs>,

    /// Sum of “rows” in pg_stat_statements, if available
    pub items_matched: Option<i64>,

    /// The actual SQL
    pub full_sql: String,
}

/// A row from pg_stat_statements
#[derive(Debug, Serialize, Deserialize)]
pub struct TopStatement {
    pub query: String,
    pub calls: i64,
    pub total_time_ms: f64,
    pub rows: i64,
}

/// A parsed “progress:” line from pgbench’s stdout
#[derive(Debug, Serialize, Deserialize)]
pub struct PgBenchLogInterval {
    pub time_sec: f64,
    pub n_xacts: u64,
    pub latency_avg_ms: f64,
    pub tps_in_interval: f64,
}

/// A single per-transaction log line from pgbench’s `-l` output.
///
/// According to current pgbench documentation, each line is something like:
///  [client_id, transaction_no, time (µs), script_no, time_epoch, time_us, ... schedule_lag/retries if present ...]
///
/// We interpret `time` (the 3rd column) as the actual "transaction elapsed time in microseconds."
/// The `time_epoch` and `time_us` are combined to form a completion timestamp if needed, but
/// here we store them in `time_us` for reference.
#[derive(Debug, Serialize, Deserialize)]
pub struct PgBenchTransactionLog {
    pub client_id: u32,
    pub transaction_id: u32,
    /// For reference, we store the absolute microseconds part of the epoch-based completion time.
    pub time_us: i64,
    /// The actual transaction latency in microseconds (column 3).
    pub latency_us: i64,
    /// Any leftover text is a statement label, if present
    pub stmt_name: String,
}

/// Aggregated stats for one statement name (min/max/mean/stddev/p99 in µs)
#[derive(Debug, Serialize, Deserialize)]
pub struct StatementLatencyStatsUs {
    pub stmt_name: String,
    pub calls: usize,
    pub latency_us: LatencyStatsUs,
}

/// Primary orchestrator for the entire benchmark workflow
pub struct BenchmarkSuite {
    config: BenchmarkSuiteConfig,
    connection: Option<PgConnection>,
    report: BenchmarkReport,
}

impl BenchmarkSuite {
    /// Build the suite, confirm connectivity, check pg_stat_statements, etc.
    pub async fn new(config: BenchmarkSuiteConfig) -> Result<Self> {
        let conn_opts = PgConnectOptions::from_str(&config.db_url).context("Invalid DB URL")?;
        let mut conn = PgConnection::connect_with(&conn_opts)
            .await
            .context("Failed to connect to PostgreSQL")?;

        Self::ensure_report_table_schema(&mut conn, &config.report_table).await?;

        // Check for pg_stat_statements
        let stat_avail: bool = sqlx::query_scalar(
            r#"SELECT EXISTS(
                SELECT 1
                FROM pg_available_extensions
                WHERE name='pg_stat_statements'
            );"#,
        )
        .fetch_one(&mut conn)
        .await
        .unwrap_or(false);

        if stat_avail {
            let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
                .execute(&mut conn)
                .await;
        }

        // System memory
        let mut sys = System::new_all();
        sys.refresh_memory();
        let total_bytes = sys.total_memory() * 1024;
        let allowed_max = (total_bytes as f64 * 0.8) as u64;

        let requested = parse_memory_str(&config.maintenance_work_mem).ok_or_else(|| {
            anyhow!(
                "Cannot parse maintenance_work_mem: {}",
                config.maintenance_work_mem
            )
        })?;

        let effective = if requested > allowed_max {
            allowed_max
        } else {
            requested
        };
        let effective_str = format_bytes(effective);

        // Set maintenance_work_mem
        let set_stmt = format!("SET maintenance_work_mem = '{}';", effective_str);
        sqlx::query(&set_stmt).execute(&mut conn).await?;

        // Validate final setting
        let (validated,): (String,) = sqlx::query_as("SHOW maintenance_work_mem;")
            .fetch_one(&mut conn)
            .await
            .context("Could not validate maintenance_work_mem")?;

        // Basic connectivity check
        let (check_str,): (String,) = sqlx::query_as("SELECT 'Connection OK'::text")
            .fetch_one(&mut conn)
            .await
            .unwrap_or_else(|_| ("Connection Failed".into(),));

        // extension version info
        let (extension_version, extension_sha, extension_build_mode) =
            Self::fetch_version_info(&mut conn).await?;

        if extension_sha.is_none() {
            eprintln!("WARNING: no extension_sha found in paradedb.version_info()");
        }

        let report = BenchmarkReport {
            extension_version,
            extension_sha,
            extension_build_mode,
            suite_started_at: Utc::now(),
            suite_finished_at: None,
            config: config.clone(),
            maintenance_work_mem_info: MaintenanceWorkMemInfo {
                configured: config.maintenance_work_mem.clone(),
                total_system_memory: format_bytes(total_bytes),
                allowed_max_80pct: format_bytes(allowed_max),
                effective_value: validated,
            },
            pg_stat_statements_available: stat_avail,
            connection_ok: check_str == "Connection OK",
            db_size_before: None,
            db_size_after: None,
            index_creation_benchmark: None,
            pgbench_tests: vec![],
            reltuples: None,
        };

        Ok(Self {
            config,
            connection: Some(conn),
            report,
        })
    }

    /// Create table for final JSON if needed
    async fn ensure_report_table_schema(conn: &mut PgConnection, table_name: &str) -> Result<()> {
        let (schema, table) = match table_name.find('.') {
            Some(idx) => (&table_name[..idx], &table_name[idx + 1..]),
            None => ("public", table_name),
        };

        let create_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ DEFAULT now(),
                git_hash text NOT NULL DEFAULT '',
                report_data JSONB NOT NULL DEFAULT '{{}}'
            );
            "#
        );
        sqlx::query(&create_sql).execute(&mut *conn).await?;

        let alter_sql = format!(
            r#"
            ALTER TABLE "{schema}"."{table}"
            ADD COLUMN IF NOT EXISTS git_hash text DEFAULT '',
            ALTER COLUMN git_hash SET DEFAULT '',
            ADD COLUMN IF NOT EXISTS report_data jsonb DEFAULT '{{}}',
            ALTER COLUMN report_data SET DEFAULT '{{}}',
            ADD COLUMN IF NOT EXISTS created_at timestamptz DEFAULT now(),
            ALTER COLUMN created_at SET DEFAULT now()
            "#
        );
        sqlx::query(&alter_sql).execute(&mut *conn).await?;

        // Verify we have a jsonb column
        let cols = sqlx::query(
            r#"
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = $1
                  AND table_name = $2
                "#,
        )
        .bind(schema)
        .bind(table)
        .fetch_all(conn)
        .await?;

        let mut found_jsonb = false;
        for row in &cols {
            let col_name: &str = row.get("column_name");
            let dt: &str = row.get("data_type");
            if col_name == "report_data" && dt == "jsonb" {
                found_jsonb = true;
                break;
            }
        }
        if !found_jsonb {
            bail!("Table {table_name} must have a 'report_data' JSONB column");
        }
        Ok(())
    }

    fn conn_mut(&mut self) -> Result<&mut PgConnection> {
        self.connection
            .as_mut()
            .ok_or_else(|| anyhow!("No active DB connection"))
    }

    fn pg_stat_statements_available(&self) -> bool {
        self.report.pg_stat_statements_available
    }

    async fn reset_pg_stat_statements(&mut self) -> Result<()> {
        // We run this in a DO block so that it doesn't return a row,
        // which messes up our "matched_items" count.
        if self.pg_stat_statements_available() {
            sqlx::query(
                r#"DO $$
                    BEGIN
                        PERFORM pg_stat_statements_reset();
                    END$$;"#,
            )
            .execute(self.conn_mut()?)
            .await?;
        }
        Ok(())
    }

    async fn fetch_db_size(&mut self) -> Result<String> {
        let (sz,): (String,) =
            sqlx::query_as("SELECT pg_size_pretty(pg_database_size(current_database()))")
                .fetch_one(self.conn_mut()?)
                .await
                .context("Could not fetch DB size")?;
        Ok(sz)
    }

    async fn fetch_version_info(
        conn: &mut PgConnection,
    ) -> Result<(Option<String>, Option<String>, Option<String>)> {
        let row = sqlx::query_as::<_, (Option<String>, Option<String>, Option<String>)>(
            "SELECT version, githash, build_mode FROM paradedb.version_info();",
        )
        .fetch_one(conn)
        .await
        .unwrap_or((None, None, None));
        Ok(row)
    }

    async fn get_current_setting(&mut self, key: &str) -> Result<Option<String>> {
        let sql = format!("SELECT current_setting('{key}') AS val;");
        let row = sqlx::query_as::<_, (String,)>(&sql)
            .fetch_one(self.conn_mut()?)
            .await;
        match row {
            Ok((val,)) => Ok(Some(val)),
            Err(_) => Ok(None),
        }
    }

    async fn fetch_index_size(&mut self, idx: &str) -> Result<String> {
        let sql = format!("SELECT pg_size_pretty(pg_relation_size('{idx}'));");
        let (sz,): (String,) = sqlx::query_as(&sql)
            .fetch_one(self.conn_mut()?)
            .await
            .context("Could not fetch index size")?;
        Ok(sz)
    }

    async fn fetch_paradedb_index_info(&mut self, idx: &str) -> Result<Vec<serde_json::Value>> {
        let sql = format!("SELECT row_to_json(x) FROM paradedb.index_info('{idx}') x;");
        let rows = sqlx::query_as::<_, (serde_json::Value,)>(sql.as_str())
            .fetch_all(self.conn_mut()?)
            .await
            .context("Could not query paradedb.index_info")?;
        Ok(rows.into_iter().map(|(val,)| val).collect())
    }

    /// Optionally call pg_prewarm to pre-load the index
    async fn try_prewarm_index(&mut self, idx: &str) -> Result<()> {
        let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_prewarm;")
            .execute(self.conn_mut()?)
            .await;
        let preload_sql = format!("SELECT pg_prewarm('{idx}');");
        let _ = sqlx::query(&preload_sql).execute(self.conn_mut()?).await;
        Ok(())
    }

    /// Build custom search index, measure system usage in background
    async fn benchmark_index_creation(&mut self) -> Result<IndexCreationBenchmarkResult> {
        let now_utc = Utc::now();
        println!("Starting custom index creation ...");

        // Drop existing
        sqlx::query("DROP INDEX IF EXISTS idx_benchmark_eslogs_bm25;")
            .execute(self.conn_mut()?)
            .await?;

        // Example settings
        let desired_settings = [
            ("maintenance_work_mem", "16GB"),
            ("paradedb.statement_parallelism", "1"),
            ("paradedb.statement_memory_budget", "15"),
            ("paradedb.create_index_parallelism", "8"),
            ("paradedb.create_index_memory_budget", "1024"),
            ("max_worker_processes", "16"),
            ("max_parallel_workers", "16"),
            ("max_parallel_workers_per_gather", "16"),
            ("shared_buffers", "8GB"),
            ("paradedb.enable_custom_scan", "true"),
        ];

        for (k, v) in &desired_settings {
            let stmt = format!("SET {k} = '{v}';");
            let _ = sqlx::query(&stmt).execute(self.conn_mut()?).await;
        }

        // Start background sysinfo thread
        let stop_flag = Arc::new(AtomicBool::new(false));
        let sf = stop_flag.clone();
        let creation_start = Instant::now();
        let handle = std::thread::spawn(move || -> Vec<SysinfoSample> {
            let mut sys = System::new_all();
            let mut samples = Vec::new();
            while !sf.load(Ordering::SeqCst) {
                sys.refresh_all();

                let cpu_usage = if sys.cpus().is_empty() {
                    0.0
                } else {
                    sys.cpus().iter().map(|c| c.cpu_usage() as f64).sum::<f64>()
                        / (sys.cpus().len() as f64)
                };
                let total_mem = sys.total_memory() * 1024;
                let used_mem = sys.used_memory() * 1024;

                let disks = Disks::new();
                let total_read = disks.iter().map(|d| d.usage().read_bytes).sum();
                let total_written = disks.iter().map(|d| d.usage().total_written_bytes).sum();

                samples.push(SysinfoSample {
                    timestamp: Utc::now(),
                    cpu_usage_percent: cpu_usage,
                    total_memory_bytes: total_mem,
                    used_memory_bytes: used_mem,
                    total_read_bytes: total_read,
                    total_written_bytes: total_written,
                });
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            samples
        });

        // Actually create the index
        let create_sql = r#"
            CREATE INDEX idx_benchmark_eslogs_bm25
            ON public.benchmark_eslogs
            USING bm25(
                id,
                process,
                cloud,
                aws_cloudwatch,
                agent,
                "timestamp",
                message,
                metrics_size,
                log_file_path
            )
            WITH (
                key_field = 'id',
                text_fields='{"message": {}, "log_file_path": {}}',
                numeric_fields='{"metrics_size": {}}',
                datetime_fields='{"timestamp": {}}',
                json_fields='{"process": {}, "cloud": {}, "aws_cloudwatch": {}, "agent": {}}'
            );
        "#;
        sqlx::query(create_sql).execute(self.conn_mut()?).await?;

        // Stop sampling
        let duration = creation_start.elapsed();
        stop_flag.store(true, Ordering::SeqCst);
        let samples = handle.join().unwrap_or_default();

        // Collect final settings
        let mut index_settings = HashMap::new();
        for (k, _) in &desired_settings {
            if let Ok(Some(val)) = self.get_current_setting(k).await {
                index_settings.insert(k.to_string(), val);
            }
        }

        // Prewarm
        let _ = self.try_prewarm_index("idx_benchmark_eslogs_bm25").await;

        let index_size = self
            .fetch_index_size("idx_benchmark_eslogs_bm25")
            .await
            .ok();
        let index_info = self
            .fetch_paradedb_index_info("idx_benchmark_eslogs_bm25")
            .await
            .ok();

        Ok(IndexCreationBenchmarkResult {
            started_at: now_utc,
            finished_at: Utc::now(),
            duration_seconds: duration.as_secs_f64(),
            index_settings,
            index_size,
            index_info,
            sysinfo_samples: samples,
        })
    }

    /// Fetch top 5 statements from pg_stat_statements by total time
    fn fetch_top_statements_sync(&mut self) -> Result<Vec<TopStatement>> {
        if !self.pg_stat_statements_available() {
            return Ok(vec![]);
        }
        let conn = self.conn_mut()?;
        let rows = block_on(async {
            sqlx::query(
                r#"
                SELECT query,
                       calls,
                       (total_plan_time + total_exec_time) AS total_ms,
                       rows
                FROM pg_stat_statements
                ORDER BY (total_plan_time + total_exec_time) DESC
                LIMIT 5;
                "#,
            )
            .fetch_all(conn)
            .await
        })?;

        let mut out = vec![];
        for row in rows {
            let query: String = row.get("query");
            let calls: i64 = row.get("calls");
            let total_ms: f64 = row.get("total_ms");
            let rows_returned: i64 = row.get("rows");
            out.push(TopStatement {
                query,
                calls,
                total_time_ms: total_ms,
                rows: rows_returned,
            });
        }
        Ok(out)
    }

    /// Sum of “rows” in pg_stat_statements
    fn fetch_total_items_matched_sync(&mut self) -> Result<Option<i64>> {
        if !self.pg_stat_statements_available() {
            return Ok(None);
        }
        let conn = self.conn_mut()?;
        let row = block_on(async {
            sqlx::query(
                "SELECT COALESCE(SUM(rows), 0)::BIGINT AS matched_items FROM pg_stat_statements;",
            )
            .fetch_one(conn)
            .await
        })?;
        let sum: i64 = row.get("matched_items");
        Ok(Some(sum))
    }

    /// Identify lines like `progress: 1.0 s, 101.0 tps, lat 12.3 ms`
    fn parse_aggregate_intervals(stdout: &str) -> Vec<PgBenchLogInterval> {
        let mut intervals = Vec::new();
        for line in stdout.lines() {
            if line.starts_with("progress:") {
                // e.g. "progress: 1.0 s, 101.0 tps, lat 12.3 ms"
                let parts: Vec<_> = line.split(',').collect();
                if parts.len() < 3 {
                    continue;
                }
                let time_part = parts[0]
                    .replacen("progress:", "", 1)
                    .replacen("s", "", 1)
                    .trim()
                    .to_string();
                let tps_part = parts[1].replace("tps", "").trim().to_string();
                let lat_part = parts[2]
                    .replace("lat", "")
                    .replace("ms", "")
                    .trim()
                    .to_string();

                let time_sec = time_part.parse::<f64>().unwrap_or(0.0);
                let tps_val = tps_part.parse::<f64>().unwrap_or(0.0);
                let lat_val = lat_part.parse::<f64>().unwrap_or(0.0);

                intervals.push(PgBenchLogInterval {
                    time_sec,
                    n_xacts: 0,
                    latency_avg_ms: lat_val,
                    tps_in_interval: tps_val,
                });
            }
        }
        intervals
    }

    /// Parse all pgbench per-transaction logs from any `pgbench_log*` file in the dir.
    ///
    /// We interpret columns per the modern `pgbench` doc:
    ///   [client_id, transaction_no, time (µs), script_no, time_epoch, time_us, ...].
    fn parse_pgbench_transaction_logs_in_dir(log_dir: &Path) -> Vec<PgBenchTransactionLog> {
        let mut tx_logs = Vec::new();
        let dir_iter = match fs::read_dir(log_dir) {
            Ok(iter) => iter,
            Err(_) => return tx_logs,
        };

        for entry in dir_iter.flatten() {
            let path = entry.path();
            let fname = path.file_name().unwrap_or_default().to_string_lossy();
            if !fname.starts_with("pgbench_log") {
                continue;
            }

            let content = match fs::read_to_string(&path) {
                Ok(c) => c,
                Err(_) => continue,
            };
            for line in content.lines() {
                let parts: Vec<_> = line.split_whitespace().collect();
                // We expect at least 6 columns to parse
                if parts.len() >= 6 {
                    if let Ok(tx) = Self::parse_transaction_line(&parts) {
                        tx_logs.push(tx);
                    }
                }
            }
        }
        tx_logs
    }

    /// Parse a single line from pgbench’s `-l` logs:
    ///
    /// Columns (at least 6):
    /// 0: client_id
    /// 1: transaction_no
    /// 2: time (microseconds)
    /// 3: script_no
    /// 4: time_epoch
    /// 5: time_us
    /// possibly more columns for schedule_lag, retries, etc.
    fn parse_transaction_line(parts: &[&str]) -> Result<PgBenchTransactionLog> {
        if parts.len() < 6 {
            bail!("Not enough columns in line: {:?}", parts);
        }

        // parse relevant columns
        let client_id = parts[0].parse::<u32>()?;
        let transaction_no = parts[1].parse::<u32>()?;
        let time = parts[2].parse::<f64>()?; // actual latency in microseconds
        let _script_no = parts[3].parse::<u32>()?;
        let _time_epoch = parts[4].parse::<i64>()?;
        let time_us = parts[5].parse::<i64>()?;

        // The “time” field is the transaction's elapsed time in microseconds
        let latency_us = time.round() as i64;

        // If there's more columns beyond the 6th, treat them as a statement name
        let stmt_name = if parts.len() > 6 {
            parts[6..].join(" ")
        } else {
            String::new()
        };

        // We store transaction_no in the `transaction_id` field.
        Ok(PgBenchTransactionLog {
            client_id,
            transaction_id: transaction_no,
            time_us,
            latency_us,
            stmt_name,
        })
    }

    /// Group logs by `stmt_name`, compute min/mean/stddev/p99 for each group
    fn group_and_compute_per_statement_stats_us(
        tx_logs: &[PgBenchTransactionLog],
    ) -> Vec<StatementLatencyStatsUs> {
        let mut map: HashMap<String, Vec<i64>> = HashMap::new();

        for tx in tx_logs {
            map.entry(tx.stmt_name.clone())
                .or_default()
                .push(tx.latency_us);
        }

        let mut out = Vec::new();
        for (stmt, latencies) in map {
            if let Some(stats) = compute_latency_stats_us(&latencies) {
                out.push(StatementLatencyStatsUs {
                    stmt_name: stmt,
                    calls: latencies.len(),
                    latency_us: stats,
                });
            }
        }
        out.sort_by(|a, b| a.stmt_name.cmp(&b.stmt_name));
        out
    }

    /// Actually run pgbench for one SQL file, parse logs, glean stats
    async fn run_single_sql_pgbench(
        &mut self,
        sql_file: &Path,
        test_name: &str,
    ) -> Result<PgBenchTestResult> {
        let start_utc = Utc::now();
        let start_instant = Instant::now();

        let base_name = sql_file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown_sql")
            .to_string();

        let sql_content = fs::read_to_string(sql_file)
            .with_context(|| format!("Could not read file: {}", sql_file.display()))?;

        // Write to a temp file so we can pass it to pgbench
        let (tmp_sql_path, tmp_file) =
            tempfile::NamedTempFile::new().map(|f| (f.path().to_path_buf(), f))?;
        {
            let mut w = std::io::BufWriter::new(&tmp_file);
            let mem_val = &self.report.maintenance_work_mem_info.effective_value;
            let set_str = format!("SET maintenance_work_mem = '{}';", mem_val);
            writeln!(w, "{}", set_str)?;
            w.write_all(sql_content.as_bytes())?;
            w.flush()?;
        }

        let db_size_before = self.fetch_db_size().await.ok();
        self.reset_pg_stat_statements().await?;

        // logs go into a scratch dir
        let scratch_dir = tempfile::tempdir()?;
        let scratch_path = scratch_dir.path();

        // Build pgbench command
        let mut cmd = Command::new("pgbench");
        cmd.current_dir(scratch_path)
            .arg("--client")
            .arg(self.config.clients.to_string())
            .arg("--transactions")
            .arg(self.config.transactions.to_string())
            .arg("--no-vacuum")
            .arg("-l")
            .arg("--report-per-command")
            .arg("--log-prefix=pgbench_log")
            .arg("--file")
            .arg(&tmp_sql_path)
            .arg(&self.config.db_url)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let output = cmd.output()?;
        if !output.status.success() {
            let stdout_str = String::from_utf8_lossy(&output.stdout);
            let stderr_str = String::from_utf8_lossy(&output.stderr);
            bail!(
                "pgbench failed for {}.\nSTDOUT:\n{}\nSTDERR:\n{}",
                sql_file.display(),
                stdout_str,
                stderr_str
            );
        }

        // Very important to run this directly after the pgbench call, otherwise
        // any other metadata-gathering calls to the database will be factored in
        // to the total match count.
        let items_matched = self.fetch_total_items_matched_sync().unwrap_or(None);

        let stdout_str = String::from_utf8_lossy(&output.stdout).to_string();

        let intervals = Self::parse_aggregate_intervals(&stdout_str);
        let transaction_log = Self::parse_pgbench_transaction_logs_in_dir(scratch_path);

        // glean TPS + latency average from stdout
        let mut tps = None;
        let mut avg_latency_ms = None;
        for line in stdout_str.lines() {
            if let Some(pos) = line.find("tps =") {
                let tail = &line[pos + 5..].trim();
                if let Some(space_i) = tail.find(' ') {
                    if let Ok(val) = tail[..space_i].trim().parse::<f64>() {
                        tps = Some(val);
                    }
                }
            } else if let Some(pos) = line.find("latency average =") {
                let tail = &line[pos + 17..].trim();
                if let Some(space_i) = tail.find(' ') {
                    if let Ok(val) = tail[..space_i].trim().parse::<f64>() {
                        avg_latency_ms = Some(val);
                    }
                }
            }
        }

        let db_size_after = self.fetch_db_size().await.ok();
        let duration = start_instant.elapsed();

        // overall stats
        let latencies_us: Vec<i64> = transaction_log.iter().map(|t| t.latency_us).collect();
        let computed_latency_stats = compute_latency_stats_us(&latencies_us);

        // per-statement stats
        let statement_latency_details =
            Self::group_and_compute_per_statement_stats_us(&transaction_log);

        Ok(PgBenchTestResult {
            test_name: test_name.to_string(),
            sql_file: base_name,
            start_time_utc: start_utc,
            end_time_utc: Utc::now(),
            duration_seconds: duration.as_secs_f64(),
            tps,
            avg_latency_ms,
            db_size_before,
            //     db_size_after,
            db_size_after,
            top_statements: if self.pg_stat_statements_available() {
                self.fetch_top_statements_sync().unwrap_or_default()
            } else {
                vec![]
            },
            intervals,
            transaction_log,
            computed_latency_stats,
            statement_latency_details,
            items_matched,
            full_sql: sql_content,
        })
    }

    /// Orchestrate index creation + run .sql tests
    pub async fn run_all_benchmarks(&mut self) -> Result<()> {
        // get estimated row count
        let est = sqlx::query_scalar(
            r#"
            SELECT reltuples::bigint
            FROM pg_class
            WHERE relname = 'benchmark_eslogs';
            "#,
        )
        .fetch_one(self.conn_mut()?)
        .await
        .unwrap_or(0);
        self.report.reltuples = Some(est);

        // DB size at start
        let initial_size = self.fetch_db_size().await.unwrap_or("<Unknown>".into());
        self.report.db_size_before = Some(initial_size);

        // optional index
        if self.config.index {
            let idx_res = self.benchmark_index_creation().await?;
            self.report.index_creation_benchmark = Some(idx_res);
        }

        // run pgbench on each .sql file that was passed
        let sql_files = self.config.sql_files.clone();
        for path in sql_files {
            if path.extension().and_then(|ex| ex.to_str()) == Some("sql") {
                println!(
                    "Running pgbench for SQL \"{}\" ({} txns, concurrency={}) ...",
                    path.display(),
                    self.config.transactions,
                    self.config.clients
                );
                // pass a reference to the cloned `path`
                let test_res = self.run_single_sql_pgbench(&path, "pgsearch").await?;
                self.report.pgbench_tests.push(test_res);
            }
        }

        // DB size at end
        let final_size = self.fetch_db_size().await.unwrap_or("<Unknown>".into());
        self.report.db_size_after = Some(final_size);

        // done
        self.report.suite_finished_at = Some(Utc::now());
        self.insert_report().await?;
        println!(
            "All benchmarks complete; final JSON inserted into table \"{}\".\nGit hash: {:?}",
            self.config.report_table, self.report.extension_sha
        );
        Ok(())
    }

    async fn insert_report(&mut self) -> Result<()> {
        let json_val = serde_json::to_value(&self.report)?;
        let git_hash = self.report.extension_sha.clone().unwrap_or_default();

        let insert_sql = format!(
            "INSERT INTO {} (report_data, git_hash) VALUES ($1::jsonb, $2::text)",
            self.config.report_table
        );
        sqlx::query(&insert_sql)
            .bind(json_val)
            .bind(git_hash)
            .execute(self.conn_mut()?)
            .await?;
        Ok(())
    }
}
