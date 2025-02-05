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

/// Convert a memory string like `"16GB"` into bytes (e.g. "16GB" -> 17179869184).
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

/// Convert a byte count into a human-friendly string (e.g. 12345678 -> "11.77MB").
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

/// Encapsulates min, max, mean, stddev, and p99 from a set of latencies.
#[derive(Debug, Serialize, Deserialize)]
pub struct LatencyStats {
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub stddev_ms: f64,
    pub p99_ms: f64,
}

/// A helper to compute LatencyStats from a set of latency measurements (in ms).
fn compute_latency_stats(latencies_ms: &[f64]) -> Option<LatencyStats> {
    if latencies_ms.is_empty() {
        return None;
    }

    let len = latencies_ms.len();
    let min_val = latencies_ms.iter().cloned().fold(f64::INFINITY, f64::min);
    let max_val = latencies_ms
        .iter()
        .cloned()
        .fold(f64::NEG_INFINITY, f64::max);

    // Mean
    let sum: f64 = latencies_ms.iter().sum();
    let mean = sum / len as f64;

    // Stddev
    let variance = latencies_ms
        .iter()
        .map(|&x| (x - mean) * (x - mean))
        .sum::<f64>()
        / len as f64;
    let stddev = variance.sqrt();

    // p99
    let mut sorted_vals = latencies_ms.to_vec();
    sorted_vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p99_index = ((len as f64) * 0.99).ceil() as usize - 1; // zero-based
    let p99_val = sorted_vals[std::cmp::min(p99_index, len - 1)];

    Some(LatencyStats {
        min_ms: min_val,
        max_ms: max_val,
        mean_ms: mean,
        stddev_ms: stddev,
        p99_ms: p99_val,
    })
}

/// Main configuration for the benchmark suite.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchmarkSuiteConfig {
    /// DB connection string, e.g. postgres://user@localhost/dbname
    pub db_url: String,

    /// Folder containing the `.sql` files to test
    pub sql_folder: PathBuf,

    /// Number of parallel clients (set to 1 for serial testing, as requested)
    pub clients: u32,

    /// Number of times each query is run
    pub transactions: u32,

    /// Additional CLI args to pass to pgbench
    pub pgbench_extra_args: Vec<String>,

    /// Whether to skip the custom index creation step
    pub skip_index: bool,

    /// The configured maintenance_work_mem setting (e.g. "16GB")
    pub maintenance_work_mem: String,

    /// The table in which to store our final JSON report
    pub report_table: String,
}

/// The top-level JSON report structure.
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
}

/// Info about how we handled maintenance_work_mem.
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

/// A sample of system resource usage (CPU, memory, disk I/O) during indexing.
#[derive(Debug, Serialize, Deserialize)]
pub struct SysinfoSample {
    pub timestamp: DateTime<Utc>,
    pub cpu_usage_percent: f64,
    pub total_memory_bytes: u64,
    pub used_memory_bytes: u64,
    pub total_read_bytes: u64,
    pub total_written_bytes: u64,
}

/// A single pgbench test result for a given `.sql` file.
#[derive(Debug, Serialize, Deserialize)]
pub struct PgBenchTestResult {
    pub test_name: String,
    pub sql_file: String,
    pub start_time_utc: DateTime<Utc>,
    pub end_time_utc: DateTime<Utc>,
    pub duration_seconds: f64,
    pub tps: Option<f64>,
    pub latency_ms: Option<f64>,
    pub db_size_before: Option<String>,
    pub db_size_after: Option<String>,

    /// If pg_stat_statements is available, the top statements by total time
    pub top_statements: Vec<TopStatement>,

    /// Snapshot of any "progress: X s, Y tps, lat Z ms" lines
    pub intervals: Vec<PgBenchLogInterval>,

    /// The raw transaction log lines (if we need per-transaction analysis)
    pub transaction_log: Vec<PgBenchTransactionLog>,

    /// The aggregated log intervals from `--aggregate-interval=1`
    pub aggregated_intervals: Vec<PgBenchAggregateIntervalRow>,

    /// Computed stats from per-transaction log
    pub computed_latency_stats: Option<LatencyStats>,
}

/// A row from pg_stat_statements (if available).
#[derive(Debug, Serialize, Deserialize)]
pub struct TopStatement {
    pub query: String,
    pub calls: i64,
    pub total_time_ms: f64,
    pub rows: i64,
}

/// A "progress:" line parse from stdout.
#[derive(Debug, Serialize, Deserialize)]
pub struct PgBenchLogInterval {
    pub time_sec: f64,
    pub n_xacts: u64,
    pub latency_avg_ms: f64,
    pub tps_in_interval: f64,
}

/// A 6-column line from the `-l` logs (per transaction).
#[derive(Debug, Serialize, Deserialize)]
pub struct PgBenchTransactionLog {
    pub client_id: u32,
    pub transaction_id: u32,
    pub time_secs: f64,
    pub latency_secs: f64,
    pub stmt_name: String,
}

/// A 15-column line from aggregated logs (`--aggregate-interval=...`).
#[derive(Debug, Serialize, Deserialize)]
pub struct PgBenchAggregateIntervalRow {
    pub interval_start: i64,
    pub num_transactions: i64,
    pub sum_latency: i64,
    pub sum_latency_2: i64,
    pub min_latency: i64,
    pub max_latency: i64,
    pub sum_lag: i64,
    pub sum_lag_2: i64,
    pub min_lag: i64,
    pub max_lag: i64,
    pub skipped: i64,
    pub retried: i64,
    pub retries: i64,
    pub serialization_failures: i64,
    pub deadlock_failures: i64,
}

/// Primary orchestrator for the benchmark workflow.
pub struct BenchmarkSuite {
    config: BenchmarkSuiteConfig,
    connection: Option<PgConnection>,
    report: BenchmarkReport,
}

impl BenchmarkSuite {
    /// Construct the suite and verify connectivity, versions, etc.
    pub async fn new(config: BenchmarkSuiteConfig) -> Result<Self> {
        // Force single-client concurrency if not already 1:
        // (We want pure per-query timings in serial, not concurrency.)
        let mut config = config;
        if config.clients != 1 {
            println!(
                "Note: Overriding concurrency to 1 (feedback: run queries in isolation). \
                 Original requested clients = {}",
                config.clients
            );
            config.clients = 1;
        }

        let conn_opts = PgConnectOptions::from_str(&config.db_url).context("Invalid DB URL")?;
        let mut conn = PgConnection::connect_with(&conn_opts)
            .await
            .context("Failed to connect to PostgreSQL")?;

        // Ensure the target table for storing the final JSON exists.
        Self::ensure_report_table_schema(&mut conn, &config.report_table).await?;

        // Check if pg_stat_statements is available.
        let stat_avail: bool = sqlx::query_scalar(
            r#"
              SELECT EXISTS(
                SELECT 1 FROM pg_available_extensions
                WHERE name='pg_stat_statements'
              );
            "#,
        )
        .fetch_one(&mut conn)
        .await
        .unwrap_or(false);

        if stat_avail {
            // Attempt to install pg_stat_statements if not present
            let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
                .execute(&mut conn)
                .await;
        }

        // Compute memory limit for maintenance_work_mem.
        let mut sys = System::new_all();
        sys.refresh_memory();
        let total_bytes = sys.total_memory() * 1024;
        let allowed_max = (total_bytes as f64 * 0.8) as u64;

        // Convert the user-supplied memory string into bytes, clamp it if needed.
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

        // Apply the effective setting for maintenance_work_mem
        let set_stmt = format!("SET maintenance_work_mem = '{}';", effective_str);
        sqlx::query(&set_stmt).execute(&mut conn).await?;

        // Verify the final setting:
        let (validated,): (String,) = sqlx::query_as("SHOW maintenance_work_mem;")
            .fetch_one(&mut conn)
            .await
            .context("Could not validate maintenance_work_mem")?;

        // Confirm connectivity:
        let (check_str,): (String,) = sqlx::query_as("SELECT 'Connection OK'::text")
            .fetch_one(&mut conn)
            .await
            .unwrap_or(("Connection Failed".into(),));

        let (extension_version, extension_sha, extension_build_mode) =
            Self::fetch_version_info(&mut conn).await?;

        // Initialize the top-level report:
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
        };

        Ok(Self {
            config,
            connection: Some(conn),
            report,
        })
    }

    /// Make sure the "report_data JSONB" column exists in the target table.
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
                report_data JSONB NOT NULL
            );
            "#
        );
        sqlx::query(&create_sql).execute(&mut *conn).await?;

        let columns = sqlx::query(
            r#"
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            "#,
        )
        .bind(schema)
        .bind(table)
        .fetch_all(conn)
        .await?;

        let mut found_jsonb = false;
        for row in columns {
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

    /// Return a mutable reference to our live PgConnection.
    fn conn_mut(&mut self) -> Result<&mut PgConnection> {
        self.connection
            .as_mut()
            .ok_or_else(|| anyhow!("No active DB connection available"))
    }

    /// True if pg_stat_statements is available for capturing top statements.
    fn pg_stat_statements_available(&self) -> bool {
        self.report.pg_stat_statements_available
    }

    /// Reset pg_stat_statements (if available).
    async fn reset_pg_stat_statements(&mut self) -> Result<()> {
        if self.pg_stat_statements_available() {
            sqlx::query("SELECT pg_stat_statements_reset()")
                .execute(self.conn_mut()?)
                .await?;
        }
        Ok(())
    }

    /// Fetch a user-friendly DB size string, e.g. "135 MB".
    async fn fetch_db_size(&mut self) -> Result<String> {
        let (sz,): (String,) = sqlx::query_as(
            "SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size",
        )
        .fetch_one(self.conn_mut()?)
        .await
        .context("Could not fetch database size")?;
        Ok(sz)
    }

    async fn fetch_version_info(
        conn: &mut PgConnection,
    ) -> Result<(Option<String>, Option<String>, Option<String>)> {
        let info: (Option<String>, Option<String>, Option<String>) =
            sqlx::query_as("SELECT version, githash, build_mode FROM paradedb.version_info();")
                .fetch_one(conn)
                .await
                .unwrap_or_default();
        Ok(info)
    }
    /// Attempt to retrieve a current_setting(...) from Postgres.

    async fn get_current_setting(&mut self, key: &str) -> Result<Option<String>> {
        let sql = format!("SELECT current_setting('{key}') AS val;");
        let result = sqlx::query_as::<_, (String,)>(&sql)
            .fetch_one(self.conn_mut()?)
            .await;
        match result {
            Ok((val,)) => Ok(Some(val)),
            Err(_) => Ok(None),
        }
    }

    /// Return the size of the named index, e.g. "352 kB".
    async fn fetch_index_size(&mut self, idx: &str) -> Result<String> {
        let sql = format!("SELECT pg_size_pretty(pg_relation_size('{idx}'))");
        let (sz,): (String,) = sqlx::query_as(&sql)
            .fetch_one(self.conn_mut()?)
            .await
            .context("Could not get index size")?;
        Ok(sz)
    }

    /// Return info from paradedb.index_info(idx) if available.
    async fn fetch_paradedb_index_info(&mut self, idx: &str) -> Result<Vec<serde_json::Value>> {
        let sql = format!(
            r#"
            SELECT row_to_json(x)
            FROM paradedb.index_info('{idx}') x
            "#,
        );
        let rows = sqlx::query_as::<_, (serde_json::Value,)>(sql.as_str())
            .fetch_all(self.conn_mut()?)
            .await
            .context("Could not query paradedb.index_info")?;
        Ok(rows.into_iter().map(|(val,)| val).collect())
    }

    /// Attempt to prewarm the given index via pg_prewarm.
    async fn try_prewarm_index(&mut self, idx: &str) -> Result<()> {
        let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_prewarm;")
            .execute(self.conn_mut()?)
            .await;
        let preload_sql = format!("SELECT pg_prewarm('{idx}');");
        let _ = sqlx::query(&preload_sql).execute(self.conn_mut()?).await;
        Ok(())
    }

    /// Build a custom search index, capturing system usage metrics in parallel.
    async fn benchmark_index_creation(&mut self) -> Result<IndexCreationBenchmarkResult> {
        let now_utc = Utc::now();
        println!("Starting custom index creation step ...");

        // Example: dropping an old index.
        sqlx::query("DROP INDEX IF EXISTS idx_benchmark_eslogs_bm25;")
            .execute(self.conn_mut()?)
            .await?;

        // Set some desired indexing-related settings.
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

        let stop_flag = Arc::new(AtomicBool::new(false));
        let sf = Arc::clone(&stop_flag);

        // Collect sysinfo samples in a dedicated thread until creation is done.
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

        // Example create index command:
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

        // Done, collect metrics.
        let duration = creation_start.elapsed();
        stop_flag.store(true, Ordering::SeqCst);

        // Join the background sampler.
        let samples = handle.join().unwrap_or_default();

        // Record the actual settings used.
        let mut index_settings = HashMap::new();
        for (k, _) in &desired_settings {
            if let Ok(Some(val)) = self.get_current_setting(k).await {
                index_settings.insert(k.to_string(), val);
            }
        }

        // Prewarm for subsequent steps.
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

    /// Grab top 5 statements from pg_stat_statements by total time, if available.
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

    /// Identify "progress: 1.0 s, 101.0 tps, lat 12.3 ms" lines in stdout.
    fn parse_aggregate_intervals(stdout: &str) -> Vec<PgBenchLogInterval> {
        let mut intervals = Vec::new();
        for line in stdout.lines() {
            if line.starts_with("progress:") {
                let parts: Vec<_> = line.split(',').collect();
                if parts.len() < 3 {
                    continue;
                }
                let time_part = parts[0].replace("progress:", "").replace("s", "");
                let time_sec: f64 = time_part.trim().parse().unwrap_or(0.0);

                let tps_part = parts[1].replace("tps", "");
                let tps_str: String = tps_part
                    .chars()
                    .filter(|c| c.is_numeric() || *c == '.')
                    .collect();
                let tps_val: f64 = tps_str.trim().parse().unwrap_or(0.0);

                let lat_part = parts[2].replace("lat", "").replace("ms", "");
                let lat_str: String = lat_part
                    .chars()
                    .filter(|c| c.is_numeric() || *c == '.')
                    .collect();
                let lat_val: f64 = lat_str.trim().parse().unwrap_or(0.0);

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

    /// Parse logs in a scratch directory for aggregated (15-col) and transaction (6-col) lines.
    fn parse_combined_logs_in_dir(
        log_dir: &Path,
    ) -> (Vec<PgBenchTransactionLog>, Vec<PgBenchAggregateIntervalRow>) {
        let mut tx_logs = Vec::new();
        let mut agg_rows = Vec::new();

        if let Ok(entries) = fs::read_dir(log_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let fname = path.file_name().unwrap_or_default().to_string_lossy();
                if !fname.starts_with("pgbench_log.") {
                    continue;
                }
                if let Ok(content) = fs::read_to_string(&path) {
                    for line in content.lines() {
                        let parts: Vec<_> = line.split_whitespace().collect();
                        // 15-column lines => aggregated intervals
                        if parts.len() == 15 {
                            if let Ok(row) = Self::parse_15_column_aggregated_line(&parts) {
                                agg_rows.push(row);
                            }
                        } else {
                            // maybe it's a CSV line with 6 columns => per-transaction
                            let csv_parts: Vec<_> = line.split(',').collect();
                            if csv_parts.len() == 6 {
                                if let Ok(tr) = Self::parse_6_column_transaction_line(&csv_parts) {
                                    tx_logs.push(tr);
                                }
                            }
                        }
                    }
                }
            }
        }
        (tx_logs, agg_rows)
    }

    /// Parse a 15-column aggregated interval line.
    fn parse_15_column_aggregated_line(parts: &[&str]) -> Result<PgBenchAggregateIntervalRow> {
        if parts.len() != 15 {
            bail!("Not a 15-column aggregated line");
        }
        Ok(PgBenchAggregateIntervalRow {
            interval_start: parts[0].parse::<i64>()?,
            num_transactions: parts[1].parse::<i64>()?,
            sum_latency: parts[2].parse::<i64>()?,
            sum_latency_2: parts[3].parse::<i64>()?,
            min_latency: parts[4].parse::<i64>()?,
            max_latency: parts[5].parse::<i64>()?,
            sum_lag: parts[6].parse::<i64>()?,
            sum_lag_2: parts[7].parse::<i64>()?,
            min_lag: parts[8].parse::<i64>()?,
            max_lag: parts[9].parse::<i64>()?,
            skipped: parts[10].parse::<i64>()?,
            retried: parts[11].parse::<i64>()?,
            retries: parts[12].parse::<i64>()?,
            serialization_failures: parts[13].parse::<i64>()?,
            deadlock_failures: parts[14].parse::<i64>()?,
        })
    }

    /// Parse a 6-column per-transaction line from pgbench's -l logs.
    fn parse_6_column_transaction_line(csv_parts: &[&str]) -> Result<PgBenchTransactionLog> {
        // Format: thread_id, client_id, xact_id, total_time_secs, latency_secs, stmt_name
        if csv_parts.len() != 6 {
            bail!("Not a 6-column transaction line");
        }
        let client_id = csv_parts[1].trim().parse::<u32>()?;
        let transaction_id = csv_parts[2].trim().parse::<u32>()?;
        let time_secs = csv_parts[3].trim().parse::<f64>()?;
        let latency_secs = csv_parts[4].trim().parse::<f64>()?;
        let stmt_name = csv_parts[5].trim().to_string();

        Ok(PgBenchTransactionLog {
            client_id,
            transaction_id,
            time_secs,
            latency_secs,
            stmt_name,
        })
    }

    fn transform_sql_to_prepared(user_sql: &str, mem_val: &str) -> String {
        // If user_sql contains multiple statements, that might cause an error.
        // But for single-statement queries, it works well.

        format!(
            "\
            SET maintenance_work_mem = '{mem_val}';\n\
            PREPARE my_statement AS\n{user_sql}\n;\n\
            EXECUTE my_statement;\n"
        )
    }

    /// Run pgbench for a single `.sql` test file, parse results, compute stats, etc.
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
            .unwrap_or("unknown_sql");

        // Read user-provided SQL script.
        let sql_content = fs::read_to_string(sql_file)
            .with_context(|| format!("Could not read file: {}", sql_file.display()))?;

        // We'll insert a small SET line and then the user script.
        let (tmp_sql_path, tmp_sql_file) =
            tempfile::NamedTempFile::new().and_then(|f| Ok((f.path().to_path_buf(), f)))?;
        {
            let mut writer = std::io::BufWriter::new(&tmp_sql_file);
            let mem_val = &self.report.maintenance_work_mem_info.effective_value;
            writeln!(writer, "SET maintenance_work_mem = '{}';", mem_val)?;
            writer.write_all(sql_content.as_bytes())?;
        }

        // Record DB size before
        let db_size_before = self.fetch_db_size().await.ok();

        // Reset pg_stat_statements (if available) so we see only this test's queries
        self.reset_pg_stat_statements().await?;

        // We'll store logs in a throwaway scratch dir
        let scratch_dir = tempfile::tempdir()?;
        let scratch_path = scratch_dir.path();

        // Prepare the pgbench invocation
        let mut cmd = Command::new("pgbench");
        cmd.current_dir(scratch_path)
            .arg("--client")
            .arg(self.config.clients.to_string())
            .arg("--transactions")
            .arg(self.config.transactions.to_string())
            .arg("--no-vacuum")
            // We'll capture each transaction log, plus aggregated intervals:
            .arg("-l")
            .arg("--aggregate-interval=1")
            .arg("--log-prefix=pgbench_log")
            // Insert any custom args (notably --protocol=prepared if not set)
            .args(&self.config.pgbench_extra_args)
            .arg("--file")
            .arg(&tmp_sql_path)
            // Finally, the DB URL
            .arg(&self.config.db_url)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // Execute
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

        // Parse stdout for TPS, average latency, etc.
        let stdout_str = String::from_utf8_lossy(&output.stdout).to_string();
        let intervals = Self::parse_aggregate_intervals(&stdout_str);

        // Parse logs for 6-col & 15-col lines
        let (transaction_log, aggregated_intervals) =
            Self::parse_combined_logs_in_dir(scratch_path);

        // Attempt to glean "tps = X" and "latency average = Y ms"
        let mut tps = None;
        let mut avg_latency = None;
        for line in stdout_str.lines() {
            if let Some(i) = line.find("tps =") {
                let tail = &line[i + 5..].trim();
                if let Some(space_i) = tail.find(' ') {
                    if let Ok(val) = tail[..space_i].trim().parse::<f64>() {
                        tps = Some(val);
                    }
                }
            } else if let Some(i) = line.find("latency average =") {
                let tail = &line[i + 17..].trim();
                if let Some(space_i) = tail.find(' ') {
                    if let Ok(val) = tail[..space_i].trim().parse::<f64>() {
                        avg_latency = Some(val);
                    }
                }
            }
        }

        // Attempt to gather top statements from pg_stat_statements
        let top_statements = if self.pg_stat_statements_available() {
            self.fetch_top_statements_sync()?
        } else {
            vec![]
        };

        // DB size after
        let db_size_after = self.fetch_db_size().await.ok();

        let duration = start_instant.elapsed();

        // Compute min/max/stddev/p99 from the per-transaction log
        let latencies: Vec<f64> = transaction_log
            .iter()
            .map(|t| t.latency_secs * 1000.0)
            .collect();
        let computed_latency_stats = compute_latency_stats(&latencies);

        Ok(PgBenchTestResult {
            test_name: test_name.into(),
            sql_file: base_name.into(),
            start_time_utc: start_utc,
            end_time_utc: Utc::now(),
            duration_seconds: duration.as_secs_f64(),
            tps,
            latency_ms: avg_latency,
            db_size_before,
            db_size_after,
            top_statements,
            intervals,
            transaction_log,
            aggregated_intervals,
            computed_latency_stats,
        })
    }

    /// Main entry point to run index creation (optionally) then all SQL tests.
    pub async fn run_all_benchmarks(&mut self) -> Result<()> {
        // Record DB size at start
        let initial_size = self.fetch_db_size().await.unwrap_or("<Unknown>".into());
        self.report.db_size_before = Some(initial_size);

        // If skipping index is false, do a custom index creation step:
        if !self.config.skip_index {
            let idx_result = self.benchmark_index_creation().await?;
            self.report.index_creation_benchmark = Some(idx_result);
        }

        // Now, run pgbench on each .sql found in the specified folder.
        let entries = fs::read_dir(&self.config.sql_folder)
            .with_context(|| format!("Could not read directory: {:?}", self.config.sql_folder))?;

        for entry in entries {
            let path = entry?.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("sql") {
                println!(
                    "Running pgbench for SQL file \"{}\" (run {} times, concurrency=1)",
                    path.display(),
                    self.config.transactions
                );
                let test_res = self.run_single_sql_pgbench(&path, "pgsearch").await?;
                self.report.pgbench_tests.push(test_res);
            }
        }

        // DB size at the end
        let final_size = self.fetch_db_size().await.unwrap_or("<Unknown>".into());
        self.report.db_size_after = Some(final_size);

        // Mark suite finished
        self.report.suite_finished_at = Some(Utc::now());

        // Insert JSON into the specified table
        self.insert_report().await?;
        println!(
            "All benchmarks complete; final JSON inserted into table \"{}\".\nGit hash: {:?}",
            self.config.report_table, self.report.extension_sha
        );
        Ok(())
    }

    /// Write the final JSON report to the specified Postgres table.
    async fn insert_report(&mut self) -> Result<()> {
        let val = serde_json::to_value(&self.report)?;
        let insert_sql = format!(
            "INSERT INTO {} (report_data) VALUES ($1::jsonb)",
            self.config.report_table
        );
        sqlx::query(&insert_sql)
            .bind(val)
            .execute(self.conn_mut()?)
            .await?;
        Ok(())
    }
}
