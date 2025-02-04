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

#![allow(dead_code)]
//! # ParadeDB Benchmarking (All-JSON + Full Index Tuning)
//!
//! This file demonstrates how to:
//! - Apply all recommended index tuning settings (ParadeDB + Postgres).
//! - Create the index, collect disk/memory usage, etc.
//! - Query index size & `paradedb.index_info` after creation.
//! - Insert all results in a single JSONB column.

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

/// Convert a memory string (e.g. "16GB") to bytes
fn parse_memory_str(s: &str) -> Option<u64> {
    let s = s.trim().to_uppercase();
    if s.ends_with("GB") {
        s.trim_end_matches("GB")
            .trim()
            .parse::<f64>()
            .ok()
            .map(|v| (v * 1024.0 * 1024.0 * 1024.0) as u64)
    } else if s.ends_with("MB") {
        s.trim_end_matches("MB")
            .trim()
            .parse::<f64>()
            .ok()
            .map(|v| (v * 1024.0 * 1024.0) as u64)
    } else if s.ends_with("KB") {
        s.trim_end_matches("KB")
            .trim()
            .parse::<f64>()
            .ok()
            .map(|v| (v * 1024.0) as u64)
    } else {
        s.parse::<u64>().ok()
    }
}

/// Convert bytes to a user-friendly string
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

/// The main config for our suite
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchmarkSuiteConfig {
    pub db_url: String,
    pub sql_folder: PathBuf,
    pub clients: u32,
    pub transactions: u32,
    pub pgbench_extra_args: Vec<String>,
    pub skip_index: bool,
    pub maintenance_work_mem: String,
    pub report_table: String,
}

/// Our top-level JSON
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkReport {
    suite_started_at: DateTime<Utc>,
    suite_finished_at: Option<DateTime<Utc>>,

    config: BenchmarkSuiteConfig,
    maintenance_work_mem_info: MaintenanceWorkMemInfo,
    pg_stat_statements_available: bool,
    connection_ok: bool,

    db_size_before: Option<String>,
    db_size_after: Option<String>,

    index_creation_benchmark: Option<IndexCreationBenchmarkResult>,
    pgbench_tests: Vec<PgBenchTestResult>,
}

/// Info on how we set maintenance_work_mem
#[derive(Debug, Serialize, Deserialize)]
pub struct MaintenanceWorkMemInfo {
    pub configured: String,
    pub total_system_memory: String,
    pub allowed_max_80pct: String,
    pub effective_value: String,
}

/// For the index creation step
#[derive(Debug, Serialize, Deserialize)]
pub struct IndexCreationBenchmarkResult {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub duration_seconds: f64,

    /// The final settings we tried to set or read
    pub index_settings: HashMap<String, String>,

    /// The final index size, e.g. "128 MB"
    pub index_size: Option<String>,

    /// The result of `SELECT row_to_json(...) FROM paradedb.index_info('idx_benchmark_eslogs_bm25')`
    pub index_info: Option<Vec<serde_json::Value>>,

    pub sysinfo_samples: Vec<SysinfoSample>,
}

/// Sysinfo sample
#[derive(Debug, Serialize, Deserialize)]
pub struct SysinfoSample {
    pub timestamp: DateTime<Utc>,
    pub cpu_usage_percent: f64,
    pub total_memory_bytes: u64,
    pub used_memory_bytes: u64,
    pub total_read_bytes: u64,
    pub total_written_bytes: u64,
}

/// A single pgbench test
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
    pub top_statements: Vec<TopStatement>,
}

/// A row from pg_stat_statements
#[derive(Debug, Serialize, Deserialize)]
pub struct TopStatement {
    pub query: String,
    pub calls: i64,
    pub total_time_ms: f64,
    pub rows: i64,
}

/// Our main suite
pub struct BenchmarkSuite {
    config: BenchmarkSuiteConfig,
    connection: Option<PgConnection>,
    report: BenchmarkReport,
}

impl BenchmarkSuite {
    /// Create suite
    pub async fn new(config: BenchmarkSuiteConfig) -> Result<Self> {
        // Connect
        let conn_opts = PgConnectOptions::from_str(&config.db_url).context("Invalid DB URL")?;
        let mut conn = PgConnection::connect_with(&conn_opts)
            .await
            .context("Failed to connect to Postgres")?;

        // ensure table
        Self::ensure_report_table_schema(&mut conn, &config.report_table).await?;

        // check pg_stat_statements
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
            let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_stat_statements;")
                .execute(&mut conn)
                .await;
        }

        // compute maintenance_work_mem
        let mut sys = System::new_all();
        sys.refresh_memory();
        let total_bytes = sys.total_memory() * 1024;
        let allowed_max = (total_bytes as f64 * 0.8) as u64;

        let configured = parse_memory_str(&config.maintenance_work_mem).ok_or_else(|| {
            anyhow!(
                "Failed to parse maintenance_work_mem: {}",
                config.maintenance_work_mem
            )
        })?;
        let effective = if configured > allowed_max {
            allowed_max
        } else {
            configured
        };
        let effective_str = format_bytes(effective);

        let set_stmt = format!("SET maintenance_work_mem = '{}';", effective_str);
        sqlx::query(&set_stmt).execute(&mut conn).await?;

        let (validated,): (String,) = sqlx::query_as("SHOW maintenance_work_mem;")
            .fetch_one(&mut conn)
            .await
            .context("Failed to validate maintenance_work_mem")?;

        // connection check
        let (check_str,): (String,) = sqlx::query_as("SELECT 'Connection OK'::text")
            .fetch_one(&mut conn)
            .await
            .unwrap_or(("Connection Failed".to_string(),));

        let report = BenchmarkReport {
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

    async fn ensure_report_table_schema(conn: &mut PgConnection, table_name: &str) -> Result<()> {
        let (schema, table) = if let Some(dot) = table_name.find('.') {
            (&table_name[..dot], &table_name[dot + 1..])
        } else {
            ("public", table_name)
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

        let rows = sqlx::query(
            r#"
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            "#,
        )
        .bind(schema)
        .bind(table)
        .fetch_all(&mut *conn)
        .await?;

        let mut found_jsonb = false;
        for row in rows {
            let col: &str = row.get("column_name");
            let dt: &str = row.get("data_type");
            if col == "report_data" && dt == "jsonb" {
                found_jsonb = true;
            }
        }
        if !found_jsonb {
            bail!("{} must have report_data JSONB", table_name);
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
        if self.pg_stat_statements_available() {
            sqlx::query("SELECT pg_stat_statements_reset()")
                .execute(self.conn_mut()?)
                .await?;
        }
        Ok(())
    }

    async fn fetch_db_size(&mut self) -> Result<String> {
        let (sz,): (String,) = sqlx::query_as(
            "SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size",
        )
        .fetch_one(self.conn_mut()?)
        .await
        .context("Failed to fetch DB size")?;
        Ok(sz)
    }

    /// Attempt to read "current_setting(key)" if superuser
    async fn get_current_setting(&mut self, key: &str) -> Result<Option<String>> {
        let sql = format!("SELECT current_setting('{key}') as val;");
        match sqlx::query_as::<_, (String,)>(sql.as_str())
            .fetch_one(self.conn_mut()?)
            .await
        {
            Ok((v,)) => Ok(Some(v)),
            Err(_) => Ok(None),
        }
    }

    /// fetch index size
    async fn fetch_index_size(&mut self, idx: &str) -> Result<String> {
        let sql = format!("SELECT pg_size_pretty(pg_relation_size('{idx}'))");
        let (sz,): (String,) = sqlx::query_as(&sql)
            .fetch_one(self.conn_mut()?)
            .await
            .context("Failed to get index size")?;
        Ok(sz)
    }

    /// fetch paradedb.index_info(...) as JSON array
    async fn fetch_paradedb_index_info(&mut self, idx: &str) -> Result<Vec<serde_json::Value>> {
        let sql = format!(
            r#"
            SELECT row_to_json(t)
            FROM paradedb.index_info('{idx}') t
            "#,
        );
        let rows = sqlx::query_as::<_, (serde_json::Value,)>(sql.as_str())
            .fetch_all(self.conn_mut()?)
            .await
            .context("Failed to query paradedb.index_info")?;
        Ok(rows.into_iter().map(|(val,)| val).collect())
    }

    /// We'll also try to create and use pg_prewarm extension.
    async fn try_prewarm_index(&mut self, idx: &str) -> Result<()> {
        let _ = sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_prewarm;")
            .execute(self.conn_mut()?)
            .await;
        // attempt to call pg_prewarm(...) on index
        let sql = format!("SELECT pg_prewarm('{idx}');");
        let _ = sqlx::query(&sql).execute(self.conn_mut()?).await;
        Ok(())
    }

    /// Actually do the index creation with all the relevant statements
    async fn benchmark_index_creation(&mut self) -> Result<IndexCreationBenchmarkResult> {
        let now_utc = Utc::now();
        println!("Starting index creation benchmark...");

        // drop old
        sqlx::query("DROP INDEX IF EXISTS idx_benchmark_eslogs_bm25;")
            .execute(self.conn_mut()?)
            .await?;

        // Settings we want to apply:
        // statement parallelism, memory, etc.
        // plus create_index_parallelism, etc.
        // Some might fail if not superuser.
        let desired_settings = [
            ("maintenance_work_mem", "16GB"),
            // For statement concurrency:
            ("paradedb.statement_parallelism", "1"),
            ("paradedb.statement_memory_budget", "15"), // MB
            // For create index concurrency:
            ("paradedb.create_index_parallelism", "8"),
            ("paradedb.create_index_memory_budget", "1024"), // MB
            // Parallel workers
            ("max_worker_processes", "16"),
            ("max_parallel_workers", "16"),
            ("max_parallel_workers_per_gather", "16"),
            // shared buffers
            ("shared_buffers", "8GB"),
            // custom scans
            ("paradedb.enable_custom_scan", "true"),
        ];

        // Try to set them
        for (k, v) in &desired_settings {
            let set_stmt = format!("SET {k} = '{v}';");
            let _ = sqlx::query(&set_stmt).execute(self.conn_mut()?).await;
        }

        // Start collecting sysinfo
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_clone = Arc::clone(&stop_flag);

        let creation_start = Instant::now();
        let handle = std::thread::spawn(move || -> Vec<SysinfoSample> {
            let mut sys = System::new_all();
            let mut samples = vec![];
            while !stop_flag_clone.load(Ordering::SeqCst) {
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

        // Create the index
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

        let duration = creation_start.elapsed();
        stop_flag.store(true, Ordering::SeqCst);
        let samples = handle.join().unwrap();

        // gather the final settings
        let mut index_settings = HashMap::new();
        for (k, _v) in &desired_settings {
            if let Ok(Some(val)) = self.get_current_setting(k).await {
                index_settings.insert(k.to_string(), val);
            }
        }

        // Attempt to create extension pg_prewarm, then prewarm the index
        let _ = self.try_prewarm_index("idx_benchmark_eslogs_bm25").await;

        // gather index size + info
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

    /// fetch top statements from pg_stat_statements
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
                       (total_plan_time + total_exec_time) as total_ms,
                       rows
                FROM pg_stat_statements
                ORDER BY (total_plan_time + total_exec_time) DESC
                LIMIT 5;
                "#,
            )
            .fetch_all(&mut *conn)
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

    /// run single .sql with pgbench
    async fn run_single_sql_pgbench(
        &mut self,
        sql_file: &Path,
        test_name: &str,
    ) -> Result<PgBenchTestResult> {
        let start_utc = Utc::now();
        let start_instant = Instant::now();

        let basefile = sql_file
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");

        // read the .sql
        let original_sql = fs::read_to_string(sql_file)
            .with_context(|| format!("Failed to read {}", sql_file.display()))?;

        // ephemeral file
        let (temp_path, temp_file) =
            tempfile::NamedTempFile::new().and_then(|f| Ok((f.path().to_path_buf(), f)))?;
        {
            let mut writer = std::io::BufWriter::new(&temp_file);
            let mem = &self.report.maintenance_work_mem_info.effective_value;
            writeln!(writer, "SET maintenance_work_mem = '{}';", mem)?;
            writer.write_all(original_sql.as_bytes())?;
        }

        let db_size_before = self.fetch_db_size().await.ok();

        // build command
        let mut cmd = Command::new("pgbench");
        cmd.arg("--client")
            .arg(self.config.clients.to_string())
            .arg("--transactions")
            .arg(self.config.transactions.to_string())
            .arg("--no-vacuum")
            .args(&self.config.pgbench_extra_args)
            .arg("--file")
            .arg(&temp_path)
            .arg(&self.config.db_url);

        let output = cmd.stdout(Stdio::piped()).stderr(Stdio::piped()).output()?;
        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!(
                "pgbench failed for {}:\nSTDOUT:\n{}\nSTDERR:\n{}",
                sql_file.display(),
                stdout,
                stderr
            );
        }
        let stdout_str = String::from_utf8_lossy(&output.stdout).to_string();
        let mut tps = None;
        let mut latency = None;
        for line in stdout_str.lines() {
            if line.starts_with("tps =") {
                if let Some(eq) = line.find('=') {
                    let after = &line[eq + 1..].trim();
                    if let Some(sp) = after.find(' ') {
                        if let Ok(val) = after[..sp].trim().parse::<f64>() {
                            tps = Some(val);
                        }
                    }
                }
            } else if line.starts_with("latency average =") {
                if let Some(eq) = line.find('=') {
                    let after = &line[eq + 1..].trim(); // e.g. "14.550 ms"
                    if let Some(sp) = after.find(' ') {
                        if let Ok(val) = after[..sp].trim().parse::<f64>() {
                            latency = Some(val);
                        }
                    }
                }
            }
        }

        let top_statements = if self.pg_stat_statements_available() {
            self.fetch_top_statements_sync()?
        } else {
            vec![]
        };
        let db_size_after = self.fetch_db_size().await.ok();
        let duration = start_instant.elapsed();

        Ok(PgBenchTestResult {
            test_name: test_name.into(),
            sql_file: basefile.into(),
            start_time_utc: start_utc,
            end_time_utc: Utc::now(),
            duration_seconds: duration.as_secs_f64(),
            tps,
            latency_ms: latency,
            db_size_before,
            db_size_after,
            top_statements,
        })
    }

    /// main entry point
    pub async fn run_all_benchmarks(&mut self) -> Result<()> {
        let before_size = self.fetch_db_size().await.unwrap_or("<Unknown>".into());
        self.report.db_size_before = Some(before_size);

        // optional index creation
        if !self.config.skip_index {
            let idx_res = self.benchmark_index_creation().await?;
            self.report.index_creation_benchmark = Some(idx_res);
        }

        // run .sql
        let dir_entries = fs::read_dir(&self.config.sql_folder)
            .with_context(|| format!("Failed to read folder {:?}", self.config.sql_folder))?;
        for entry in dir_entries {
            let path = entry?.path();
            if path.extension().and_then(|x| x.to_str()) == Some("sql") {
                self.reset_pg_stat_statements().await?;
                println!("Running pgbench for file: {}", path.display());
                let test_result = self.run_single_sql_pgbench(&path, "pgsearch").await?;
                self.report.pgbench_tests.push(test_result);
            }
        }

        let after_size = self.fetch_db_size().await.unwrap_or("<Unknown>".into());
        self.report.db_size_after = Some(after_size);

        self.report.suite_finished_at = Some(Utc::now());
        self.insert_report().await?;
        println!(
            "All benchmarks done; final JSON inserted into {}.",
            self.config.report_table
        );
        Ok(())
    }

    async fn insert_report(&mut self) -> Result<()> {
        let val = serde_json::to_value(&self.report)?;
        let q = format!(
            "INSERT INTO {} (report_data) VALUES ($1::jsonb)",
            self.config.report_table
        );
        sqlx::query(&q).bind(val).execute(self.conn_mut()?).await?;
        Ok(())
    }
}

// -----------------------------------------------------------------------------
// If you want to run it as a binary, uncomment below
// -----------------------------------------------------------------------------
/*
#[async_std::main]
async fn main() -> Result<()> {
    let config = BenchmarkSuiteConfig {
        db_url: "postgres://user:pass@localhost:5432/mydb".into(),
        sql_folder: "sql-pgsearch".into(),
        clients: 4,
        transactions: 100,
        pgbench_extra_args: vec!["--log".to_string(), "--report-per-command".to_string()],
        skip_index: false,
        maintenance_work_mem: "16GB".into(),
        report_table: "public.benchmarks".into(),
    };
    let mut suite = BenchmarkSuite::new(config).await?;
    suite.run_all_benchmarks().await?;
    Ok(())
}
*/
