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

mod bench_hits;
mod benchmark;
mod ci_benchmark;
mod cli;
mod elastic;
mod subcommand;
mod tables;

use std::path::PathBuf;

use anyhow::Result;
use async_std::task::block_on;
use ci_benchmark::BenchmarkSuite;
use cli::{Cli, Corpus, EsLogsCommand, HitsCommand, Subcommand};
use dotenvy::dotenv;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Load env vars from a .env in any parent folder.
    dotenv().ok();

    let cli = Cli::default();
    match cli.subcommand {
        Subcommand::Install => subcommand::install(),
        Subcommand::Bench(bench) => match bench.corpus {
            Corpus::Eslogs(eslogs) => match eslogs.command {
                EsLogsCommand::Generate {
                    seed,
                    events,
                    table,
                    url,
                } => block_on(subcommand::bench_eslogs_generate(seed, events, table, url)),
                EsLogsCommand::BuildSearchIndex { table, index, url } => block_on(
                    subcommand::bench_eslogs_build_search_index(table, index, url),
                ),
                EsLogsCommand::QuerySearchIndex {
                    table,
                    query,
                    limit,
                    url,
                } => block_on(subcommand::bench_eslogs_query_search_index(
                    table, query, limit, url,
                )),
                EsLogsCommand::BuildGinIndex { table, index, url } => {
                    block_on(subcommand::bench_eslogs_build_gin_index(table, index, url))
                }
                EsLogsCommand::QueryGinIndex {
                    table,
                    query,
                    limit,
                    url,
                } => block_on(subcommand::bench_eslogs_query_gin_index(
                    table, query, limit, url,
                )),
                EsLogsCommand::BuildParquetTable { table, url } => {
                    block_on(subcommand::bench_eslogs_build_parquet_table(table, url))
                }
                EsLogsCommand::CountParquetTable { table, url } => {
                    block_on(subcommand::bench_eslogs_count_parquet_table(table, url))
                }
                EsLogsCommand::BuildElasticIndex {
                    table,
                    url,
                    elastic_url,
                } => block_on(subcommand::bench_eslogs_build_elastic_table(
                    elastic_url,
                    url,
                    table,
                )),
                EsLogsCommand::QueryElasticIndex {
                    elastic_url,
                    field,
                    term,
                } => block_on(subcommand::bench_eslogs_query_elastic_table(
                    elastic_url,
                    field,
                    term,
                )),
                EsLogsCommand::RunCiSuite {
                    sql,
                    url,
                    report,
                    skip_index,
                } => {
                    let db_url = url;
                    let sql_folder = PathBuf::from(sql.clone());

                    let config = ci_benchmark::BenchmarkSuiteConfig {
                        db_url,
                        sql_folder,
                        clients: 1,
                        transactions: 100,
                        pgbench_extra_args: vec![
                            "--log".to_string(),
                            "--report-per-command".to_string(),
                        ],
                        skip_index,
                        maintenance_work_mem: "16GB".into(),
                        report_table: report,
                    };

                    let mut suite = block_on(BenchmarkSuite::new(config))?;
                    block_on(suite.run_all_benchmarks())?;

                    Ok(())
                }
                EsLogsCommand::ReportCiSuite { git_hash, url, report } => {
                    use std::str::FromStr;
                    use sqlx::Connection;
                    use sqlx::postgres::PgConnectOptions;
                    use sqlx::PgConnection;
                    use serde_json::Value;

                    let conn_opts = PgConnectOptions::from_str(&url)?;
                    let mut conn = block_on(PgConnection::connect_with(&conn_opts))?;

                    // Grab the most recent row with the given git_hash
                    let row = block_on(
                        sqlx::query_as::<_, (Option<Value>,)>(
                            &format!(
                                "SELECT report_data
                                 FROM {table}
                                 WHERE git_hash LIKE ($1 || '%')
                                 ORDER BY created_at DESC
                                 LIMIT 1",
                                table = report
                            )
                        )
                        .bind(&git_hash)
                        .fetch_optional(&mut conn)
                    )?;

                    // Pretty-print the JSON if found
                    match row {
                        Some((Some(doc),)) => {
                            println!("{}", serde_json::to_string_pretty(&doc)?);
                        },
                        Some((None,)) => {
                            println!("No JSON found in that row!");
                        },
                        None => {
                            println!("No row found with git_hash = {}", git_hash);
                        }
                    }
                    Ok(())
                }
            },
            Corpus::Hits(hits) => match hits.command {
                HitsCommand::Run {
                    workload,
                    url,
                    full,
                } => block_on(bench_hits::bench_hits(&url, &workload, full)),
            },
        },
    }
}
