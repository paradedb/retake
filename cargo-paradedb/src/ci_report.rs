use anyhow::{bail, Result};
use async_std::task::block_on;
use minijinja::Environment;
use serde_json::Value;
use sqlx::postgres::PgConnectOptions;
use crate::ci_json::BenchmarkSuite;
use sqlx::{Connection, PgConnection};
use std::fs;
use std::str::FromStr;

pub fn report_ci_suite(rev: &str, url: &str, table: &str) -> Result<()> {
    // 1) Connect to the DB
    let conn_opts = PgConnectOptions::from_str(url)?;
    let mut conn = block_on(PgConnection::connect_with(&conn_opts))?;

    // 2) Fetch the most recent JSON row with matching revision prefix
    let row = block_on(
        sqlx::query_as::<_, (Option<Value>,)>(&format!(
            "SELECT report_data
                 FROM {table}
                 WHERE git_hash LIKE ($1 || '%')
                 ORDER BY created_at DESC
                 LIMIT 1",
            table = table
        ))
        .bind(rev)
        .fetch_optional(&mut conn),
    )?;

    let Some((Some(json_report),)) = row else {
        bail!("No row found with revision ~ '{}'", rev);
    };

    // 3) Load the HTML file manually, then add it to our environment
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("templates")
        .join("report.html");
    let template_str = fs::read_to_string(&path)?;
    let mut env = Environment::new();
    // Add it under the name "report.html"
    env.add_template("report.html", &template_str)?;

    let tmpl = env.get_template("report.html")?;

    // 4) Render the HTML
    let rendered = tmpl.render(minijinja::context! {
        report => json_report
    })?;

    // 5) Print (or write) the resulting HTML
    println!("{}", rendered);
    Ok(())
}

/// Compare two JSON results (by revision) side-by-side in `compare.html`.
pub fn compare_ci_suites(url: &str) -> Result<()> {
    // Connect to the DB
    let conn_opts = PgConnectOptions::from_str(url)?;
    let mut conn = block_on(PgConnection::connect_with(&conn_opts))?;

    // Hard-coded fetch #1: from public.neon_results (pgBench)
    let (pgbench_data,) = block_on(
        sqlx::query_as::<_, (Value,)>(
            "SELECT report_data FROM public.neon_results LIMIT 1"
        )
        .fetch_one(&mut conn),
    )?;

    // Hard-coded fetch #2: from public.es_results (Rally)
    let (rally_data,) = block_on(
        sqlx::query_as::<_, (Value,)>(
            "SELECT report_data FROM public.es_results LIMIT 1"
        )
        .fetch_one(&mut conn),
    )?;

    // Parse the first as pgBench JSON:
    let suite1 = BenchmarkSuite::from_pgbench_json(&pgbench_data);

    // Parse the second as Rally JSON:
    let suite2 = BenchmarkSuite::from_rally_json(&rally_data);

    // Load the "compare.html" template
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("templates")
        .join("compare.html");
    let template_str = fs::read_to_string(&path)?;
    let mut env = Environment::new();
    env.add_template("compare.html", &template_str)?;
    let tmpl = env.get_template("compare.html")?;

    // Render the template with both suites in context
    let rendered = tmpl.render(minijinja::context! {
        pgbench_suite => suite1,
        rally_suite   => suite2
    })?;

    // Print to stdout
    println!("{}", rendered);
    Ok(())
}
