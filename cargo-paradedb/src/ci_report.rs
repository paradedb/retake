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
pub fn compare_ci_suites(
    rev1: &str,
    rev2: &str,
    url: &str,
    table: &str,
    out_file: &str,
) -> Result<()> {
    // 1) Connect to the DB
    let conn_opts = PgConnectOptions::from_str(url)?;
    let mut conn = block_on(PgConnection::connect_with(&conn_opts))?;

    // 2) Fetch row for rev1
    let row1 = block_on(
        sqlx::query_as::<_, (Option<Value>,)>(&format!(
            "SELECT report_data
             FROM {table}
             WHERE git_hash LIKE ($1 || '%')
             ORDER BY created_at DESC
             LIMIT 1"
        ))
        .bind(rev1)
        .fetch_optional(&mut conn),
    )?;
    let Some((Some(json1),)) = row1 else {
        anyhow::bail!("No row found with revision ~ '{}'", rev1);
    };

    // 3) Fetch row for rev2
    let row2 = block_on(
        sqlx::query_as::<_, (Option<Value>,)>(&format!(
            "SELECT report_data
             FROM {table}
             WHERE git_hash LIKE ($1 || '%')
             ORDER BY created_at DESC
             LIMIT 1"
        ))
        .bind(rev2)
        .fetch_optional(&mut conn),
    )?;
    let Some((Some(json2),)) = row2 else {
        anyhow::bail!("No row found with revision ~ '{}'", rev2);
    };

    // 4) Parse both JSON objects into BenchmarkSuite
    let suite1 = BenchmarkSuite::from_pgbench_json(&json1);
    let suite2 = BenchmarkSuite::from_pgbench_json(&json2);
    // Alternatively, if some are Rally data, you may want from_rally_json
    // or logic that picks the correct parser.

    // 5) Load the "compare.html" template
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("templates")
        .join("compare.html");
    let template_str = fs::read_to_string(&path)?;
    let mut env = Environment::new();
    env.add_template("compare.html", &template_str)?;
    let tmpl = env.get_template("compare.html")?;

    // 6) Render the template with both suites in context
    let rendered = tmpl.render(minijinja::context! {
        pgbench_suite => suite1,
        rally_suite   => suite2
    })?;

    // 7) Write to output file
    fs::write(out_file, rendered)?;
    println!("Wrote comparison report to {out_file}");
    Ok(())
}
