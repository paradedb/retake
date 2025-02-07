use anyhow::{Result, bail};
use async_std::task::block_on;
use serde_json::Value;
use sqlx::{Connection, PgConnection};
use sqlx::postgres::PgConnectOptions;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;

use minijinja::{Environment, Source};

pub fn report_ci_suite(git_hash: &str, url: &str, report_table: &str) -> Result<()> {
    // 1) Connect to the DB
    let conn_opts = PgConnectOptions::from_str(url)?;
    let mut conn = block_on(PgConnection::connect_with(&conn_opts))?;

    // 2) Fetch the most recent JSON row with matching git_hash prefix
    let row = block_on(
        sqlx::query_as::<_, (Option<Value>,)>(
            &format!(
                "SELECT report_data
                 FROM {table}
                 WHERE git_hash LIKE ($1 || '%')
                 ORDER BY created_at DESC
                 LIMIT 1",
                table = report_table
            )
        )
        .bind(git_hash)
        .fetch_optional(&mut conn),
    )?;

    let Some((Some(json_report),)) = row else {
        bail!("No row found with git_hash ~ '{}'", git_hash);
    };

    // 3) Load the Minijinja template from templates/report.html
    let mut env = Environment::new();
    let mut source = Source::new();
    source.load_path("templates")?;
    env.set_source(source);
    let tmpl = env.get_template("report.html")?;

    // 4) Render the HTML
    let rendered = tmpl.render(minijinja::context! {
        report => json_report
    })?;

    // 5) Print (or write) the resulting HTML
    println!("{}", rendered);
    Ok(())
}
