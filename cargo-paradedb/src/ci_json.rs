use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// A high-level suite of benchmarks from either pgBench-like JSON or
/// Elasticsearch Rally-like JSON.
///
/// This "intermediate representation" tries to gather as much relevant data
/// as possible into a single, common format that you can later compare or chart.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BenchmarkSuite {
    /// Arbitrary suite-level metadata that might come from
    /// pgBench, Rally, or both.
    pub suite_metadata: SuiteMetadata,

    /// A collection of test results by test name/key.
    /// For pgBench, `test_name` often corresponds to the `sql_file`.
    /// For Rally, it often corresponds to the top-level keys like `term`, `range`, etc.
    pub tests: HashMap<String, TestResult>,
}

/// Suite-level metadata that you might want to track, e.g. total DB size or
/// extension version, or the index size from Elasticsearch, etc.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SuiteMetadata {
    // Example: from pgBench
    pub extension_version: Option<String>,
    pub extension_build_mode: Option<String>,
    pub db_size_before: Option<String>,
    pub db_size_after: Option<String>,
    pub suite_started_at: Option<String>,
    pub suite_finished_at: Option<String>,

    // Example: from ES Rally
    pub store_size_gb: Option<f64>,
    pub dataset_size_gb: Option<f64>,
    pub segment_count: Option<u64>,
    // ... etc. Expand as needed
}

/// A single test (or scenario) result that has been normalized
/// from either a pgBench `pgbench_tests` entry or
/// an Elasticsearch Rally operation block.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TestResult {
    /// Logical name of the test, e.g. "range-field-conjunction", "term", etc.
    pub name: String,

    //--------------------------------------------------------
    // Throughput
    //--------------------------------------------------------
    /// From pgBench: "tps" (transactions per second).
    /// From Rally: often we interpret "Mean Throughput" as ops/s.
    pub tps: Option<f64>,

    /// Rally also has min/median/max throughput, which pgBench typically doesn't.
    pub min_throughput: Option<f64>,
    pub mean_throughput: Option<f64>,
    pub median_throughput: Option<f64>,
    pub max_throughput: Option<f64>,

    //--------------------------------------------------------
    // Latencies
    //--------------------------------------------------------
    /// pgBench's reported average latency in ms (avg_latency_ms).
    pub avg_latency_ms: Option<f64>,

    /// Common latencies in ms.
    /// From pgBench: "p99_us", "max_us", etc. after unit conversion (us -> ms).
    /// From Rally: "50th percentile latency", "90th percentile latency", etc.
    pub p50_latency_ms: Option<f64>,
    pub p90_latency_ms: Option<f64>,
    pub p99_latency_ms: Option<f64>,
    pub p999_latency_ms: Option<f64>,
    pub p9999_latency_ms: Option<f64>,
    pub max_latency_ms: Option<f64>,
    pub min_latency_ms: Option<f64>,
    pub mean_latency_ms: Option<f64>,
    pub stddev_latency_ms: Option<f64>,

    //--------------------------------------------------------
    // Additional Rally metrics: service time vs processing time
    //--------------------------------------------------------
    /// Rally also reports service-time and processing-time percentiles.
    /// If you want to track them, you can store them separately:
    pub p50_service_time_ms: Option<f64>,
    pub p90_service_time_ms: Option<f64>,
    pub p99_service_time_ms: Option<f64>,
    pub p100_service_time_ms: Option<f64>,

    pub p50_processing_time_ms: Option<f64>,
    pub p90_processing_time_ms: Option<f64>,
    pub p99_processing_time_ms: Option<f64>,
    pub p100_processing_time_ms: Option<f64>,

    //--------------------------------------------------------
    // Error rates, item counts, etc.
    //--------------------------------------------------------
    /// Rally has "error rate (%)", pgBench typically doesn't.
    pub error_rate_percent: Option<f64>,

    /// For pgBench, might store "items_matched".
    /// For Rally, might store doc counts if relevant.
    pub items_matched: Option<u64>,
    pub doc_count: Option<u64>,

    //--------------------------------------------------------
    // Possibly more fields from your environment
    //--------------------------------------------------------
    /// Some tests in pgBench contain intervals or other breakdown data.
    pub intervals: Option<Vec<Value>>,

    /// If you want to store the raw SQL or full query, you can do so:
    pub full_sql: Option<String>,
}

impl BenchmarkSuite {
    // ----------------------------------------------------------------
    //  PG-BENCH PARSER
    // ----------------------------------------------------------------

    /// Create a BenchmarkSuite from a JSON `Value` that matches
    /// the pgBench-style structure (like the large sample you posted).
    ///
    /// This method extracts as much as possible from the provided JSON,
    /// populates metadata, and populates a set of `TestResult`s keyed by
    /// the `sql_file`.
    pub fn from_pgbench_json(v: &Value) -> Self {
        let mut suite = BenchmarkSuite::default();

        // Top-level metadata
        suite.suite_metadata.extension_version = v
            .get("extension_version")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        suite.suite_metadata.extension_build_mode = v
            .get("extension_build_mode")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        suite.suite_metadata.db_size_before = v
            .get("db_size_before")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        suite.suite_metadata.db_size_after = v
            .get("db_size_after")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        suite.suite_metadata.suite_started_at = v
            .get("suite_started_at")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());
        suite.suite_metadata.suite_finished_at = v
            .get("suite_finished_at")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string());

        // The array of pgbench_tests
        if let Some(tests) = v.get("pgbench_tests").and_then(|x| x.as_array()) {
            for test_obj in tests {
                let mut test_result = TestResult::default();

                // The "name" we unify from "sql_file"
                if let Some(sql_file) = test_obj.get("sql_file").and_then(|x| x.as_str()) {
                    test_result.name = sql_file.to_string();
                }

                // TPS from "tps"
                test_result.tps = test_obj.get("tps").and_then(|x| x.as_f64());

                // avg_latency_ms
                test_result.avg_latency_ms =
                    test_obj.get("avg_latency_ms").and_then(|x| x.as_f64());

                // items_matched
                test_result.items_matched = test_obj.get("items_matched").and_then(|x| x.as_u64());

                // We can store the intervals if needed
                if let Some(intervals) = test_obj.get("intervals") {
                    test_result.intervals = Some(vec![intervals.clone()]);
                }

                // Possibly store "full_sql"
                if let Some(full_sql) = test_obj.get("full_sql").and_then(|x| x.as_str()) {
                    test_result.full_sql = Some(full_sql.to_string());
                }

                // Now parse the "computed_latency_stats"
                if let Some(stats) = test_obj.get("computed_latency_stats") {
                    // p99_us => p99_latency_ms
                    if let Some(p99_us) = stats.get("p99_us").and_then(|x| x.as_u64()) {
                        test_result.p99_latency_ms = Some(p99_us as f64 / 1000.0);
                    }
                    // min_us, max_us, mean_us, etc.
                    if let Some(min_us) = stats.get("min_us").and_then(|x| x.as_u64()) {
                        test_result.min_latency_ms = Some(min_us as f64 / 1000.0);
                    }
                    if let Some(max_us) = stats.get("max_us").and_then(|x| x.as_u64()) {
                        test_result.max_latency_ms = Some(max_us as f64 / 1000.0);
                    }
                    if let Some(mean_us) = stats.get("mean_us").and_then(|x| x.as_u64()) {
                        test_result.mean_latency_ms = Some(mean_us as f64 / 1000.0);
                    }
                    if let Some(stddev_us) = stats.get("stddev_us").and_then(|x| x.as_u64()) {
                        test_result.stddev_latency_ms = Some(stddev_us as f64 / 1000.0);
                    }
                }

                // We can store zero error rate as None or 0.
                // Typically pgBench doesn't track it, so let's leave it as None:
                test_result.error_rate_percent = None;

                // Add to our suite
                if !test_result.name.is_empty() {
                    suite.tests.insert(test_result.name.clone(), test_result);
                }
            }
        }

        suite
    }

    // ----------------------------------------------------------------
    //  ES RALLY PARSER
    // ----------------------------------------------------------------

    /// Create a BenchmarkSuite from an Elasticsearch Rally-style JSON `Value`.
    ///
    /// In Rally results, tests are top-level keys (e.g. "term", "range", "scroll", etc.),
    /// each with fields like "Mean Throughput", "100th percentile service time", etc.
    /// This function attempts to map them into the same `BenchmarkSuite` IR.
    pub fn from_rally_json(v: &Value) -> Self {
        let mut suite = BenchmarkSuite::default();

        // Example: parse some top-level fields for suite metadata, e.g. "Store size", "Dataset size"...
        // (In the large snippet, these appear in sections like `"" -> { "Store size": ... }` or "create-index" -> "Store size" etc.)
        // For demonstration, we look for a top-level path: `"" -> { "Store size": ..., "Dataset size": ... }`
        if let Some(default_obj) = v.get("") {
            if let Some(store_size) = default_obj.get("Store size").and_then(|x| x.get("value")) {
                suite.suite_metadata.store_size_gb = store_size.as_f64();
            }
            if let Some(dataset_size) = default_obj.get("Dataset size").and_then(|x| x.get("value"))
            {
                suite.suite_metadata.dataset_size_gb = dataset_size.as_f64();
            }
            if let Some(segment_count) = default_obj
                .get("Segment count")
                .and_then(|x| x.get("value"))
            {
                suite.suite_metadata.segment_count = segment_count.as_u64();
            }
        }

        // Now, each Rally operation is a top-level key (e.g. "term", "range", "scroll", "default", etc.).
        // We'll iterate over all top-level keys. If the value is an object with known fields,
        // we interpret them as a single `TestResult`.
        if let Some(obj) = v.as_object() {
            for (test_name, metrics_obj) in obj {
                // Possibly skip some "special" sections, e.g. "create-index", "bulk-insert", ...
                // or parse them specially if you want that data.
                // Here we'll show a skip example:
                let skip_list = ["create-index", "bulk-insert", ""];
                if skip_list.contains(&test_name.as_str()) {
                    continue;
                }

                // If we get here, we treat test_name as a test result
                if let Some(metrics_map) = metrics_obj.as_object() {
                    let mut test_result = TestResult::default();
                    test_result.name = test_name.clone();

                    // We try to parse throughput:
                    // Rally often has "Min Throughput", "Mean Throughput", etc. in docs/s or ops/s
                    test_result.min_throughput =
                        get_rally_metric_as_f64(metrics_map, "Min Throughput");
                    test_result.mean_throughput =
                        get_rally_metric_as_f64(metrics_map, "Mean Throughput");
                    test_result.median_throughput =
                        get_rally_metric_as_f64(metrics_map, "Median Throughput");
                    test_result.max_throughput =
                        get_rally_metric_as_f64(metrics_map, "Max Throughput");

                    // Because pgBench calls it "tps", let's store Rally's mean_throughput also in tps:
                    // (completely optional, your call how you unify them)
                    test_result.tps = test_result.mean_throughput;

                    // Rally also has "error rate" (value, in %)
                    test_result.error_rate_percent =
                        get_rally_metric_as_f64(metrics_map, "error rate");

                    // Next, parse latencies:
                    // Rally typically has "50th percentile latency" => p50_latency_ms, etc.
                    test_result.p50_latency_ms =
                        get_rally_metric_as_f64(metrics_map, "50th percentile latency");
                    test_result.p90_latency_ms =
                        get_rally_metric_as_f64(metrics_map, "90th percentile latency");
                    test_result.p99_latency_ms =
                        get_rally_metric_as_f64(metrics_map, "99th percentile latency");
                    // Rally also has "99.9th" or "99.99th" in some cases, if you want them:
                    test_result.p999_latency_ms =
                        get_rally_metric_as_f64(metrics_map, "99.9th percentile latency");
                    test_result.p9999_latency_ms =
                        get_rally_metric_as_f64(metrics_map, "99.99th percentile latency");
                    test_result.max_latency_ms =
                        get_rally_metric_as_f64(metrics_map, "100th percentile latency");

                    // Similarly for service time:
                    test_result.p50_service_time_ms =
                        get_rally_metric_as_f64(metrics_map, "50th percentile service time");
                    test_result.p90_service_time_ms =
                        get_rally_metric_as_f64(metrics_map, "90th percentile service time");
                    test_result.p99_service_time_ms =
                        get_rally_metric_as_f64(metrics_map, "99th percentile service time");
                    test_result.p100_service_time_ms =
                        get_rally_metric_as_f64(metrics_map, "100th percentile service time");

                    // And processing time:
                    test_result.p50_processing_time_ms =
                        get_rally_metric_as_f64(metrics_map, "50th percentile processing time");
                    test_result.p90_processing_time_ms =
                        get_rally_metric_as_f64(metrics_map, "90th percentile processing time");
                    test_result.p99_processing_time_ms =
                        get_rally_metric_as_f64(metrics_map, "99th percentile processing time");
                    test_result.p100_processing_time_ms =
                        get_rally_metric_as_f64(metrics_map, "100th percentile processing time");

                    // Insert into suite
                    suite.tests.insert(test_name.clone(), test_result);
                }
            }
        }

        suite
    }
}

/// A small helper function that tries to find something like:
/// `"Min Throughput": { "value": 9305.62, "Unit": "docs/s" }`
/// or simply `"Min Throughput": 9305.62` in the Rally metrics map,
/// returning it as f64 if available.
fn get_rally_metric_as_f64(metrics_map: &serde_json::Map<String, Value>, key: &str) -> Option<f64> {
    if let Some(val) = metrics_map.get(key) {
        // Sometimes it's directly a number
        if let Some(num) = val.as_f64() {
            return Some(num);
        }
        // Sometimes it's an object with "value"
        if let Some(obj) = val.as_object() {
            if let Some(vv) = obj.get("value").and_then(|x| x.as_f64()) {
                return Some(vv);
            }
        }
    }
    None
}

// ------------------------------------------------------------------
// EXAMPLE USAGE
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn example_pgbench_parsing() {
        // Minimal stub of a pgbench-like JSON:
        let pgbench_sample = json!({
          "extension_version": "0.15.1",
          "extension_build_mode": "debug",
          "db_size_before": "122 MB",
          "db_size_after": "121 MB",
          "suite_started_at": "2025-02-09T22:01:12.840603Z",
          "suite_finished_at": "2025-02-09T22:02:24.999197Z",
          "pgbench_tests": [
            {
              "tps": 96.245928,
              "full_sql": "SELECT * FROM t1;",
              "sql_file": "my_test_query",
              "intervals": [],
              "test_name": "pgsearch",
              "items_matched": 2,
              "avg_latency_ms": 10.39,
              "computed_latency_stats": {
                "max_us": 34725,
                "min_us": 8603,
                "p99_us": 24479,
                "mean_us": 10385,
                "stddev_us": 3124
              }
            }
          ]
        });

        let suite = BenchmarkSuite::from_pgbench_json(&pgbench_sample);
        assert_eq!(suite.tests.len(), 1);

        let test = suite.tests.get("my_test_query").unwrap();
        assert!((test.tps.unwrap() - 96.245928).abs() < 1e-6);
        assert_eq!(test.p99_latency_ms, Some(24.479));
        // etc.
    }

    #[test]
    fn example_rally_parsing() {
        // Minimal stub of a Rally-like JSON:
        let rally_sample = json!({
          "term": {
            "Min Throughput": { "value": 9305.62, "Unit": "docs/s" },
            "Mean Throughput": 12795.4,
            "Median Throughput": 12735.0,
            "Max Throughput": 13141.9,
            "50th percentile latency": 2.55485,
            "90th percentile latency": 3.26851,
            "99th percentile latency": 3.55275,
            "100th percentile latency": 3.64325,
            "50th percentile service time": 0.90141,
            "99th percentile service time": 1.19656,
            "error rate": 0
          },
          "": {
            "Store size": { "value": 52.9144, "Unit": "GB" },
            "Dataset size": { "value": 52.9144, "Unit": "GB" },
            "Segment count": { "value": 82, "Unit": "" }
          }
        });

        let suite = BenchmarkSuite::from_rally_json(&rally_sample);
        assert_eq!(suite.tests.len(), 1);

        // Check suite-level metadata
        assert_eq!(suite.suite_metadata.store_size_gb, Some(52.9144));
        assert_eq!(suite.suite_metadata.dataset_size_gb, Some(52.9144));
        assert_eq!(suite.suite_metadata.segment_count, Some(82));

        // Check the "term" test
        let test = suite.tests.get("term").unwrap();
        assert_eq!(test.min_throughput, Some(9305.62));
        assert_eq!(test.mean_throughput, Some(12795.4));
        assert_eq!(test.error_rate_percent, Some(0.0));
        assert_eq!(test.p50_latency_ms, Some(2.55485));
        assert_eq!(test.p99_service_time_ms, Some(1.19656));
        assert_eq!(test.p99_latency_ms, Some(3.55275));
    }
}

#[cfg(test)]
mod integration_test {
    use super::*;
    use async_std::test as async_test;
    use sqlx::{PgPool, Row};
    use anyhow::Result;
    use serde_json::Value;

    /// This test connects to a local Postgres instance at the hardcoded URL,
    /// fetches JSON from two tables, and verifies that `from_pgbench_json`
    /// and `from_rally_json` can parse them into `BenchmarkSuite`.
    #[async_test]
    async fn e2e_fetch_and_parse() -> Result<()> {
        // Hardcoded URL (replace if needed)
        let db_url = "postgres://neilhansen@localhost:28817/postgres";
        let pool = PgPool::connect(db_url).await?;

        // 1) Fetch pgBench JSON from public.neon_results
        let row = sqlx::query("SELECT neon_results FROM public.neon_results LIMIT 1")
            .fetch_one(&pool)
            .await?;
        let pgbench_json_str = row.try_get::<String, _>("neon_results")?;
        let pgbench_data: Value = serde_json::from_str(&pgbench_json_str)?;
        let pgbench_suite = BenchmarkSuite::from_pgbench_json(&pgbench_data);
        assert!(
            !pgbench_suite.tests.is_empty(),
            "pgBench suite should have tests"
        );

        // 2) Fetch Rally JSON from public.es_results
        let row = sqlx::query("SELECT data FROM public.es_results LIMIT 1")
            .fetch_one(&pool)
            .await?;
        let es_json_str = row.try_get::<String, _>("data")?;
        let es_data: Value = serde_json::from_str(&es_json_str)?;
        let rally_suite = BenchmarkSuite::from_rally_json(&es_data);
        assert!(
            !rally_suite.tests.is_empty(),
            "Rally suite should have tests"
        );

        Ok(())
    }
}
