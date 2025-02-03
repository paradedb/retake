#!/usr/bin/env bash
#
# run_pg_benchmarks_with_stats.sh (Cross-Platform iostat + pgbench --log-prefix)
#
# This script is an expanded version that:
#   - Runs pgbench on custom SQL files.
#   - Collects additional DB metrics (pg_stat_statements, DB size).
#   - Collects system resource usage with iostat in a cross-platform way.
#   - Stores per-transaction logs in a subdirectory via --log-prefix.
#   - Benchmarks index creation using detailed resource metrics.
#

###############################################################################
#                             CONFIGURATION                                   #
###############################################################################

# Connection string or rely on environment variables:
DB_URL="postgres://neilhansen@localhost:28817/postgres"

# Folders containing .sql queries:
FOLDER_PG_NATIVE="sql-pgnative"
FOLDER_PG_SEARCH="sql-pgsearch"

# Where to store logs:
LOG_DIR="pgbench-logs-with-stats"
SUMMARY_FILE="$LOG_DIR/benchmark_summary.txt"

# Subdirectory for the per-transaction logs (when using --log-prefix)
# e.g., logs will appear like "pgbench_logs/pgbench_log.<PID>"
PG_BENCH_LOG_SUBDIR="$LOG_DIR/pgbench_logs"

# Basic pgbench settings:
CLIENTS=4
TRANSACTIONS=1000
PG_BENCH_EXTRA_ARGS="--log --report-per-command"

# If you want time-based runs, you could do:
#   DURATION=30
# and use: pgbench -T "$DURATION" instead of -t "$TRANSACTIONS".

###############################################################################
#                 CROSS-PLATFORM SELECTION OF iostat COMMAND                 #
###############################################################################
OS="$(uname -s)"
if [ "$OS" = "Linux" ]; then
  SYSSTAT_COMMAND="iostat -x 1"
elif [ "$OS" = "Darwin" ]; then
  # macOS iostat doesn't support -x
  # -d for disk stats, -C for CPU, -w 1 is update frequency
  SYSSTAT_COMMAND="iostat -d -C -w 1"
else
  # Fallback for unknown OS
  SYSSTAT_COMMAND="iostat 1"
fi

# Where to capture system-level stats
SYSSTAT_LOG="$LOG_DIR/iostat.log"

###############################################################################
#                        HELPER FUNCTIONS                                     #
###############################################################################

run_single_benchmark() {
  local sqlfile="$1"
  local label="$2"
  local basefile
  basefile="$(basename "$sqlfile" .sql)"
  local out_prefix="$LOG_DIR/${label}_${basefile}"
  local out_log="${out_prefix}_pgbench.log"

  echo "--------------------------------------------------------------"
  echo "Running benchmark on: $sqlfile"
  echo " Label: $label / basefile: $basefile"
  echo " Clients: $CLIENTS, Transactions: $TRANSACTIONS"
  echo " Logging pgbench stdout/stderr to: $out_log"
  echo " Per-transaction logs will go into: $PG_BENCH_LOG_SUBDIR/pgbench_log.<PID>"
  echo "--------------------------------------------------------------"

  # Optionally reset pg_stat_statements before each single run (comment/uncomment):
  # psql "$DB_URL" -c "SELECT pg_stat_statements_reset();" > /dev/null 2>&1

  # Run pgbench
  pgbench \
    --client="$CLIENTS" \
    --transactions="$TRANSACTIONS" \
    --no-vacuum \
    $PG_BENCH_EXTRA_ARGS \
    --log-prefix="$PG_BENCH_LOG_SUBDIR/pgbench_log" \
    --file="$sqlfile" \
    "$DB_URL" \
    > "$out_log" 2>&1

  # Parse TPS line
  local tps_line
  tps_line="$(grep -E '^tps =' "$out_log" || true)"
  if [[ -n "$tps_line" ]]; then
    echo "[${label}-${basefile}] $tps_line" >> "$SUMMARY_FILE"
  else
    echo "[${label}-${basefile}] (No TPS line found; possibly an error?)" >> "$SUMMARY_FILE"
  fi

  # Parse average latency
  local lat_line
  lat_line="$(grep -E '^latency average =' "$out_log" || true)"
  if [[ -n "$lat_line" ]]; then
    echo "[${label}-${basefile}] $lat_line" >> "$SUMMARY_FILE"
  fi
  echo "" >> "$SUMMARY_FILE"

  # Optionally fetch top statements from pg_stat_statements for THIS run only
  # if you did a reset before. For example:
  echo "===== pg_stat_statements (Top 5 by total_time) for ${label}-${basefile} =====" >> "$SUMMARY_FILE"
  psql "$DB_URL" -c "
    SELECT query,
         calls,
         to_char(total_plan_time + total_exec_time, 'FM999999999.00') AS total_time,
         rows
    FROM pg_stat_statements
    ORDER BY (total_plan_time + total_exec_time) DESC
    LIMIT 5;
  " >> "$SUMMARY_FILE"
  echo "" >> "$SUMMARY_FILE"
}

benchmark_index_creation() {
  echo "----- Benchmarking Index Creation -----" >> "$SUMMARY_FILE"
  
  # Drop the index if it exists.
  echo "Dropping existing index idx_benchmark_eslogs_bm25 (if any)..." >> "$SUMMARY_FILE"
  psql "$DB_URL" -c "DROP INDEX IF EXISTS idx_benchmark_eslogs_bm25;" >/dev/null 2>&1

  # Start a system monitor using dstat to capture CPU, disk, network, etc.
  echo "Starting dstat to monitor resource usage during index creation..."
  dstat -tcnd --output "$LOG_DIR/dstat_index_creation.csv" 1 > /dev/null 2>&1 &
  local DSTAT_PID=$!

  # Run the CREATE INDEX command wrapped with GNU time to capture detailed resource metrics.
  echo "Creating index idx_benchmark_eslogs_bm25 and collecting resource metrics..." >> "$SUMMARY_FILE"
  local TIME_OUTPUT
  TIME_OUTPUT=$( { /usr/bin/time -v psql "$DB_URL" -c "CREATE INDEX idx_benchmark_eslogs_bm25
    ON public.benchmark_eslogs
    USING bm25 (
      id,
      process,
      cloud,
      aws_cloudwatch,
      agent,
      \"timestamp\",
      message,
      metrics_size,
      log_file_path
    )
    WITH (
      key_field = 'id',
      text_fields='{
        \"message\":        {},
        \"log_file_path\":  {}
      }',
      numeric_fields='{
        \"metrics_size\":   {}
      }',
      datetime_fields='{
        \"timestamp\":      {}
      }',
      json_fields='{
      	\"process\":        {},
      	\"cloud\":          {},
        \"aws_cloudwatch\": {},
        \"agent\":          {}
      }'
    );" ; } 2>&1 )
  echo "$TIME_OUTPUT" >> "$SUMMARY_FILE"

  # Stop the dstat monitor.
  echo "Stopping dstat (PID=$DSTAT_PID)..." >> "$SUMMARY_FILE"
  kill $DSTAT_PID 2>/dev/null

  echo "----- Index Creation Benchmark Complete -----" >> "$SUMMARY_FILE"
}

###############################################################################
#                          MAIN SCRIPT LOGIC                                  #
###############################################################################

echo "Creating $LOG_DIR ..."
mkdir -p "$LOG_DIR"

# Also create a subdirectory for per-transaction logs
mkdir -p "$PG_BENCH_LOG_SUBDIR"

echo "======================================================================" > "$SUMMARY_FILE"
echo "  PG Benchmarks Summary (with extra stats) on $(date)"                >> "$SUMMARY_FILE"
echo "  DB URL: $DB_URL"                                                   >> "$SUMMARY_FILE"
echo "  Clients: $CLIENTS, Transactions/Client: $TRANSACTIONS"             >> "$SUMMARY_FILE"
echo "======================================================================" >> "$SUMMARY_FILE"
echo ""                                                                   >> "$SUMMARY_FILE"

# 1. Quick check that we can connect
echo "Checking DB connection..."
psql "$DB_URL" -c "SELECT 'Connection OK' AS status;" || {
  echo "[FATAL] Unable to connect to database."
  exit 1
}

# 2. Collect DB size before
echo "===== Database size BEFORE the tests ====="       >> "$SUMMARY_FILE"
psql "$DB_URL" -c "
  SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size_before;
" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"

# 3. (Optional) pg_stat_statements reset for a clean slate
echo "Resetting pg_stat_statements..."
psql "$DB_URL" -c "SELECT pg_stat_statements_reset();" > /dev/null 2>&1

# 4. Launch a system-level stat collector in background
echo "Starting system-level stats collection with: $SYSSTAT_COMMAND"
$SYSSTAT_COMMAND > "$SYSSTAT_LOG" 2>&1 &
SYSSTAT_PID=$!

# 5. Run pgbench for pgnative folder
echo "********** BEGIN Benchmarking: $FOLDER_PG_NATIVE **********" >> "$SUMMARY_FILE"
for sql_file in "$FOLDER_PG_NATIVE"/*.sql; do
  [ -f "$sql_file" ] || continue
  run_single_benchmark "$sql_file" "pgnative"
done

# 6. Run pgbench for pgsearch folder
echo "" >> "$SUMMARY_FILE"
echo "********** BEGIN Benchmarking: $FOLDER_PG_SEARCH **********" >> "$SUMMARY_FILE"
for sql_file in "$FOLDER_PG_SEARCH"/*.sql; do
  [ -f "$sql_file" ] || continue
  run_single_benchmark "$sql_file" "pgsearch"
done

# 7. Stop system stats (iostat)
echo "Killing system stat process (PID=$SYSSTAT_PID)..."
kill $SYSSTAT_PID 2>/dev/null

# 8. Benchmark index creation with resource metrics
benchmark_index_creation

# 9. Collect DB size after
echo "===== Database size AFTER the tests ====="        >> "$SUMMARY_FILE"
psql "$DB_URL" -c "
  SELECT pg_size_pretty(pg_database_size(current_database())) AS db_size_after;
" >> "$SUMMARY_FILE"

# 10. Summarize final results
echo ""                                                >> "$SUMMARY_FILE"
echo "======================================================================" >> "$SUMMARY_FILE"
echo "All tests completed at: $(date)"                                    >> "$SUMMARY_FILE"
echo "Logs are in: $LOG_DIR"                                             >> "$SUMMARY_FILE"
echo "System-level stats are in: $SYSSTAT_LOG"                           >> "$SUMMARY_FILE"
echo "Per-transaction logs are in: $PG_BENCH_LOG_SUBDIR"                 >> "$SUMMARY_FILE"
echo "dstat output for index creation is in: $LOG_DIR/dstat_index_creation.csv" >> "$SUMMARY_FILE"
echo "======================================================================" >> "$SUMMARY_FILE"

echo "========================= DONE ========================="
echo "Benchmarking complete. See $SUMMARY_FILE for summary."
echo "System stats in $SYSSTAT_LOG"
echo "Per-transaction logs in $PG_BENCH_LOG_SUBDIR"
