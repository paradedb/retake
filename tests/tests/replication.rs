mod fixtures;

use anyhow::Result;
use cmd_lib::{run_cmd, run_fun};
use dotenvy::dotenv;
use fixtures::db::Query;
use rstest::*;
use sqlx::{Connection, PgConnection};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Once;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

// Static variables for initializing port assignment and ensuring one-time setup
static INIT: Once = Once::new();
static LAST_PORT: AtomicUsize = AtomicUsize::new(49152);

const RETRIES: u32 = 60;
const RETRY_DELAY: u64 = 1000; // measured in milliseconds

// Function to check if a port can be bound (i.e., is available)
fn can_bind(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
}

// Function to get a free port in the dynamic port range
fn get_free_port() -> u16 {
    let port_upper_bound = 65535;
    let port_lower_bound = 49152;

    INIT.call_once(|| {
        LAST_PORT.store(port_lower_bound, Ordering::SeqCst);
    });

    loop {
        let port = LAST_PORT.fetch_add(1, Ordering::SeqCst);
        if port > port_upper_bound {
            LAST_PORT.store(port_lower_bound, Ordering::SeqCst);
            continue;
        }

        if can_bind(port as u16) {
            return port as u16;
        }
    }
}

// Struct to manage an ephemeral PostgreSQL instance
struct EphemeralPostgres {
    pub tempdir_path: String,
    pub host: String,
    pub port: u16,
    pub dbname: String,
    pub pg_ctl_path: PathBuf,
}

// Implement Drop trait to ensure the PostgreSQL instance is properly stopped
impl Drop for EphemeralPostgres {
    fn drop(&mut self) {
        let path = &self.tempdir_path;
        let pg_ctl_path = &self.pg_ctl_path;
        run_cmd!($pg_ctl_path -D $path stop &> /dev/null)
            .unwrap_or_else(|_| println!("postgres instance at {} already shut down", self.port));
        std::fs::remove_dir_all(self.tempdir_path.clone()).unwrap();
    }
}

// Implementation of EphemeralPostgres
impl EphemeralPostgres {
    fn pg_bin_path() -> PathBuf {
        let pg_config_path = std::env::var("PG_CONFIG").expect(
            "PG_CONFIG variable must be set to enable creating ephemeral Postgres instances",
        );
        if !PathBuf::from(&pg_config_path).exists() {
            panic!("PG_CONFIG variable must a valid path to enable creating ephemeral Postgres instances, received {pg_config_path}");
        }
        match run_fun!($pg_config_path --bindir) {
            Ok(path) => PathBuf::from(path.trim().to_string()),
            Err(err) => panic!("could run pg_config --bindir to get Postgres bin folder: {err}"),
        }
    }

    fn pg_basebackup_path() -> PathBuf {
        Self::pg_bin_path().join("pg_basebackup")
    }

    fn initdb_path() -> PathBuf {
        Self::pg_bin_path().join("initdb")
    }

    fn pg_ctl_path() -> PathBuf {
        Self::pg_bin_path().join("pg_ctl")
    }

    fn new_from_initialized(
        tempdir_path: &Path,
        postgresql_conf: Option<&str>,
        pg_hba_conf: Option<&str>,
    ) -> Self {
        let tempdir_path = tempdir_path.to_str().unwrap().to_string();
        let port = get_free_port();
        let pg_ctl_path = Self::pg_ctl_path();

        // Write to postgresql.conf
        let config_content = match postgresql_conf {
            Some(config) => format!("port = {}\n{}", port, config.trim()),
            None => format!("port = {}", port),
        };
        let config_path = format!("{}/postgresql.conf", tempdir_path);
        std::fs::write(config_path, config_content).expect("Failed to write to postgresql.conf");

        // Write to pg_hba.conf
        if let Some(config_content) = pg_hba_conf {
            let config_path = format!("{}/pg_hba.conf", tempdir_path);

            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(config_path)
                .expect("Failed to open pg_hba.conf");

            writeln!(file, "{}", config_content).expect("Failed to append to pg_hba.conf");
        }

        // Create log directory
        let timestamp = chrono::Utc::now().timestamp_millis();
        let logfile = format!("/tmp/ephemeral_postgres_logs/{}.log", timestamp);
        std::fs::create_dir_all(Path::new(&logfile).parent().unwrap())
            .expect("Failed to create log directory");

        // Start PostgreSQL
        run_cmd!($pg_ctl_path -D $tempdir_path -l $logfile start)
            .expect("Failed to start Postgres");

        Self {
            // TempDir needs to be stored on the struct to avoid being dropped, otherwise the
            // temp folder will be deleted before the test finishes.
            tempdir_path,
            host: "localhost".to_string(),
            port,
            dbname: "postgres".to_string(),
            pg_ctl_path,
        }
    }

    fn new(postgresql_conf: Option<&str>, pg_hba_conf: Option<&str>) -> Self {
        // Make sure .env files are loaded before reading env vars.
        dotenv().ok();

        let init_db_path = Self::initdb_path();
        let tempdir = TempDir::new().expect("Failed to create temp dir");
        let tempdir_path = tempdir.into_path();

        // Initialize PostgreSQL data directory
        run_cmd!($init_db_path -D $tempdir_path &> /dev/null)
            .expect("Failed to initialize Postgres data directory");

        Self::new_from_initialized(tempdir_path.as_path(), postgresql_conf, pg_hba_conf)
    }

    // Method to establish a connection to the PostgreSQL instance
    async fn connection(&self) -> Result<PgConnection> {
        Ok(PgConnection::connect(&format!(
            "postgresql://{}:{}/{}",
            self.host, self.port, self.dbname
        ))
        .await?)
    }
}

#[rstest]
async fn test_ephemeral_postgres_with_pg_basebackup() -> Result<()> {
    let config = "
        wal_level = logical
        max_replication_slots = 4
        max_wal_senders = 4
        shared_preload_libraries = 'pg_search'
    ";

    let source_postgres = EphemeralPostgres::new(Some(config), None);
    let mut source_conn = source_postgres.connection().await?;
    let source_port = source_postgres.port;
    let source_username = "SELECT CURRENT_USER"
        .fetch_one::<(String,)>(&mut source_conn)
        .0;

    "CREATE TABLE text_array_table (
            id SERIAL PRIMARY KEY,
            text_array TEXT[]
        )"
    .execute(&mut source_conn);

    "INSERT INTO text_array_table (text_array) VALUES
        (ARRAY['apple', 'banana', 'cherry']),
        (ARRAY['dog', 'elephant', 'fox']),
        (ARRAY['grape', 'honeydew', 'kiwi']),
        (ARRAY['lion', 'monkey', 'newt']),
        (ARRAY['octopus', 'penguin', 'quail']),
        (ARRAY['rabbit', 'snake', 'tiger']),
        (ARRAY['umbrella', 'vulture', 'wolf']),
        (ARRAY['x-ray', 'yak', 'zebra']),
        (ARRAY['alpha', 'bravo', 'charlie']),
        (ARRAY['delta', 'echo', 'foxtrot'])"
        .execute(&mut source_conn);

    // Create pg_search extension and bm25 index
    "CREATE EXTENSION pg_search".execute(&mut source_conn);

    "
    CREATE INDEX text_array_table_idx ON text_array_table
    USING bm25 (id, text_array)
    WITH (key_field = 'id');
    "
    .execute(&mut source_conn);

    // Verify search results before pg_basebackup
    let source_results: Vec<(i32,)> = sqlx::query_as(
        "SELECT id FROM text_array_table WHERE text_array_table @@@ 'text_array:dog' ORDER BY id",
    )
    .fetch_all(&mut source_conn)
    .await?;
    assert_eq!(source_results.len(), 1);

    let target_tempdir = TempDir::new().expect("Failed to create temp dir");
    let target_tempdir_path = target_tempdir.into_path();

    // Permissions for the --pgdata directory passed to pg_basebackup
    // should be u=rwx (0700) or u=rwx,g=rx (0750)
    std::fs::set_permissions(
        target_tempdir_path.as_path(),
        std::fs::Permissions::from_mode(0o700),
    )
    .expect("couldn't set permissions on target_tempdir path");

    // Run pg_basebackup
    let pg_basebackup = EphemeralPostgres::pg_basebackup_path();
    run_cmd!($pg_basebackup --pgdata $target_tempdir_path --host localhost --port $source_port --username $source_username)
    .expect("Failed to run pg_basebackup");

    let target_postgres =
        EphemeralPostgres::new_from_initialized(target_tempdir_path.as_path(), Some(config), None);
    let mut target_conn = target_postgres.connection().await?;

    // Verify the content in the target database
    let target_results: Vec<(i32,)> = sqlx::query_as(
        "SELECT id FROM text_array_table WHERE text_array_table @@@ 'text_array:dog'",
    )
    .fetch_all(&mut target_conn)
    .await?;

    assert_eq!(source_results.len(), target_results.len());

    // Verify the table content
    let source_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM text_array_table")
        .fetch_one(&mut source_conn)
        .await?;
    let target_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM text_array_table")
        .fetch_one(&mut target_conn)
        .await?;

    assert_eq!(source_count, target_count);

    Ok(())
}

#[rstest] // Segfaults on PG13.
#[ignore = "failing on block storage"]
async fn test_replication_with_pg_search_only_on_replica() -> Result<()> {
    let config = "
        wal_level = logical
        max_replication_slots = 4
        max_wal_senders = 4
        shared_preload_libraries = 'pg_search'
    ";

    let source_postgres = EphemeralPostgres::new(Some(config), None);
    let target_postgres = EphemeralPostgres::new(Some(config), None);

    let mut source_conn = source_postgres.connection().await?;
    let mut target_conn = target_postgres.connection().await?;

    // Do not install pg_search on the source database

    // Create the mock_items table schema on the source
    let schema = "
        CREATE TABLE mock_items (
          id SERIAL PRIMARY KEY,
          description TEXT,
          rating INTEGER CHECK (rating BETWEEN 1 AND 5),
          category VARCHAR(255),
          in_stock BOOLEAN,
          metadata JSONB,
          created_at TIMESTAMP,
          last_updated_date DATE,
          latest_available_time TIME
        )
    ";
    schema.execute(&mut source_conn);

    // Create publication for replication
    "CREATE PUBLICATION mock_items_pub FOR TABLE mock_items".execute(&mut source_conn);

    // Install pg_search on the replica and create the same table schema
    "CREATE EXTENSION pg_search".execute(&mut target_conn);
    schema.execute(&mut target_conn);

    // Create the bm25 index on the description field on the replica
    "
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description)
    WITH (key_field = 'id');
    "
    .execute(&mut target_conn);

    // Create subscription on the replica
    format!(
        "CREATE SUBSCRIPTION mock_items_sub
         CONNECTION 'host={} port={} dbname={}'
         PUBLICATION mock_items_pub;",
        source_postgres.host, source_postgres.port, source_postgres.dbname
    )
    .execute(&mut target_conn);

    // Insert a new item into the source database
    "INSERT INTO mock_items (description, category, in_stock, latest_available_time, last_updated_date, metadata, created_at, rating)
    VALUES ('Green hiking shoes', 'Footwear', true, '16:00:00', '2024-07-11', '{}', '2024-07-11 16:00:00', 3)"
    .execute(&mut source_conn);

    // Verify the insert is replicated to the target database and can be searched using pg_search
    let target_results: Vec<(String,)> =
        "SELECT description FROM mock_items WHERE mock_items @@@ 'description:shoes' ORDER BY id"
            .fetch_retry(&mut target_conn, RETRIES, RETRY_DELAY, |result| {
                !result.is_empty()
            });

    assert_eq!(target_results.len(), 1);
    assert_eq!(target_results[0].0, "Green hiking shoes");

    Ok(())
}

#[rstest]
#[ignore = "not supported in community"]
async fn test_wal_streaming_replication_without_pg_search() -> Result<()> {
    // Primary Postgres setup + insert data
    let postgresql_conf = "
        listen_addresses = 'localhost'
        wal_level = replica
        max_wal_senders = 4
        shared_preload_libraries = 'pg_search'
    ";
    let pg_hba_conf = "
        host replication all 127.0.0.1/32 md5
        host replication all ::1/128 md5
    ";
    let source_postgres = EphemeralPostgres::new(Some(postgresql_conf), Some(pg_hba_conf));
    let mut source_conn = source_postgres.connection().await?;
    let source_port = source_postgres.port;
    let source_username = "replicator";

    "CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator_pass'"
        .execute(&mut source_conn);

    // Standby Postgres setup
    let postgresql_conf = "
        shared_preload_libraries = 'pg_search'
    ";
    let target_tempdir = TempDir::new().expect("Failed to create temp dir");
    let target_tempdir_path = target_tempdir.into_path();

    // Permissions for the --pgdata directory passed to pg_basebackup
    // should be u=rwx (0700) or u=rwx,g=rx (0750)
    std::fs::set_permissions(
        target_tempdir_path.as_path(),
        std::fs::Permissions::from_mode(0o700),
    )
    .expect("couldn't set permissions on target_tempdir path");

    // Run pg_basebackup
    let pg_basebackup = EphemeralPostgres::pg_basebackup_path();
    run_cmd!($pg_basebackup -D $target_tempdir_path -Fp -Xs -P -R -h localhost -U $source_username --port $source_port &> /dev/null)
        .expect("Failed to run pg_basebackup");

    let target_postgres = EphemeralPostgres::new_from_initialized(
        target_tempdir_path.as_path(),
        Some(postgresql_conf),
        None,
    );

    // Create the mock_items table schema on the source
    let schema = "
        CREATE EXTENSION pg_search;
        CALL paradedb.create_bm25_test_table(
            schema_name => 'public',
            table_name => 'mock_items'
        )
    ";
    schema.execute(&mut source_conn);

    thread::sleep(Duration::from_millis(1000));

    let mut target_conn = target_postgres.connection().await?;
    let target_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM mock_items")
        .fetch_one(&mut target_conn)
        .await?;

    assert_eq!(target_count, (41,));

    "
    CREATE INDEX search_idx ON mock_items
    USING bm25 (id, description, category, rating, in_stock, created_at, metadata)
    WITH (key_field = 'id');
    "
    .execute(&mut source_conn);

    thread::sleep(Duration::from_millis(1000));

    let rows: Vec<(i32,)> =
        "SELECT id FROM mock_items WHERE mock_items @@@ 'description:shoes' ORDER BY id"
            .fetch(&mut target_conn);

    assert_eq!(rows.len(), 3);

    Ok(())
}

#[rstest]
async fn test_physical_streaming_replication() -> Result<()> {
    // Create a unique directory for WAL archiving
    let archive_dir = TempDir::new().expect("Failed to create archive dir for WALs");
    // No need to set custom permissions, but you could if desired.

    // Adjust the archive_command to avoid file existence checks since this is a new directory.
    let primary_config = format!(
        "
        listen_addresses = 'localhost'
        wal_level = replica
        archive_mode = on
        archive_command = 'cp %p {}/%f'
        max_wal_senders = 3
        wal_keep_size = '160MB'
        shared_preload_libraries = 'pg_search'
        ",
        archive_dir.path().display()
    );

    let primary_pg_hba = "
        host replication replicator 127.0.0.1/32 md5
        host replication replicator ::1/128 md5
    ";

    // Step 1: Create and start the primary Postgres instance
    let primary_postgres = EphemeralPostgres::new(Some(&primary_config), Some(primary_pg_hba));
    let mut primary_conn = primary_postgres.connection().await?;

    // Create a replication user and test table on primary
    "CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator_pass';"
        .execute(&mut primary_conn);
    "CREATE EXTENSION pg_search;".execute(&mut primary_conn);
    "CREATE TABLE test_data (id SERIAL PRIMARY KEY, info TEXT);".execute(&mut primary_conn);

    // Insert initial data on primary
    "INSERT INTO test_data (info) VALUES ('initial');".execute(&mut primary_conn);

    let primary_port = primary_postgres.port;

    // Step 2: Create the standby using pg_basebackup
    let standby_tempdir = TempDir::new().expect("Failed to create temp dir for standby");
    std::fs::set_permissions(
        standby_tempdir.path(),
        std::fs::Permissions::from_mode(0o700),
    )?;

    let pg_basebackup = EphemeralPostgres::pg_basebackup_path();
    let standby_tempdir = standby_tempdir.path();
    run_cmd!(
        $pg_basebackup
        -D $standby_tempdir
        -Fp -Xs -P -R
        -h localhost
        -U replicator
        --port $primary_port
        &> /dev/null
    )
    .expect("Failed to run pg_basebackup for standby setup");

    let standby_config = "
        shared_preload_libraries = 'pg_search'
        hot_standby = on
    ";

    // Start the standby
    let standby_postgres =
        EphemeralPostgres::new_from_initialized(standby_tempdir, Some(standby_config), None);
    let mut standby_conn = standby_postgres.connection().await?;

    // Wait a moment for the standby to catch up
    std::thread::sleep(std::time::Duration::from_secs(2));

    // Verify that the initial data replicated
    let standby_data: Vec<(String,)> =
        "SELECT info FROM test_data"
            .fetch_retry(&mut standby_conn, 60, 1000, |result| !result.is_empty());

    assert_eq!(standby_data.len(), 1);
    assert_eq!(standby_data[0].0, "initial");

    // (Optional) Insert more data on primary and verify it appears on standby
    "INSERT INTO test_data (info) VALUES ('from_primary');".execute(&mut primary_conn);

    let standby_data: Vec<(String,)> = "SELECT info FROM test_data WHERE info='from_primary'"
        .fetch_retry(&mut standby_conn, 60, 1000, |result| !result.is_empty());

    assert_eq!(standby_data.len(), 1);

    // Insert a different value into the primary and ensure it streams over
    "INSERT INTO test_data (info) VALUES ('from_primary_2');".execute(&mut primary_conn);

    // Now, check for 'from_primary_2'
    let standby_data: Vec<(String,)> = "SELECT info FROM test_data WHERE info='from_primary_2'"
        .fetch_retry(&mut standby_conn, 60, 1000, |result| !result.is_empty());

    assert_eq!(standby_data.len(), 1);

    // Optional: Test synchronous replication
    // Reconfigure primary to require synchronous replication
    // This ensures commits wait for replication confirmation.
    "ALTER SYSTEM SET synchronous_standby_names = '*';".execute(&mut primary_conn);
    let pg_ctl_path = primary_postgres.pg_ctl_path.clone();
    let tempdir_path = primary_postgres.tempdir_path.clone();
    run_cmd!($pg_ctl_path -D $tempdir_path restart &> /dev/null)
        .expect("Failed to restart primary with sync config");

    // Reconnect after restart
    let mut primary_conn = primary_postgres.connection().await?;
    // Insert a row, then check standby to ensure synchronous commit.
    // If no connected standby matches, the commit on the primary will block indefinitely.
    "BEGIN; INSERT INTO test_data (info) VALUES ('sync_test'); COMMIT;".execute(&mut primary_conn);

    let sync_row: Vec<(String,)> = "SELECT info FROM test_data WHERE info='sync_test'".fetch_retry(
        &mut standby_conn,
        60,
        1000,
        |result| !result.is_empty(),
    );
    assert_eq!(sync_row.len(), 1);

    // Optional: Failover test - Stop primary and promote standby
    let pg_ctl_path = primary_postgres.pg_ctl_path.clone();
    let tempdir_path = primary_postgres.tempdir_path.clone();
    run_cmd!($pg_ctl_path -D $tempdir_path stop &> /dev/null).unwrap();

    // Promote standby using pg_ctl promote
    let tempdir_path = standby_postgres.tempdir_path.clone();
    let pg_ctl_path = standby_postgres.pg_ctl_path.clone();
    run_cmd!($pg_ctl_path -D $tempdir_path promote &> /dev/null)
        .expect("Failed to promote standby");

    thread::sleep(Duration::from_secs(2));
    let mut standby_conn = standby_postgres.connection().await?;
    "INSERT INTO test_data (info) VALUES ('promoted_standby');".execute(&mut standby_conn);

    // Ensure we can read back the inserted row from the now promoted standby
    let promoted_data: Vec<(String,)> = "SELECT info FROM test_data WHERE info='promoted_standby'"
        .fetch_retry(&mut standby_conn, 60, 1000, |result| !result.is_empty());
    assert_eq!(promoted_data.len(), 1);

    Ok(())
}

#[rstest]
#[ignore = "not supported in community"]
async fn test_wal_streaming_replication_with_pg_search() -> Result<()> {
    // Primary Postgres setup + insert data
    let postgresql_conf = "
        listen_addresses = 'localhost'
        wal_level = replica
        max_wal_senders = 4
        shared_preload_libraries = 'pg_search'
        # It's often helpful to have a short wal_keep_size or max_wal_senders for testing
    ";
    let pg_hba_conf = "
        host replication all 127.0.0.1/32 md5
        host replication all ::1/128 md5
    ";
    let source_postgres = EphemeralPostgres::new(Some(postgresql_conf), Some(pg_hba_conf));
    let mut source_conn = source_postgres.connection().await?;
    let source_port = source_postgres.port;
    let source_username = "replicator";

    // Create a replication user on primary
    format!(
        "CREATE USER {} WITH REPLICATION ENCRYPTED PASSWORD 'replicator_pass'",
        source_username
    )
    .execute(&mut source_conn);

    // Install pg_search on primary and create a test table
    "CREATE EXTENSION pg_search".execute(&mut source_conn);
    "CREATE TABLE items (
        id SERIAL PRIMARY KEY,
        description TEXT,
        category TEXT,
        created_at TIMESTAMP
    )"
    .execute(&mut source_conn);

    // Insert initial data
    "INSERT INTO items (description, category, created_at) VALUES
        ('Red running shoes', 'Footwear', NOW()),
        ('Blue sports shoes', 'Footwear', NOW()),
        ('Wireless headphones', 'Electronics', NOW()),
        ('4K television', 'Electronics', NOW())"
        .execute(&mut source_conn);

    // Create a bm25 index on the items table
    "
    CREATE INDEX items_search_idx ON items
    USING bm25 (id, description, category)
    WITH (key_field = 'id');
    "
    .execute(&mut source_conn);

    // Verify that searching on the primary works
    let source_results: Vec<(i32,)> =
        "SELECT id FROM items WHERE items @@@ 'description:shoes' ORDER BY id"
            .fetch(&mut source_conn);
    assert_eq!(source_results.len(), 2);

    // Set up the standby using pg_basebackup
    let target_tempdir = TempDir::new().expect("Failed to create temp dir for standby");
    let target_tempdir_path = target_tempdir.into_path();

    // Permissions for the --pgdata directory passed to pg_basebackup
    // should be u=rwx (0700) or u=rwx,g=rx (0750)
    std::fs::set_permissions(
        target_tempdir_path.as_path(),
        std::fs::Permissions::from_mode(0o700),
    )?;

    let pg_basebackup = EphemeralPostgres::pg_basebackup_path();
    run_cmd!(
        $pg_basebackup
        --pgdata $target_tempdir_path
        --host localhost
        --port $source_port
        --username $source_username
        -Fp -Xs -P -R
        &> /dev/null
    )
    .expect("Failed to run pg_basebackup for standby setup");

    // Start the standby also with pg_search preloaded
    let standby_config = "
        shared_preload_libraries = 'pg_search'
        hot_standby = on
    ";

    let standby_postgres = EphemeralPostgres::new_from_initialized(
        target_tempdir_path.as_path(),
        Some(standby_config),
        None,
    );
    let mut standby_conn = standby_postgres.connection().await?;

    // Wait for the standby to catch up
    // The fetch_retry helper is used in previous tests; you can adapt a similar approach here.
    let standby_data: Vec<(i32,)> =
        "SELECT id FROM items WHERE items @@@ 'description:shoes' ORDER BY id".fetch_retry(
            &mut standby_conn,
            60,
            1000,
            |result| !result.is_empty(),
        );

    assert_eq!(standby_data.len(), 2);

    // Insert a new item on the primary and verify it appears on the standby
    "INSERT INTO items (description, category, created_at)
     VALUES ('Green hiking shoes', 'Footwear', NOW())"
        .execute(&mut source_conn);

    let new_item_standby: Vec<(String,)> =
        "SELECT description FROM items WHERE items @@@ 'description:hiking' ORDER BY id"
            .fetch_retry(&mut standby_conn, 60, 1000, |result| !result.is_empty());

    assert_eq!(new_item_standby.len(), 1);
    assert_eq!(new_item_standby[0].0, "Green hiking shoes");

    // Test that we can still find electronics on the standby after inserts
    let electronics: Vec<(i32,)> =
        "SELECT id FROM items WHERE items @@@ 'category:Electronics' ORDER BY id"
            .fetch(&mut standby_conn);
    assert_eq!(electronics.len(), 2);

    Ok(())
}
