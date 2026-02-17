//! Import orchestration - coordinates the entire import process

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use mysql::prelude::*;
use mysql::{Conn, LocalInfileHandler, Opts};
use tracing::{debug, info, warn};

use jibs_protocol::{
    framing::write_message, ClientMessage, ColumnDef, CompressionMode, ExecutionPlan,
    PreserveRule, ServerMessage, SetRule, Value,
};

use crate::progress::ImportProgress;
use crate::resolver;
use crate::server_binary;
use crate::ssh::{get_server_path, RemoteProcess, SshConfig, SshSession};

// ============================================================================
// Loader Pool - manages parallel MySQL connections for data loading
// ============================================================================

/// Work item for loader workers
struct LoadWork {
    table: String,
    columns: Arc<Vec<ColumnDef>>,
    data: Vec<u8>,
    result_tx: std::sync::mpsc::Sender<Result<u64>>,
}

/// Worker initialization result
enum WorkerInitResult {
    Ready,
    Failed(String),
}

/// Pool of loader workers for parallel data loading
struct LoaderPool {
    work_tx: std::sync::mpsc::Sender<LoadWork>,
    worker_handles: Vec<std::thread::JoinHandle<()>>,
}

impl LoaderPool {
    /// Create a new loader pool with N workers
    /// Returns an error if any worker fails to initialize
    fn new(mysql_url: &str, num_workers: usize) -> Result<Self> {
        // Use std sync channel for thread workers
        let (work_tx, work_rx) = std::sync::mpsc::channel::<LoadWork>();
        let work_rx = Arc::new(std::sync::Mutex::new(work_rx));

        // Channel for workers to report initialization status
        let (init_tx, init_rx) = std::sync::mpsc::channel::<(usize, WorkerInitResult)>();

        let mut worker_handles = Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            let url = mysql_url.to_string();
            let rx = Arc::clone(&work_rx);
            let init_reporter = init_tx.clone();

            let handle = std::thread::spawn(move || {
                // Connect to MySQL
                let opts = match Opts::from_url(&url) {
                    Ok(o) => o,
                    Err(e) => {
                        let msg = format!("Invalid MySQL URL: {}", e);
                        let _ = init_reporter.send((worker_id, WorkerInitResult::Failed(msg)));
                        return;
                    }
                };

                let mut conn = match Conn::new(opts) {
                    Ok(c) => c,
                    Err(e) => {
                        let msg = format!("Failed to connect: {}", e);
                        let _ = init_reporter.send((worker_id, WorkerInitResult::Failed(msg)));
                        return;
                    }
                };

                // Disable FK checks for this connection
                if let Err(e) = conn.query_drop("SET FOREIGN_KEY_CHECKS = 0") {
                    let msg = format!("Failed to disable FK checks: {}", e);
                    let _ = init_reporter.send((worker_id, WorkerInitResult::Failed(msg)));
                    return;
                }
                if let Err(e) = conn.query_drop("SET UNIQUE_CHECKS = 0") {
                    let msg = format!("Failed to disable unique checks: {}", e);
                    let _ = init_reporter.send((worker_id, WorkerInitResult::Failed(msg)));
                    return;
                }

                // Report successful initialization
                let _ = init_reporter.send((worker_id, WorkerInitResult::Ready));
                debug!("Loader worker {} connected", worker_id);

                // Process work items
                loop {
                    let work = {
                        let rx_guard = rx.lock().unwrap();
                        rx_guard.recv()
                    };

                    let work = match work {
                        Ok(w) => w,
                        Err(_) => break, // Channel closed
                    };

                    let LoadWork {
                        table,
                        columns,
                        data,
                        result_tx,
                    } = work;

                    // Load data
                    let result = load_tsv_data_with_conn(&mut conn, &table, &columns, &data);
                    let _ = result_tx.send(result);
                }

                debug!("Loader worker {} shutting down", worker_id);
            });

            worker_handles.push(handle);
        }

        // Drop our copy of init_tx so the channel can close if all workers report
        drop(init_tx);

        // Wait for all workers to report their initialization status
        // Use a timeout to avoid hanging if workers get stuck connecting
        let mut failed_workers: Vec<(usize, String)> = Vec::new();
        let mut ready_count = 0;
        let init_timeout = std::time::Duration::from_secs(30);

        for _ in 0..num_workers {
            match init_rx.recv_timeout(init_timeout) {
                Ok((worker_id, WorkerInitResult::Ready)) => {
                    ready_count += 1;
                    debug!("Worker {} ready ({}/{})", worker_id, ready_count, num_workers);
                }
                Ok((worker_id, WorkerInitResult::Failed(msg))) => {
                    failed_workers.push((worker_id, msg));
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // Worker didn't respond in time - likely stuck connecting
                    return Err(anyhow::anyhow!(
                        "Loader pool initialization timed out after {}s ({}/{} workers ready). \
                         Check MySQL connectivity to {}",
                        init_timeout.as_secs(),
                        ready_count,
                        num_workers,
                        mysql_url
                    ));
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    // Channel closed unexpectedly
                    return Err(anyhow::anyhow!(
                        "Loader pool initialization failed: channel closed ({}/{} workers ready)",
                        ready_count,
                        num_workers
                    ));
                }
            }
        }

        // If any workers failed, report the errors
        if !failed_workers.is_empty() {
            let error_msgs: Vec<String> = failed_workers
                .iter()
                .map(|(id, msg)| format!("Worker {}: {}", id, msg))
                .collect();
            return Err(anyhow::anyhow!(
                "Loader pool initialization failed ({}/{} workers ready):\n  {}",
                ready_count,
                num_workers,
                error_msgs.join("\n  ")
            ));
        }

        info!("All {} loader workers connected and ready", num_workers);

        Ok(Self {
            work_tx,
            worker_handles,
        })
    }

    /// Submit data for loading, returns a receiver for the result
    fn submit(
        &self,
        table: String,
        columns: Arc<Vec<ColumnDef>>,
        data: Vec<u8>,
    ) -> Result<std::sync::mpsc::Receiver<Result<u64>>> {
        let (result_tx, result_rx) = std::sync::mpsc::channel();

        self.work_tx
            .send(LoadWork {
                table,
                columns,
                data,
                result_tx,
            })
            .map_err(|_| anyhow::anyhow!("Loader pool shut down"))?;

        Ok(result_rx)
    }

    /// Wait for all workers to finish and shut down
    fn shutdown(self) {
        // Drop the sender to signal workers to stop
        drop(self.work_tx);

        // Wait for all workers
        for handle in self.worker_handles {
            let _ = handle.join();
        }
    }
}

/// Load TSV data with a provided connection (for worker pool)
fn load_tsv_data_with_conn(
    conn: &mut Conn,
    table: &str,
    columns: &[ColumnDef],
    tsv_data: &[u8],
) -> Result<u64> {
    use std::io::Write;

    if tsv_data.is_empty() {
        return Ok(0);
    }

    // Set up the local infile handler
    let data = tsv_data.to_vec();

    let handler = LocalInfileHandler::new(move |_file_name, local_infile| {
        local_infile.write_all(&data)?;
        Ok(())
    });

    conn.set_local_infile_handler(Some(handler));

    // Build column list
    let col_list: Vec<String> = columns.iter().map(|c| format!("`{}`", c.name)).collect();

    // Execute LOAD DATA LOCAL INFILE
    let load_sql = format!(
        r"LOAD DATA LOCAL INFILE 'data.tsv' INTO TABLE `{}` FIELDS TERMINATED BY '\t' ESCAPED BY '\\' LINES TERMINATED BY '\n' ({})",
        table,
        col_list.join(", ")
    );

    debug!("LOAD DATA SQL (worker): {}", load_sql);
    let result = conn.query_iter(&load_sql)?;
    let affected = result.affected_rows();

    Ok(affected)
}

// ============================================================================
// Import Configuration and Main Entry Point
// ============================================================================

/// Configuration for an import operation
pub struct ImportConfig {
    /// Path to the .jibs configuration file (None = import all tables)
    pub config_path: Option<PathBuf>,
    pub remote_host: String,
    pub remote_mysql: String,
    pub local_mysql: String,
    pub vars: HashMap<String, String>,
    pub var_file: Option<PathBuf>,
    pub resume: bool,
    pub clean: bool,
    pub parallel: usize,
    pub compression: CompressionMode,
    pub identity_file: Option<PathBuf>,
    pub ssh_port: u16,
    /// For `get` command: filter to specific aggregates with custom where clauses
    /// Each pair is (aggregate_name, where_clause)
    pub aggregate_overrides: Option<Vec<(String, String)>>,
    /// SSH host key verification mode
    pub host_key_verification: crate::ssh::HostKeyVerification,
    /// Maximum message size in bytes (default: 100MB)
    pub max_message_size: usize,
    /// Debug: simulate crash after N tables (for testing resume)
    #[cfg(feature = "test-utils")]
    pub fail_after_tables: Option<usize>,
}

/// Run the import process
pub async fn run_import(config: ImportConfig) -> Result<()> {
    info!("Starting import from {}", config.remote_host);

    // Create execution plan - either from config file or empty (import all tables)
    let mut plan = if let Some(config_path) = &config.config_path {
        // Load additional variables from file if specified
        let mut vars = config.vars.clone();
        if let Some(var_file) = &config.var_file {
            let content = std::fs::read_to_string(var_file)?;
            let file_vars: HashMap<String, serde_json::Value> = serde_json::from_str(&content)?;
            for (k, v) in file_vars {
                vars.entry(k).or_insert_with(|| match v {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                });
            }
        }

        // Detect file type by extension
        let extension = config_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        if extension == "json" {
            // Parse as JSON config
            crate::json_config::parse_json_config(config_path, &vars)
                .map_err(|e| anyhow::anyhow!("JSON config error: {}", e))?
        } else {
            // Parse as .jibs DSL
            let source = std::fs::read_to_string(config_path)?;
            let program = jibs_parser::parse(&source).map_err(|errors| {
                anyhow::anyhow!(
                    "Parse failed: {}",
                    errors
                        .iter()
                        .map(|e| e.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?;

            // Resolve the execution plan
            resolver::resolve(config_path, &program, &vars)
                .map_err(|e| anyhow::anyhow!("Resolution failed: {}", e))?
        }
    } else {
        // No config file - import all tables
        info!("No config file specified, importing all tables");
        ExecutionPlan::default()
    };

    // Apply aggregate overrides if this is a `get` command
    if let Some(overrides) = &config.aggregate_overrides {
        plan = apply_aggregate_overrides(plan, overrides)?;
    }

    info!(
        "Resolved plan: {} aggregates, {} relations, {} excluded tables",
        plan.aggregates.len(),
        plan.relations.len(),
        plan.excluded_tables.len()
    );

    // Connect to SSH
    let ssh_config = SshConfig::parse(
        &config.remote_host,
        config.ssh_port,
        config.identity_file.clone(),
        config.host_key_verification,
    )?;
    info!(
        "Connecting to {}@{}:{}",
        ssh_config.user, ssh_config.host, ssh_config.port
    );
    let session = SshSession::connect(ssh_config).await?;

    // Deploy server binary if needed
    let server_path = deploy_server(&session).await?;

    // Connect to local MySQL
    info!("Connecting to local MySQL: {}", config.local_mysql);
    let local_opts = Opts::from_url(&config.local_mysql)
        .map_err(|e| anyhow::anyhow!("Invalid local MySQL URL: {}", e))?;
    let mut local_conn = Conn::new(local_opts)?;

    // Check for existing state from a previous interrupted import
    let existing_backups = find_backup_tables(&mut local_conn)?;
    let has_checkpoint = checkpoint_exists(&mut local_conn)?;
    let has_previous_state = !existing_backups.is_empty() || has_checkpoint;

    if has_previous_state {
        if config.clean {
            // Clean up and start fresh
            info!("Cleaning up state from previous import");
            for backup_table in &existing_backups {
                local_conn.query_drop(format!("DROP TABLE `{}`", backup_table))?;
                info!("  Dropped {}", backup_table);
            }
            cleanup_checkpoint(&mut local_conn)?;
            if has_checkpoint {
                info!("  Dropped checkpoint table");
            }
        } else if !config.resume {
            // Error: previous state exists but not resuming or cleaning
            let mut state_parts = Vec::new();
            if !existing_backups.is_empty() {
                state_parts.push(format!("backup tables: {}", existing_backups.join(", ")));
            }
            if has_checkpoint {
                let completed = get_completed_tables(&mut local_conn)?;
                state_parts.push(format!("checkpoint ({} tables completed)", completed.len()));
            }
            return Err(anyhow::anyhow!(
                "Found state from a previous interrupted import:\n  {}\n\n\
                 Use --resume to continue the interrupted import, or\n\
                 Use --clean to discard the state and start fresh.",
                state_parts.join("\n  ")
            ));
        } else {
            let completed = get_completed_tables(&mut local_conn)?;
            info!(
                "Resuming import: {} tables already completed, {} backup tables",
                completed.len(),
                existing_backups.len()
            );
        }
    }

    // Disable foreign key checks for import
    local_conn.query_drop("SET FOREIGN_KEY_CHECKS = 0")?;
    local_conn.query_drop("SET UNIQUE_CHECKS = 0")?;

    // Create loader pool for parallel loading (if parallel > 1)
    let loader_pool = if config.parallel > 1 {
        info!("Creating loader pool with {} workers", config.parallel);
        Some(LoaderPool::new(&config.local_mysql, config.parallel)?)
    } else {
        None
    };

    // Start the remote server (credentials sent via protocol, not in process listing)
    info!("Starting remote server: {}", server_path);
    let mut server = session.start_process(&server_path).await?;

    // Send credentials via protocol (not visible in process listing)
    let creds_msg = ClientMessage::Credentials {
        mysql_url: config.remote_mysql.clone(),
    };
    send_message(&mut server, &creds_msg).await?;

    // Run the import protocol
    let result = run_protocol(
        &mut server,
        &mut local_conn,
        loader_pool,
        plan,
        config.compression,
        config.resume,
        config.max_message_size,
        #[cfg(feature = "test-utils")]
        config.fail_after_tables,
        #[cfg(not(feature = "test-utils"))]
        None,
        config.parallel as u32,
    )
    .await;

    // Re-enable checks
    local_conn.query_drop("SET FOREIGN_KEY_CHECKS = 1")?;
    local_conn.query_drop("SET UNIQUE_CHECKS = 1")?;

    // Handle result
    match result {
        Ok((stats, pool)) => {
            // Shutdown loader pool if used
            if let Some(pool) = pool {
                debug!("Shutting down loader pool");
                pool.shutdown();
            }
            info!(
                "Import complete: {} tables, {} rows",
                stats.tables_imported, stats.rows_imported
            );
            Ok(())
        }
        Err((e, pool)) => {
            // Shutdown loader pool if used
            if let Some(pool) = pool {
                pool.shutdown();
            }
            // Try to send shutdown
            let _ = send_message(&mut server, &ClientMessage::Shutdown).await;
            Err(e)
        }
    }
}

/// Import statistics
struct ImportStats {
    tables_imported: usize,
    rows_imported: u64,
}

/// Apply aggregate overrides for the `get` command
///
/// Filters the plan to only include the specified aggregates, and replaces
/// their where clauses with the provided overrides.
fn apply_aggregate_overrides(
    mut plan: ExecutionPlan,
    overrides: &[(String, String)],
) -> Result<ExecutionPlan> {
    let mut new_aggregates = Vec::new();

    for (agg_name, where_clause) in overrides {
        // Find the aggregate by name
        let original = plan
            .aggregates
            .iter()
            .find(|a| a.name == *agg_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Aggregate '{}' not found in config. Available aggregates: {}",
                    agg_name,
                    plan.aggregates
                        .iter()
                        .map(|a| a.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })?;

        // Clone and override the where clause
        let mut modified = original.clone();
        modified.where_clause = Some(where_clause.clone());
        new_aggregates.push(modified);
    }

    plan.aggregates = new_aggregates;
    Ok(plan)
}

/// Prefix for backup tables
const BACKUP_TABLE_PREFIX: &str = "_jibs_preserve_";

/// Name of the checkpoint table
const CHECKPOINT_TABLE: &str = "_jibs_checkpoint";

/// Name of the backup table used to preserve rows
fn preserve_backup_table(table: &str) -> String {
    format!("{}{}", BACKUP_TABLE_PREFIX, table)
}

/// Find all existing backup tables from a previous import
fn find_backup_tables(conn: &mut Conn) -> Result<Vec<String>> {
    let tables: Vec<String> = conn.query_map(
        format!(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE '{}%'",
            BACKUP_TABLE_PREFIX
        ),
        |table_name: String| table_name,
    )?;
    Ok(tables)
}

/// Check if checkpoint table exists
fn checkpoint_exists(conn: &mut Conn) -> Result<bool> {
    let exists: Option<String> = conn.query_first(format!(
        "SELECT TABLE_NAME FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
        CHECKPOINT_TABLE
    ))?;
    Ok(exists.is_some())
}

/// Create the checkpoint table
fn create_checkpoint_table(conn: &mut Conn) -> Result<()> {
    conn.query_drop(format!(
        "CREATE TABLE IF NOT EXISTS `{}` (
            table_name VARCHAR(255) PRIMARY KEY,
            row_count BIGINT UNSIGNED NOT NULL,
            completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
        CHECKPOINT_TABLE
    ))?;
    Ok(())
}

/// Get set of completed tables from checkpoint
fn get_completed_tables(conn: &mut Conn) -> Result<std::collections::HashSet<String>> {
    if !checkpoint_exists(conn)? {
        return Ok(std::collections::HashSet::new());
    }
    let tables: Vec<String> = conn.query_map(
        format!("SELECT table_name FROM `{}`", CHECKPOINT_TABLE),
        |name: String| name,
    )?;
    Ok(tables.into_iter().collect())
}

/// Mark a table as complete in the checkpoint
fn mark_table_complete(conn: &mut Conn, table: &str, row_count: u64) -> Result<()> {
    conn.query_drop(format!(
        "INSERT INTO `{}` (table_name, row_count) VALUES ('{}', {})",
        CHECKPOINT_TABLE, table, row_count
    ))?;
    Ok(())
}

/// Clean up the checkpoint table
fn cleanup_checkpoint(conn: &mut Conn) -> Result<()> {
    conn.query_drop(format!("DROP TABLE IF EXISTS `{}`", CHECKPOINT_TABLE))?;
    Ok(())
}

/// Run the import protocol with the remote server
async fn run_protocol(
    server: &mut RemoteProcess,
    local_conn: &mut Conn,
    loader_pool: Option<LoaderPool>,
    plan: ExecutionPlan,
    compression: CompressionMode,
    is_resume: bool,
    max_message_size: usize,
    fail_after_tables: Option<usize>,
    parallel: u32,
) -> std::result::Result<(ImportStats, Option<LoaderPool>), (anyhow::Error, Option<LoaderPool>)> {
    // Wrap the inner logic to handle errors while preserving the loader pool
    match run_protocol_inner(
        server,
        local_conn,
        &loader_pool,
        plan,
        compression,
        is_resume,
        max_message_size,
        fail_after_tables,
        parallel,
    )
    .await
    {
        Ok(stats) => Ok((stats, loader_pool)),
        Err(e) => Err((e, loader_pool)),
    }
}

/// Inner protocol implementation
async fn run_protocol_inner(
    server: &mut RemoteProcess,
    local_conn: &mut Conn,
    loader_pool: &Option<LoaderPool>,
    plan: ExecutionPlan,
    compression: CompressionMode,
    is_resume: bool,
    max_message_size: usize,
    fail_after_tables: Option<usize>,
    parallel: u32,
) -> Result<ImportStats> {
    let mut stats = ImportStats {
        tables_imported: 0,
        rows_imported: 0,
    };

    // Set up checkpointing
    create_checkpoint_table(local_conn)?;
    let completed_tables = if is_resume {
        get_completed_tables(local_conn)?
    } else {
        std::collections::HashSet::new()
    };

    // Send Init message
    debug!("Sending execution plan to server");
    let init_msg = ClientMessage::Init {
        plan: plan.clone(),
        compression,
        parallel,
    };
    send_message(server, &init_msg).await?;

    // Wait for Ready
    let ready_msg: ServerMessage = recv_message(server, max_message_size).await?;
    let (tables, negotiated_compression) = match ready_msg {
        ServerMessage::Ready {
            tables,
            compression,
        } => {
            debug!("Server ready: {} tables discovered", tables.len());
            (tables, compression)
        }
        ServerMessage::Error { message, .. } => {
            return Err(anyhow::anyhow!("Server error: {}", message));
        }
        other => {
            return Err(anyhow::anyhow!("Unexpected message: {:?}", other));
        }
    };

    // Create table info map for estimated rows lookup
    let table_info: std::collections::HashMap<String, u64> = tables
        .iter()
        .map(|t| (t.name.clone(), t.estimated_rows))
        .collect();

    // Initialize progress tracking
    let skipped_count = completed_tables.len();
    let mut progress = ImportProgress::new(&tables, skipped_count);

    // Send Start message
    debug!("Starting data transfer");
    let start_msg = ClientMessage::Start { resume_from: None };
    send_message(server, &start_msg).await?;

    // Track tables with preserved backups
    let mut tables_with_preserves: Vec<String> = Vec::new();

    // Track skipped tables (already completed in previous run)
    let mut skipped_tables: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Track schemas per table (for interleaved streaming)
    let mut table_schemas: HashMap<String, Arc<Vec<ColumnDef>>> = HashMap::new();

    // Track pending load results globally (not per-table) to avoid blocking at table boundaries
    // Each entry is (table_name, result_receiver)
    let mut pending_loads: Vec<(String, std::sync::mpsc::Receiver<Result<u64>>)> = Vec::new();

    // Maximum number of pending chunks before we start draining
    // This bounds memory usage while allowing cross-table parallelism
    const MAX_PENDING_CHUNKS: usize = 100;

    loop {
        let msg: ServerMessage = recv_message(server, max_message_size).await?;

        match msg {
            ServerMessage::Schema { table, columns } => {
                // Check if this table was already completed in a previous run
                if completed_tables.contains(&table) {
                    progress.skip_table(&table);
                    skipped_tables.insert(table.clone());
                    continue;
                }

                // Get estimated rows for progress tracking
                let estimated_rows = table_info.get(&table).copied().unwrap_or(0);
                progress.start_table(&table, estimated_rows);

                // Store schema for this table
                table_schemas.insert(table.clone(), Arc::new(columns.clone()));

                // Backup preserved rows BEFORE dropping the table
                let table_preserves: Vec<&PreserveRule> = plan
                    .preserves
                    .iter()
                    .filter(|p| p.table == table)
                    .collect();

                if !table_preserves.is_empty() {
                    if backup_preserved_rows(local_conn, &table, &table_preserves)? {
                        tables_with_preserves.push(table.clone());
                    }
                }

                // Get anonymization rules for this table
                let anon_rules = plan.anonymization.get(&table);

                // Create table in local MySQL (drops existing table)
                create_table(local_conn, &table, &columns, anon_rules)?;
            }

            ServerMessage::Data {
                table,
                row_count,
                tsv_data,
                checkpoint,
            } => {
                // Skip data for already-completed tables
                if skipped_tables.contains(&table) {
                    debug!("Skipping data chunk for {} (already completed)", table);
                    // Still need to send ack
                    let ack_msg = ClientMessage::Ack { checkpoint };
                    send_message(server, &ack_msg).await?;
                    continue;
                }

                let decompressed = maybe_decompress(tsv_data, negotiated_compression)?;
                let bytes_received = decompressed.len();

                debug!(
                    "Data chunk: {} rows, {} bytes for {}",
                    row_count, bytes_received, table
                );

                // Get schema for this table
                let schema = table_schemas
                    .get(&table)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("No schema for table {}", table))?;

                // Load data into MySQL - use pool if available
                if let Some(pool) = loader_pool {
                    // Submit to loader pool for parallel processing
                    let result_rx = pool.submit(table.clone(), schema, decompressed)?;
                    pending_loads.push((table.clone(), result_rx));

                    // If we have too many pending chunks, drain some to bound memory
                    // Use try_recv to collect completed loads without blocking
                    if pending_loads.len() > MAX_PENDING_CHUNKS {
                        let mut still_pending = Vec::new();
                        for (tbl, rx) in pending_loads.drain(..) {
                            match rx.try_recv() {
                                Ok(Ok(loaded)) => stats.rows_imported += loaded,
                                Ok(Err(e)) => {
                                    return Err(anyhow::anyhow!("Loader error for {}: {}", tbl, e));
                                }
                                Err(std::sync::mpsc::TryRecvError::Empty) => {
                                    // Still pending, keep it
                                    still_pending.push((tbl, rx));
                                }
                                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                    return Err(anyhow::anyhow!("Loader worker died for {}", tbl));
                                }
                            }
                        }
                        pending_loads = still_pending;

                        // If still too many, block on the oldest one
                        if pending_loads.len() > MAX_PENDING_CHUNKS {
                            if let Some((tbl, rx)) = pending_loads.first() {
                                match rx.recv() {
                                    Ok(Ok(loaded)) => stats.rows_imported += loaded,
                                    Ok(Err(e)) => {
                                        return Err(anyhow::anyhow!("Loader error for {}: {}", tbl, e));
                                    }
                                    Err(_) => {
                                        return Err(anyhow::anyhow!("Loader worker died for {}", tbl));
                                    }
                                }
                            }
                            pending_loads.remove(0);
                        }
                    }
                } else {
                    // Load directly (sequential)
                    let loaded = load_tsv_data(local_conn, &table, &schema, &decompressed)?;
                    stats.rows_imported += loaded;
                }

                // Update progress
                progress.update_table(&table, row_count, bytes_received);

                // Send ack
                let ack_msg = ClientMessage::Ack { checkpoint };
                send_message(server, &ack_msg).await?;
            }

            ServerMessage::TableDone { table, row_count } => {
                // Skip marking complete for already-completed tables
                if skipped_tables.contains(&table) {
                    debug!("Table {} was already complete", table);
                    continue;
                }

                // IMPORTANT: Before marking the table complete in checkpoint, we must ensure
                // all data chunks for THIS table have been loaded. Otherwise, if we crash
                // after marking complete but before loads finish, we'd skip incomplete data
                // on resume.
                //
                // Strategy:
                // 1. Wait (blocking) for all pending loads for THIS table
                // 2. Non-blocking drain of loads for OTHER tables (keep parallelism)
                {
                    let mut still_pending = Vec::new();
                    for (tbl, rx) in pending_loads.drain(..) {
                        if tbl == table {
                            // This table's chunk - must wait for it
                            match rx.recv() {
                                Ok(Ok(loaded)) => stats.rows_imported += loaded,
                                Ok(Err(e)) => {
                                    return Err(anyhow::anyhow!("Loader error for {}: {}", tbl, e));
                                }
                                Err(_) => {
                                    return Err(anyhow::anyhow!("Loader worker died for {}", tbl));
                                }
                            }
                        } else {
                            // Other table's chunk - try non-blocking
                            match rx.try_recv() {
                                Ok(Ok(loaded)) => stats.rows_imported += loaded,
                                Ok(Err(e)) => {
                                    return Err(anyhow::anyhow!("Loader error for {}: {}", tbl, e));
                                }
                                Err(std::sync::mpsc::TryRecvError::Empty) => {
                                    still_pending.push((tbl, rx));
                                }
                                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                    return Err(anyhow::anyhow!("Loader worker died for {}", tbl));
                                }
                            }
                        }
                    }
                    pending_loads = still_pending;
                }

                progress.finish_table(&table, row_count);
                stats.tables_imported += 1;

                // Clean up schema for this table
                table_schemas.remove(&table);

                // Mark table as complete in checkpoint
                // Safe to do now - all chunks for this table have been loaded
                mark_table_complete(local_conn, &table, row_count)?;

                // Debug: simulate crash for testing resume
                if let Some(fail_after) = fail_after_tables {
                    if stats.tables_imported >= fail_after {
                        return Err(anyhow::anyhow!(
                            "[DEBUG] Simulated crash after {} tables (--fail-after-tables)",
                            fail_after
                        ));
                    }
                }
            }

            ServerMessage::Done => {
                // Wait for all remaining pending loads to complete before finishing
                if !pending_loads.is_empty() {
                    debug!(
                        "Waiting for {} remaining pending loads before Done",
                        pending_loads.len()
                    );
                    for (tbl, rx) in pending_loads.drain(..) {
                        match rx.recv() {
                            Ok(Ok(loaded)) => stats.rows_imported += loaded,
                            Ok(Err(e)) => {
                                return Err(anyhow::anyhow!("Loader error for {}: {}", tbl, e));
                            }
                            Err(_) => {
                                return Err(anyhow::anyhow!("Loader worker died for {}", tbl));
                            }
                        }
                    }
                }

                progress.finish();
                break;
            }

            ServerMessage::Error {
                message,
                recoverable,
            } => {
                if recoverable {
                    progress.suspend(|| warn!("Recoverable server error: {}", message));
                } else {
                    return Err(anyhow::anyhow!("Server error: {}", message));
                }
            }

            ServerMessage::Ready { .. } => {
                return Err(anyhow::anyhow!("Unexpected Ready message"));
            }
        }
    }

    // Restore preserved rows from backup tables
    // On resume, we need to restore from any existing backup tables too
    let backup_tables = find_backup_tables(local_conn)?;
    if !backup_tables.is_empty() {
        info!("Restoring preserved rows for {} tables", backup_tables.len());
        for backup_table in &backup_tables {
            // Extract original table name from backup table name
            let table = backup_table.strip_prefix(BACKUP_TABLE_PREFIX).unwrap_or(backup_table);
            restore_preserved_rows(local_conn, table)?;
        }
    }

    // Run set (upsert) blocks
    if !plan.sets.is_empty() {
        info!("Executing {} set blocks", plan.sets.len());
        for set_rule in &plan.sets {
            execute_set_block(local_conn, set_rule)?;
        }
    }

    // Run after statements
    for statement in &plan.after_statements {
        info!("Running after statement: {}", statement);
        local_conn.query_drop(statement)?;
    }

    // Clean up checkpoint table on successful completion
    cleanup_checkpoint(local_conn)?;

    // Send shutdown
    send_message(server, &ClientMessage::Shutdown).await?;

    Ok(stats)
}

/// Send a message to the server
async fn send_message(server: &mut RemoteProcess, msg: &ClientMessage) -> Result<()> {
    let mut buffer = Vec::new();
    write_message(&mut buffer, msg)
        .map_err(|e| anyhow::anyhow!("Failed to serialize message: {}", e))?;
    server
        .write(&buffer)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send message: {}", e))?;
    Ok(())
}

/// Receive a message from the server
async fn recv_message(
    server: &mut RemoteProcess,
    max_message_size: usize,
) -> Result<ServerMessage> {
    // Read length prefix
    let mut len_bytes = [0u8; 4];
    server
        .read_exact(&mut len_bytes)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read message length: {}", e))?;
    let len = u32::from_le_bytes(len_bytes) as usize;

    if len > max_message_size {
        return Err(anyhow::anyhow!(
            "Message too large: {} bytes (max: {} bytes, ~{}MB). \
             Consider using --max-message-size to increase the limit.",
            len,
            max_message_size,
            max_message_size / (1024 * 1024)
        ));
    }

    // Read message body
    let mut buffer = vec![0u8; len];
    server
        .read_exact(&mut buffer)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read message body: {}", e))?;

    // Decode
    let (msg, _) = bincode::decode_from_slice(&buffer, jibs_protocol::framing::bincode_config())
        .map_err(|e| anyhow::anyhow!("Failed to decode message: {}", e))?;

    Ok(msg)
}

/// Deploy the server binary to the remote host if needed
async fn deploy_server(session: &SshSession) -> Result<String> {
    // Detect remote architecture
    let (code, arch_output, _) = session
        .exec("uname -m")
        .await
        .map_err(|e| anyhow::anyhow!("Failed to detect architecture: {}", e))?;

    if code != 0 {
        return Err(anyhow::anyhow!("Failed to detect remote architecture"));
    }

    let arch = arch_output.trim();
    debug!("Remote architecture: {}", arch);

    // Get the appropriate embedded binary
    let server_binary = server_binary::get_server_binary(arch);

    if let Some(binary) = server_binary {
        // Compute hash-based path for CAS deployment
        let server_path = get_server_path(binary);

        // Check if binary already exists at this path
        let (code, _, _) = session
            .exec(&format!("test -x {}", server_path))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to check for server: {}", e))?;

        if code == 0 {
            info!("Server already deployed: {}", server_path);
            return Ok(server_path);
        }

        // Upload the binary
        info!(
            "Uploading server binary ({} bytes) to {}",
            binary.len(),
            server_path
        );

        session
            .upload_file(binary, &server_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to upload server: {}", e))?;

        info!("Server deployed successfully");
        return Ok(server_path);
    }

    // No server available
    let available = server_binary::available_architectures();
    if available.is_empty() {
        Err(anyhow::anyhow!(
            "No embedded server binary available and jibs-server not found on remote host.\n\
             Build the server for Linux with:\n  \
             cross build -p jibs_server --release --target x86_64-unknown-linux-musl\n\
             Then rebuild the client to embed it."
        ))
    } else {
        Err(anyhow::anyhow!(
            "No server binary for architecture '{}'. Available: {:?}\n\
             jibs-server also not found on remote host.",
            arch,
            available
        ))
    }
}

/// Create a table in local MySQL based on schema
fn create_table(
    conn: &mut Conn,
    table: &str,
    columns: &[ColumnDef],
    anon_rules: Option<&Vec<jibs_protocol::AnonymizeRule>>,
) -> Result<()> {
    use jibs_protocol::AnonymizeTarget;

    // Drop existing table
    conn.query_drop(format!("DROP TABLE IF EXISTS `{}`", table))?;

    let mut column_defs = Vec::new();

    for col in columns {
        // Use full_type which includes the complete type definition
        // (e.g., "enum('a','b')", "varchar(255)", "int unsigned")
        let mut def = format!("`{}` {}", col.name, col.full_type);

        // Check if this column is being anonymized to NULL
        let is_anonymized_to_null = anon_rules
            .map(|rules| {
                rules
                    .iter()
                    .any(|r| r.column == col.name && matches!(r.target, AnonymizeTarget::Null))
            })
            .unwrap_or(false);

        // Make column nullable if not already or if being anonymized to NULL
        if !col.nullable && !is_anonymized_to_null {
            def.push_str(" NOT NULL");
        }

        if col.flags.auto_increment {
            def.push_str(" AUTO_INCREMENT");
        }

        column_defs.push(def);
    }

    // Add primary key
    let pk_cols: Vec<&str> = columns
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.as_str())
        .collect();

    if !pk_cols.is_empty() {
        column_defs.push(format!(
            "PRIMARY KEY ({})",
            pk_cols
                .iter()
                .map(|c| format!("`{}`", c))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let create_sql = format!(
        "CREATE TABLE `{}` (\n  {}\n)",
        table,
        column_defs.join(",\n  ")
    );

    debug!("Creating table: {}", create_sql);
    conn.query_drop(&create_sql)?;
    Ok(())
}

/// Load TSV data into a table using LOAD DATA LOCAL INFILE
fn load_tsv_data(
    conn: &mut Conn,
    table: &str,
    columns: &[ColumnDef],
    tsv_data: &[u8],
) -> Result<u64> {
    use std::io::Write;

    if tsv_data.is_empty() {
        return Ok(0);
    }

    // Set up the local infile handler
    let data = tsv_data.to_vec();

    let handler = LocalInfileHandler::new(move |_file_name, local_infile| {
        local_infile.write_all(&data)?;
        Ok(())
    });

    conn.set_local_infile_handler(Some(handler));

    // Build column list
    let col_list: Vec<String> = columns.iter().map(|c| format!("`{}`", c.name)).collect();

    // Execute LOAD DATA LOCAL INFILE
    // ESCAPED BY '\\' tells MySQL to interpret \N as NULL
    let load_sql = format!(
        r"LOAD DATA LOCAL INFILE 'data.tsv' INTO TABLE `{}` FIELDS TERMINATED BY '\t' ESCAPED BY '\\' LINES TERMINATED BY '\n' ({})",
        table,
        col_list.join(", ")
    );

    debug!("LOAD DATA SQL: {}", load_sql);
    let result = conn.query_iter(&load_sql)?;
    let affected = result.affected_rows();

    Ok(affected)
}

/// Decompress data if needed
fn maybe_decompress(data: Vec<u8>, compression: CompressionMode) -> Result<Vec<u8>> {
    match compression {
        CompressionMode::None | CompressionMode::Auto => Ok(data),
        CompressionMode::Zstd => {
            if data.len() < 4 {
                return Err(anyhow::anyhow!("Invalid compressed data"));
            }

            let uncompressed_len =
                u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

            let decompressed = zstd::decode_all(&data[4..])
                .map_err(|e| anyhow::anyhow!("Decompression failed: {}", e))?;

            if decompressed.len() != uncompressed_len {
                return Err(anyhow::anyhow!("Decompressed size mismatch"));
            }

            Ok(decompressed)
        }
    }
}

/// Execute a set (upsert) block
///
/// Logic:
/// 1. Check if a row matching the match_clause exists
/// 2. If found: UPDATE with the assignments
/// 3. If not found: INSERT with match_clause + assignments
fn execute_set_block(conn: &mut Conn, set_rule: &SetRule) -> Result<()> {
    // Build WHERE clause from match conditions
    let where_parts: Vec<String> = set_rule
        .match_clause
        .iter()
        .map(|a| format!("`{}` = {}", a.column, value_to_sql(&a.value)))
        .collect();
    let where_clause = where_parts.join(" AND ");

    // Check if row exists
    let select_sql = format!(
        "SELECT 1 FROM `{}` WHERE {} LIMIT 1",
        set_rule.table, where_clause
    );
    debug!("Set block check: {}", select_sql);

    let exists: Option<u8> = conn.query_first(&select_sql)?;

    if exists.is_some() {
        // Row exists - UPDATE
        if !set_rule.assignments.is_empty() {
            let set_parts: Vec<String> = set_rule
                .assignments
                .iter()
                .map(|a| format!("`{}` = {}", a.column, value_to_sql(&a.value)))
                .collect();

            let update_sql = format!(
                "UPDATE `{}` SET {} WHERE {}",
                set_rule.table,
                set_parts.join(", "),
                where_clause
            );
            debug!("Set block update: {}", update_sql);
            conn.query_drop(&update_sql)?;
            info!(
                "Updated row in {} where {}",
                set_rule.table, where_clause
            );
        }
    } else {
        // Row doesn't exist - INSERT
        let mut all_assignments: Vec<_> = set_rule.match_clause.iter().collect();
        all_assignments.extend(set_rule.assignments.iter());

        let columns: Vec<String> = all_assignments
            .iter()
            .map(|a| format!("`{}`", a.column))
            .collect();
        let values: Vec<String> = all_assignments
            .iter()
            .map(|a| value_to_sql(&a.value))
            .collect();

        let insert_sql = format!(
            "INSERT INTO `{}` ({}) VALUES ({})",
            set_rule.table,
            columns.join(", "),
            values.join(", ")
        );
        debug!("Set block insert: {}", insert_sql);
        conn.query_drop(&insert_sql)?;
        info!(
            "Inserted row into {} with {}",
            set_rule.table, where_clause
        );
    }

    Ok(())
}

/// Convert a Value to SQL literal
fn value_to_sql(value: &Value) -> String {
    match value {
        Value::String(s) => {
            // Escape single quotes
            let escaped = s.replace('\'', "''");
            format!("'{}'", escaped)
        }
        Value::StringArray(arr) => {
            // Convert array to comma-separated quoted strings
            arr.iter()
                .map(|s| {
                    let escaped = s.replace('\'', "''");
                    format!("'{}'", escaped)
                })
                .collect::<Vec<_>>()
                .join(", ")
        }
        Value::Int(i) => i.to_string(),
        Value::IntArray(arr) => arr
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(", "),
        Value::Float(f) => f.to_string(),
        Value::FloatArray(arr) => arr
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(", "),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        Value::BoolArray(arr) => arr
            .iter()
            .map(|b| if *b { "1" } else { "0" })
            .collect::<Vec<_>>()
            .join(", "),
        Value::Null => "NULL".to_string(),
    }
}

/// Backup rows matching preserve rules to a backup table before the main table is dropped.
/// Returns true if a backup exists (either created now or from a previous run).
///
/// On resume: uses existing backup table if present.
/// On fresh start: creates new backup table.
fn backup_preserved_rows(
    conn: &mut Conn,
    table: &str,
    preserve_rules: &[&PreserveRule],
) -> Result<bool> {
    let backup_table = preserve_backup_table(table);

    // Check if backup table already exists (resume scenario)
    let backup_exists: Option<String> = conn.query_first(format!(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
        backup_table
    ))?;

    if backup_exists.is_some() {
        let count: Option<u64> = conn.query_first(format!("SELECT COUNT(*) FROM `{}`", backup_table))?;
        debug!(
            "Using existing backup {} ({} rows)",
            backup_table,
            count.unwrap_or(0)
        );
        return Ok(true);
    }

    // Check if the source table exists
    let table_exists: Option<String> = conn.query_first(format!(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
        table
    ))?;

    if table_exists.is_none() {
        debug!("Table {} doesn't exist locally, nothing to preserve", table);
        return Ok(false);
    }

    // Build combined WHERE clause from all preserve rules for this table
    let where_clauses: Vec<String> = preserve_rules
        .iter()
        .map(|p| format!("({})", p.where_clause))
        .collect();
    let combined_where = where_clauses.join(" OR ");

    // Create backup table with same structure and copy matching rows
    let create_backup_sql = format!(
        "CREATE TABLE `{}` AS SELECT * FROM `{}` WHERE {}",
        backup_table, table, combined_where
    );
    debug!("Backup preserve: {}", create_backup_sql);
    conn.query_drop(&create_backup_sql)?;

    // Check how many rows were backed up
    let count: Option<u64> = conn.query_first(format!("SELECT COUNT(*) FROM `{}`", backup_table))?;
    let row_count = count.unwrap_or(0);

    if row_count == 0 {
        // No rows matched, drop the empty backup table
        conn.query_drop(format!("DROP TABLE `{}`", backup_table))?;
        debug!("No rows to preserve in {}", table);
        return Ok(false);
    }

    info!("Backed up {} preserved rows from {} to {}", row_count, table, backup_table);
    Ok(true)
}

/// Restore previously preserved rows from backup table after import
fn restore_preserved_rows(conn: &mut Conn, table: &str) -> Result<()> {
    let backup_table = preserve_backup_table(table);

    // Check if backup table exists
    let backup_exists: Option<String> = conn.query_first(format!(
        "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
        backup_table
    ))?;

    if backup_exists.is_none() {
        debug!("No backup table {} found", backup_table);
        return Ok(());
    }

    // Get column names from backup table
    let columns: Vec<String> = conn.query_map(
        format!("SHOW COLUMNS FROM `{}`", backup_table),
        |row: mysql::Row| {
            let field: String = row.get(0).unwrap();
            field
        },
    )?;

    if columns.is_empty() {
        conn.query_drop(format!("DROP TABLE `{}`", backup_table))?;
        return Ok(());
    }

    let col_list = columns
        .iter()
        .map(|c| format!("`{}`", c))
        .collect::<Vec<_>>()
        .join(", ");

    // Use REPLACE INTO to restore rows (handles both insert and update)
    let restore_sql = format!(
        "REPLACE INTO `{}` ({}) SELECT {} FROM `{}`",
        table, col_list, col_list, backup_table
    );
    debug!("Restore preserve: {}", restore_sql);
    conn.query_drop(&restore_sql)?;

    // Get count of restored rows
    let count: Option<u64> = conn.query_first(format!("SELECT COUNT(*) FROM `{}`", backup_table))?;
    let row_count = count.unwrap_or(0);

    // Drop backup table
    conn.query_drop(format!("DROP TABLE `{}`", backup_table))?;

    info!("Restored {} preserved rows to {}", row_count, table);
    Ok(())
}
