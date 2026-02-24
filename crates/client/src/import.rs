//! Import orchestration - coordinates the entire import process

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use mysql::prelude::*;
use mysql::{Conn, LocalInfileHandler, Opts};
use tracing::{debug, info, warn};

use jibs_protocol::{
    ClientMessage, ColumnDef, CompressionMode, ExecutionPlan, MessageWriter, PreserveRule,
    ServerMessage, ServerMetrics, SetRule, Value,
};

use crate::metrics::ClientMetrics;
use crate::progress::ImportProgress;
use crate::resolver;
use crate::server_binary;
use crate::ssh::{get_server_path, ProcessReader, ProcessWriter, RemoteProcess, SshConfig, SshSession};

// ============================================================================
// Loader Pool - manages parallel MySQL connections for data loading
// ============================================================================

/// Result from a DDL (CREATE TABLE) operation
struct DdlResult {
    ddl_ns: u64,
}

/// Work item for loader workers
enum LoadWork {
    /// Create (or recreate) a table — must complete before any LoadData for the same table
    CreateTable {
        table: String,
        columns: Vec<ColumnDef>,
        anon_rules: Option<Vec<jibs_protocol::AnonymizeRule>>,
        result_tx: crossbeam_channel::Sender<Result<DdlResult>>,
    },
    /// Decompress + LOAD DATA for a chunk of rows
    LoadData {
        table: String,
        columns: Arc<Vec<ColumnDef>>,
        data: Vec<u8>,
        compression: CompressionMode,
        result_tx: crossbeam_channel::Sender<Result<LoadResult>>,
    },
}

/// Worker initialization result
enum WorkerInitResult {
    Ready,
    Failed(String),
}

/// Pool of loader workers for parallel data loading
struct LoaderPool {
    work_tx: crossbeam_channel::Sender<LoadWork>,
    worker_handles: Vec<std::thread::JoinHandle<()>>,
}

impl LoaderPool {
    /// Create a new loader pool with N workers
    /// Returns an error if any worker fails to initialize
    fn new(mysql_url: &str, num_workers: usize) -> Result<Self> {
        let (work_tx, work_rx) = crossbeam_channel::unbounded::<LoadWork>();

        // Channel for workers to report initialization status
        let (init_tx, init_rx) = crossbeam_channel::unbounded::<(usize, WorkerInitResult)>();

        let mut worker_handles = Vec::with_capacity(num_workers);

        for worker_id in 0..num_workers {
            let url = mysql_url.to_string();
            let rx = work_rx.clone();
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
                // Allow inserting 0 into auto-increment columns
                if let Err(e) = conn.query_drop("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO'") {
                    let msg = format!("Failed to set SQL mode: {}", e);
                    let _ = init_reporter.send((worker_id, WorkerInitResult::Failed(msg)));
                    return;
                }

                // Report successful initialization
                let _ = init_reporter.send((worker_id, WorkerInitResult::Ready));
                debug!("Loader worker {} connected", worker_id);

                // Process work items
                loop {
                    let work = rx.recv();

                    let work = match work {
                        Ok(w) => w,
                        Err(_) => break, // Channel closed
                    };

                    match work {
                        LoadWork::CreateTable {
                            table,
                            columns,
                            anon_rules,
                            result_tx,
                        } => {
                            let result = (|| -> Result<DdlResult> {
                                let ddl_start = Instant::now();
                                create_table(
                                    &mut conn,
                                    &table,
                                    &columns,
                                    anon_rules.as_ref(),
                                )?;
                                Ok(DdlResult {
                                    ddl_ns: ddl_start.elapsed().as_nanos() as u64,
                                })
                            })();
                            let _ = result_tx.send(result);
                        }
                        LoadWork::LoadData {
                            table,
                            columns,
                            data,
                            compression,
                            result_tx,
                        } => {
                            let result = (|| -> Result<LoadResult> {
                                let decompress_start = Instant::now();
                                let decompressed = maybe_decompress(data, compression)?;
                                let decompress_ns =
                                    decompress_start.elapsed().as_nanos() as u64;

                                let load_start = Instant::now();
                                let rows = load_tsv_data_with_conn(
                                    &mut conn,
                                    &table,
                                    &columns,
                                    decompressed,
                                )?;
                                let load_ns = load_start.elapsed().as_nanos() as u64;

                                Ok(LoadResult {
                                    rows,
                                    decompress_ns,
                                    load_ns,
                                })
                            })();
                            let _ = result_tx.send(result);
                        }
                    }
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
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
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
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
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

    /// Submit a CREATE TABLE job to the pool, returns a receiver for the result.
    fn submit_ddl(
        &self,
        table: String,
        columns: Vec<ColumnDef>,
        anon_rules: Option<Vec<jibs_protocol::AnonymizeRule>>,
    ) -> Result<crossbeam_channel::Receiver<Result<DdlResult>>> {
        let (result_tx, result_rx) = crossbeam_channel::unbounded();

        self.work_tx
            .send(LoadWork::CreateTable {
                table,
                columns,
                anon_rules,
                result_tx,
            })
            .map_err(|_| anyhow::anyhow!("Loader pool shut down"))?;

        Ok(result_rx)
    }

    /// Submit data for loading, returns a receiver for the result.
    /// Data may be compressed — workers decompress before loading.
    fn submit(
        &self,
        table: String,
        columns: Arc<Vec<ColumnDef>>,
        data: Vec<u8>,
        compression: CompressionMode,
    ) -> Result<crossbeam_channel::Receiver<Result<LoadResult>>> {
        let (result_tx, result_rx) = crossbeam_channel::unbounded();

        self.work_tx
            .send(LoadWork::LoadData {
                table,
                columns,
                data,
                compression,
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
    tsv_data: Vec<u8>,
) -> Result<u64> {
    use std::io::Write;

    if tsv_data.is_empty() {
        return Ok(0);
    }

    // Set up the local infile handler
    let data = tsv_data;

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

/// Protocol-specific configuration passed to run_protocol
struct ProtocolConfig {
    compression: CompressionMode,
    is_resume: bool,
    max_message_size: usize,
    fail_after_tables: Option<usize>,
    parallel: u32,
    collect_metrics: bool,
}

// ============================================================================
// Pending Load Helpers - manage parallel loader pool results
// ============================================================================

/// Result from a parallel load worker including timing info
struct LoadResult {
    rows: u64,
    decompress_ns: u64,
    load_ns: u64,
}

/// Accumulator for parallel worker timing (separate from row counts)
struct LoadAccum {
    decompress_ns: u64,
    load_ns: u64,
}

impl LoadAccum {
    fn new() -> Self {
        Self {
            decompress_ns: 0,
            load_ns: 0,
        }
    }

    fn add(&mut self, result: &LoadResult) {
        self.decompress_ns += result.decompress_ns;
        self.load_ns += result.load_ns;
    }
}

type PendingLoad = (String, crossbeam_channel::Receiver<Result<LoadResult>>);

/// Info needed to finalize a table after all its loads complete
struct DeferredTableDone {
    row_count: u64,
}

/// Drain completed loads without blocking, returns remaining pending loads.
/// Also finalizes any deferred tables whose loads have all completed.
fn drain_completed_loads(
    pending_loads: Vec<PendingLoad>,
    load_accum: &mut LoadAccum,
    deferred: &mut HashMap<String, DeferredTableDone>,
    local_conn: &mut Conn,
    progress: &ImportProgress,
    stats: &mut ImportStats,
    table_schemas: &mut HashMap<String, Arc<Vec<ColumnDef>>>,
    fail_after_tables: Option<usize>,
) -> Result<Vec<PendingLoad>> {
    let mut still_pending = Vec::new();
    for (table, rx) in pending_loads {
        match rx.try_recv() {
            Ok(Ok(result)) => {
                stats.rows_imported += result.rows;
                load_accum.add(&result);
            }
            Ok(Err(e)) => return Err(anyhow::anyhow!("Loader error for {}: {}", table, e)),
            Err(crossbeam_channel::TryRecvError::Empty) => still_pending.push((table, rx)),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                return Err(anyhow::anyhow!("Loader worker died for {}", table))
            }
        }
    }

    // Check if any deferred tables can now be checkpointed
    finalize_completed_tables(&still_pending, deferred, local_conn, progress, stats, table_schemas)?;

    if let Some(fail_after) = fail_after_tables {
        if stats.tables_imported >= fail_after {
            return Err(anyhow::anyhow!(
                "[DEBUG] Simulated crash after {} tables (--fail-after-tables)",
                fail_after
            ));
        }
    }

    Ok(still_pending)
}

/// Finalize tables whose loads have all completed (non-blocking).
/// A table is ready when it's in `deferred` and has no entries in `pending_loads`.
fn finalize_completed_tables(
    pending_loads: &[PendingLoad],
    deferred: &mut HashMap<String, DeferredTableDone>,
    local_conn: &mut Conn,
    progress: &ImportProgress,
    stats: &mut ImportStats,
    table_schemas: &mut HashMap<String, Arc<Vec<ColumnDef>>>,
) -> Result<()> {
    if deferred.is_empty() {
        return Ok(());
    }

    // Build set of tables that still have pending loads
    let mut pending_tables: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for (table, _) in pending_loads {
        pending_tables.insert(table.as_str());
    }

    // Finalize any deferred tables with no remaining loads
    let ready: Vec<String> = deferred
        .keys()
        .filter(|t| !pending_tables.contains(t.as_str()))
        .cloned()
        .collect();

    for table in ready {
        let info = deferred.remove(&table).unwrap();
        progress.finish_table(&table, info.row_count);
        stats.tables_imported += 1;
        stats.tables_imported_names.push(table.clone());
        table_schemas.remove(&table);
        Checkpoint::mark_complete(local_conn, &table, info.row_count)?;
    }

    Ok(())
}

/// Wait for a specific load to complete (blocking)
fn wait_for_load(
    table: &str,
    rx: &crossbeam_channel::Receiver<Result<LoadResult>>,
) -> Result<LoadResult> {
    match rx.recv() {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(e)) => Err(anyhow::anyhow!("Loader error for {}: {}", table, e)),
        Err(_) => Err(anyhow::anyhow!("Loader worker died for {}", table)),
    }
}

/// Wait for all remaining pending loads to complete (blocking),
/// then finalize all deferred tables.
fn wait_for_all_loads(
    pending_loads: Vec<PendingLoad>,
    load_accum: &mut LoadAccum,
    deferred: &mut HashMap<String, DeferredTableDone>,
    local_conn: &mut Conn,
    progress: &ImportProgress,
    stats: &mut ImportStats,
    table_schemas: &mut HashMap<String, Arc<Vec<ColumnDef>>>,
) -> Result<()> {
    for (table, rx) in pending_loads {
        let result = wait_for_load(&table, &rx)?;
        stats.rows_imported += result.rows;
        load_accum.add(&result);
    }

    // Finalize all remaining deferred tables (no loads left)
    for (table, info) in deferred.drain() {
        progress.finish_table(&table, info.row_count);
        stats.tables_imported += 1;
        stats.tables_imported_names.push(table.clone());
        table_schemas.remove(&table);
        Checkpoint::mark_complete(local_conn, &table, info.row_count)?;
    }

    Ok(())
}

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
    /// Number of client-side loader pool workers (None = use `parallel` value)
    pub client_parallel: Option<usize>,
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
    /// Whether to collect and display timing metrics
    pub collect_metrics: bool,
    /// Whether to show a report of slowest tables after import
    pub show_report: bool,
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
    let has_checkpoint = Checkpoint::exists(&mut local_conn)?;
    let has_previous_state = !existing_backups.is_empty() || has_checkpoint;

    if has_previous_state {
        if config.clean {
            // Clean up and start fresh
            info!("Cleaning up state from previous import");
            for backup_table in &existing_backups {
                local_conn.query_drop(format!("DROP TABLE `{}`", backup_table))?;
                info!("  Dropped {}", backup_table);
            }
            Checkpoint::cleanup(&mut local_conn)?;
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
                let completed = Checkpoint::get_completed(&mut local_conn)?;
                state_parts.push(format!("checkpoint ({} tables completed)", completed.len()));
            }
            return Err(anyhow::anyhow!(
                "Found state from a previous interrupted import:\n  {}\n\n\
                 Use --resume to continue the interrupted import, or\n\
                 Use --clean to discard the state and start fresh.",
                state_parts.join("\n  ")
            ));
        } else {
            let completed = Checkpoint::get_completed(&mut local_conn)?;
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
    // Allow inserting 0 into auto-increment columns (e.g. store_website.website_id = 0)
    local_conn.query_drop("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO'")?;

    // Drop all FK constraints in the local database to prevent MySQL ERROR 1822.
    // When tables are recreated in parallel (with only a PK, no secondary indexes),
    // MySQL re-validates orphaned FK constraints from existing tables and fails if
    // the required index is missing on the newly created referenced table.
    drop_all_foreign_keys(&mut local_conn)?;

    // Create loader pool for parallel loading
    let client_workers = config.client_parallel.unwrap_or(config.parallel);
    let loader_pool = if client_workers > 1 {
        info!("Creating loader pool with {} workers", client_workers);
        Some(LoaderPool::new(&config.local_mysql, client_workers)?)
    } else {
        None
    };

    // Start the remote server (credentials sent via protocol, not in process listing)
    info!("Starting remote server: {}", server_path);
    let mut server = session.start_process(&server_path).await?;

    // Send credentials via protocol (not visible in process listing)
    let mut encoder: MessageWriter<()> = MessageWriter::with_capacity(4096, ());
    let creds_msg = ClientMessage::Credentials {
        mysql_url: config.remote_mysql.clone(),
    };
    send_message(&mut server, &mut encoder, &creds_msg).await?;

    // Run the import protocol (takes ownership of server for split)
    let protocol_config = ProtocolConfig {
        compression: config.compression,
        is_resume: config.resume,
        max_message_size: config.max_message_size,
        #[cfg(feature = "test-utils")]
        fail_after_tables: config.fail_after_tables,
        #[cfg(not(feature = "test-utils"))]
        fail_after_tables: None,
        parallel: config.parallel as u32,
        collect_metrics: config.collect_metrics,
    };
    let outcome = run_protocol(server, &mut local_conn, loader_pool, plan, protocol_config, encoder).await;

    // Shutdown loader pool if used
    if let Some(pool) = outcome.loader_pool {
        debug!("Shutting down loader pool");
        pool.shutdown();
    }

    // Display metrics if enabled (on both success and interruption)
    if config.collect_metrics {
        outcome.client_metrics.display(outcome.stats.server_metrics.as_ref());
    }

    // Display report if enabled and we have table data
    if config.show_report && !outcome.stats.table_durations.is_empty() {
        display_report(&outcome.stats.table_durations);
    }

    // Re-enable checks
    let _ = local_conn.query_drop("SET FOREIGN_KEY_CHECKS = 1");
    let _ = local_conn.query_drop("SET UNIQUE_CHECKS = 1");

    // Handle result
    match outcome.result {
        Ok(()) => {
            info!(
                "Import complete: {} tables, {} rows",
                outcome.stats.tables_imported, outcome.stats.rows_imported
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Import statistics
struct ImportStats {
    tables_imported: usize,
    tables_imported_names: Vec<String>,
    rows_imported: u64,
    server_metrics: Option<ServerMetrics>,
    /// Per-table durations: (name, rows, duration)
    table_durations: Vec<(String, u64, Duration)>,
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

// ============================================================================
// Checkpoint - tracks import progress for resume functionality
// ============================================================================

/// Checkpoint manager for tracking import progress
struct Checkpoint;

impl Checkpoint {
    /// Check if checkpoint table exists
    fn exists(conn: &mut Conn) -> Result<bool> {
        let exists: Option<String> = conn.query_first(format!(
            "SELECT TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{}'",
            CHECKPOINT_TABLE
        ))?;
        Ok(exists.is_some())
    }

    /// Create the checkpoint table
    fn create(conn: &mut Conn) -> Result<()> {
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
    fn get_completed(conn: &mut Conn) -> Result<std::collections::HashSet<String>> {
        if !Self::exists(conn)? {
            return Ok(std::collections::HashSet::new());
        }
        let tables: Vec<String> = conn.query_map(
            format!("SELECT table_name FROM `{}`", CHECKPOINT_TABLE),
            |name: String| name,
        )?;
        Ok(tables.into_iter().collect())
    }

    /// Mark a table as complete in the checkpoint
    fn mark_complete(conn: &mut Conn, table: &str, row_count: u64) -> Result<()> {
        conn.query_drop(format!(
            "INSERT INTO `{}` (table_name, row_count) VALUES ('{}', {})",
            CHECKPOINT_TABLE, table, row_count
        ))?;
        Ok(())
    }

    /// Clean up the checkpoint table
    fn cleanup(conn: &mut Conn) -> Result<()> {
        conn.query_drop(format!("DROP TABLE IF EXISTS `{}`", CHECKPOINT_TABLE))?;
        Ok(())
    }
}

/// Outcome of the protocol run - always carries metrics even on error/interruption
struct ProtocolOutcome {
    result: Result<()>,
    stats: ImportStats,
    client_metrics: ClientMetrics,
    loader_pool: Option<LoaderPool>,
}

/// Run the import protocol with the remote server
async fn run_protocol(
    server: RemoteProcess,
    local_conn: &mut Conn,
    loader_pool: Option<LoaderPool>,
    plan: ExecutionPlan,
    config: ProtocolConfig,
    encoder: MessageWriter<()>,
) -> ProtocolOutcome {
    let mut stats = ImportStats {
        tables_imported: 0,
        tables_imported_names: Vec::new(),
        rows_imported: 0,
        server_metrics: None,
        table_durations: Vec::new(),
    };

    let mut client_metrics = ClientMetrics::new();
    if config.collect_metrics {
        client_metrics.start();
    }

    let result = run_protocol_inner(server, local_conn, &loader_pool, plan, &config, &mut stats, &mut client_metrics, encoder).await;

    // Always stop metrics timing
    if config.collect_metrics {
        client_metrics.stop();
    }

    ProtocolOutcome {
        result,
        stats,
        client_metrics,
        loader_pool,
    }
}

/// Inner protocol implementation
async fn run_protocol_inner(
    mut server: RemoteProcess,
    local_conn: &mut Conn,
    loader_pool: &Option<LoaderPool>,
    plan: ExecutionPlan,
    config: &ProtocolConfig,
    stats: &mut ImportStats,
    client_metrics: &mut ClientMetrics,
    mut encoder: MessageWriter<()>,
) -> Result<()> {

    // Set up checkpointing
    Checkpoint::create(local_conn)?;
    let completed_tables = if config.is_resume {
        Checkpoint::get_completed(local_conn)?
    } else {
        std::collections::HashSet::new()
    };

    // Send Init message
    debug!("Sending execution plan to server");
    let init_msg = ClientMessage::Init {
        plan: plan.clone(),
        compression: config.compression,
        parallel: config.parallel,
        collect_metrics: config.collect_metrics,
    };
    send_message(&mut server, &mut encoder, &init_msg).await?;

    // Wait for Ready
    let ready_msg: ServerMessage = recv_message(&mut server, config.max_message_size).await?;
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

    // Build table_id → name reverse lookup
    let id_to_name: std::collections::HashMap<u16, String> = tables
        .iter()
        .map(|t| (t.table_id, t.name.clone()))
        .collect();

    // Create table info map for estimated rows lookup
    let table_info: std::collections::HashMap<String, u64> = tables
        .iter()
        .map(|t| (t.name.clone(), t.estimated_rows))
        .collect();

    // Initialize progress tracking
    let skipped_count = completed_tables.len();
    let progress = ImportProgress::new(&tables, skipped_count);

    // Send Start message
    debug!("Starting data transfer");
    send_message(&mut server, &mut encoder, &ClientMessage::Start).await?;

    // Track tables with preserved backups
    let mut tables_with_preserves: Vec<String> = Vec::new();

    // Track skipped tables (already completed in previous run)
    let mut skipped_tables: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Track schemas per table (for interleaved streaming)
    let mut table_schemas: HashMap<String, Arc<Vec<ColumnDef>>> = HashMap::new();

    // Track pending load results globally (not per-table) to avoid blocking at table boundaries
    // Each entry is (table_name, result_receiver)
    let mut pending_loads: Vec<PendingLoad> = Vec::new();

    // Track pending DDL (CREATE TABLE) operations dispatched to the loader pool.
    // When a Data message arrives, we wait for DDL completion before submitting load.
    let mut pending_ddls: HashMap<String, crossbeam_channel::Receiver<Result<DdlResult>>> =
        HashMap::new();

    // Tables where TableDone has been received but not all loads have completed yet.
    // These get checkpointed once their loads finish (checked during non-blocking drains).
    let mut deferred_table_dones: HashMap<String, DeferredTableDone> = HashMap::new();

    // Accumulator for parallel worker timing
    let mut load_accum = LoadAccum::new();

    // Maximum number of pending chunks before we start draining
    // This bounds memory usage while allowing cross-table parallelism
    const MAX_PENDING_CHUNKS: usize = 100;

    // Split the process into reader/writer halves and spawn a read-ahead task.
    // This keeps the SSH pipe drained even while the main loop is busy processing,
    // preventing backpressure from stalling the server.
    let (mut reader, mut writer) = server.split();

    // Channel depth tracking for metrics
    let channel_depth = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let channel_depth_sender = std::sync::Arc::clone(&channel_depth);

    // Spawn read-ahead task: continuously reads messages from SSH and buffers them.
    // The bounded channel (32 messages) absorbs bursts so the server doesn't stall.
    let max_message_size = config.max_message_size;
    let (msg_tx, mut msg_rx) = tokio::sync::mpsc::channel::<Result<ServerMessage>>(32);
    tokio::spawn(async move {
        loop {
            let result = recv_message_from_reader(&mut reader, max_message_size).await;
            let is_done = result.as_ref().map(|m| matches!(m, ServerMessage::Done { .. })).unwrap_or(false);
            let is_err = result.is_err();
            if msg_tx.send(result).await.is_err() {
                break; // Receiver dropped
            }
            channel_depth_sender.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if is_done || is_err {
                break;
            }
        }
    });

    // Pin ctrl_c future for use in select!
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    loop {
        // Receive from the read-ahead buffer (not directly from SSH)
        let recv_start = Instant::now();
        let msg = tokio::select! {
            msg = msg_rx.recv() => match msg {
                Some(Ok(msg)) => msg,
                Some(Err(e)) => return Err(e),
                None => return Err(anyhow::anyhow!("Server connection closed unexpectedly")),
            },
            _ = &mut ctrl_c => {

                // Transfer parallel worker timing collected so far
                if config.collect_metrics {
                    client_metrics.add_parallel_decompress_time(Duration::from_nanos(
                        load_accum.decompress_ns,
                    ));
                    client_metrics.add_parallel_load_time(Duration::from_nanos(load_accum.load_ns));
                    client_metrics.add_rows_loaded(stats.rows_imported);
                }
                // Capture partial table durations
                stats.table_durations = progress.table_durations();
                progress.finish();
                return Err(anyhow::anyhow!("Interrupted"));
            }
        };
        if config.collect_metrics {
            client_metrics.add_recv_time(recv_start.elapsed());
            // Track read-ahead channel depth (value before we consumed this message)
            let depth = channel_depth.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            client_metrics.record_channel_depth(depth);
        }

        match msg {
            ServerMessage::Schema { table_id, columns } => {
                let table = id_to_name.get(&table_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown table_id {} in Schema", table_id))?;

                // Check if this table was already completed in a previous run
                if completed_tables.contains(table) {
                    progress.skip_table(table);
                    skipped_tables.insert(table.clone());
                    continue;
                }

                // Get estimated rows for progress tracking
                let estimated_rows = table_info.get(table).copied().unwrap_or(0);
                progress.start_table(table, estimated_rows);

                // Store schema for this table
                table_schemas.insert(table.clone(), Arc::new(columns.clone()));

                // Backup preserved rows BEFORE dropping the table
                let table_preserves: Vec<&PreserveRule> = plan
                    .preserves
                    .iter()
                    .filter(|p| p.table == *table)
                    .collect();

                if !table_preserves.is_empty() {
                    if backup_preserved_rows(local_conn, table, &table_preserves)? {
                        tables_with_preserves.push(table.clone());
                    }
                }

                // Get anonymization rules for this table
                let anon_rules = plan.anonymization.get(table).cloned();

                // Dispatch CREATE TABLE to loader pool (parallel) or run synchronously
                if let Some(pool) = loader_pool {
                    let ddl_rx = pool.submit_ddl(
                        table.clone(),
                        columns,
                        anon_rules,
                    )?;
                    pending_ddls.insert(table.clone(), ddl_rx);
                } else {
                    let ddl_start = Instant::now();
                    create_table(local_conn, table, &columns, anon_rules.as_ref())?;
                    if config.collect_metrics {
                        client_metrics.add_ddl_time(ddl_start.elapsed());
                    }
                }
            }

            ServerMessage::Data {
                table_id,
                row_count,
                tsv_data,
            } => {
                let table = id_to_name.get(&table_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown table_id {} in Data", table_id))?;

                // Skip data for already-completed tables
                if skipped_tables.contains(table) {
                    debug!("Skipping data chunk for {} (already completed)", table);
                    continue;
                }

                if config.collect_metrics {
                    client_metrics.add_compressed_bytes(tsv_data.len() as u64);
                    client_metrics.add_message();
                    // Read uncompressed size from zstd header (first 4 bytes)
                    if matches!(negotiated_compression, CompressionMode::Zstd) && tsv_data.len() >= 4 {
                        let uncompressed_len =
                            u32::from_le_bytes([tsv_data[0], tsv_data[1], tsv_data[2], tsv_data[3]]) as u64;
                        client_metrics.add_uncompressed_bytes(uncompressed_len);
                    } else {
                        client_metrics.add_uncompressed_bytes(tsv_data.len() as u64);
                    }
                }

                // Get schema for this table
                let schema = table_schemas
                    .get(table)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("No schema for table {}", table))?;

                let chunk_bytes = tsv_data.len();

                // Load data into MySQL - use pool if available
                if let Some(pool) = loader_pool {
                    // Ensure CREATE TABLE has completed before loading data
                    if let Some(ddl_rx) = pending_ddls.remove(table) {
                        let ddl_result = ddl_rx
                            .recv()
                            .map_err(|_| anyhow::anyhow!("DDL worker died for {}", table))?
                            .map_err(|e| anyhow::anyhow!("DDL error for {}: {}", table, e))?;
                        if config.collect_metrics {
                            client_metrics
                                .add_ddl_time(Duration::from_nanos(ddl_result.ddl_ns));
                        }
                    }

                    // Submit compressed data to loader pool — workers decompress + load
                    let result_rx = pool.submit(table.clone(), schema, tsv_data, negotiated_compression)?;
                    pending_loads.push((table.clone(), result_rx));

                    // If we have too many pending chunks, drain some to bound memory
                    if pending_loads.len() > MAX_PENDING_CHUNKS {
                        let wait_start = Instant::now();
                        pending_loads = drain_completed_loads(
                            pending_loads,
                            &mut load_accum,
                            &mut deferred_table_dones,
                            local_conn,
                            &progress,
                            stats,
                            &mut table_schemas,
                            config.fail_after_tables,
                        )?;

                        // If still too many, block on the oldest one
                        if pending_loads.len() > MAX_PENDING_CHUNKS {
                            if let Some((tbl, rx)) = pending_loads.first() {
                                let result = wait_for_load(tbl, rx)?;
                                stats.rows_imported += result.rows;
                                load_accum.add(&result);
                            }
                            pending_loads.remove(0);
                        }
                        if config.collect_metrics {
                            client_metrics.add_wait_loads_time(wait_start.elapsed());
                        }
                    }
                } else {
                    // Sequential mode: decompress + load on main thread
                    let decompress_start = Instant::now();
                    let decompressed = maybe_decompress(tsv_data, negotiated_compression)?;
                    if config.collect_metrics {
                        client_metrics.add_decompress_time(decompress_start.elapsed());
                    }

                    let load_start = Instant::now();
                    let loaded = load_tsv_data(local_conn, table, &schema, &decompressed)?;
                    if config.collect_metrics {
                        client_metrics.add_load_time(load_start.elapsed());
                        client_metrics.add_rows_loaded(loaded);
                    }
                    stats.rows_imported += loaded;
                }

                // Update progress (use compressed size for byte tracking)
                progress.update_table(table, row_count.into(), chunk_bytes);
            }

            ServerMessage::TableDone { table_id, row_count, metrics: table_done_metrics } => {
                let table = id_to_name.get(&table_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown table_id {} in TableDone", table_id))?;

                // Store latest server metrics snapshot for use on interruption
                if table_done_metrics.is_some() {
                    stats.server_metrics = table_done_metrics;
                }

                // Skip marking complete for already-completed tables
                if skipped_tables.contains(table) {
                    debug!("Table {} was already complete", table);
                    continue;
                }

                // Ensure DDL completed for this table (handles 0-row tables
                // where no Data message triggered the DDL wait)
                if let Some(ddl_rx) = pending_ddls.remove(table) {
                    let ddl_result = ddl_rx
                        .recv()
                        .map_err(|_| anyhow::anyhow!("DDL worker died for {}", table))?
                        .map_err(|e| anyhow::anyhow!("DDL error for {}: {}", table, e))?;
                    if config.collect_metrics {
                        client_metrics.add_ddl_time(Duration::from_nanos(ddl_result.ddl_ns));
                    }
                }

                // Defer checkpoint: instead of blocking the main loop waiting for
                // this table's loads, record it as "done" and checkpoint it later
                // when its loads complete (checked during non-blocking drains).
                // This keeps the main loop free to receive and submit more work.
                deferred_table_dones.insert(table.clone(), DeferredTableDone { row_count });

                // Non-blocking drain to finalize any tables that are already done
                let wait_start = Instant::now();
                pending_loads = drain_completed_loads(
                    pending_loads,
                    &mut load_accum,
                    &mut deferred_table_dones,
                    local_conn,
                    &progress,
                    stats,
                    &mut table_schemas,
                    config.fail_after_tables,
                )?;
                if config.collect_metrics {
                    client_metrics.add_wait_loads_time(wait_start.elapsed());
                }
            }

            ServerMessage::Done { table_dispositions, metrics: server_metrics } => {
                // Drain any remaining pending DDLs
                for (tbl, ddl_rx) in pending_ddls.drain() {
                    let ddl_result = ddl_rx
                        .recv()
                        .map_err(|_| anyhow::anyhow!("DDL worker died for {}", tbl))?
                        .map_err(|e| anyhow::anyhow!("DDL error for {}: {}", tbl, e))?;
                    if config.collect_metrics {
                        client_metrics
                            .add_ddl_time(Duration::from_nanos(ddl_result.ddl_ns));
                    }
                }

                // Wait for all remaining pending loads and finalize deferred tables
                {
                    debug!(
                        "Waiting for {} remaining pending loads before Done",
                        pending_loads.len()
                    );
                    let wait_start = Instant::now();
                    wait_for_all_loads(
                        pending_loads,
                        &mut load_accum,
                        &mut deferred_table_dones,
                        local_conn,
                        &progress,
                        stats,
                        &mut table_schemas,
                    )?;
                    if config.collect_metrics {
                        client_metrics.add_wait_loads_time(wait_start.elapsed());
                    }
                }

                // Transfer parallel worker timing to client metrics
                if config.collect_metrics {
                    client_metrics.add_parallel_decompress_time(Duration::from_nanos(
                        load_accum.decompress_ns,
                    ));
                    client_metrics
                        .add_parallel_load_time(Duration::from_nanos(load_accum.load_ns));
                    client_metrics.add_rows_loaded(stats.rows_imported);
                }

                // Store server metrics
                stats.server_metrics = server_metrics;

                // Capture per-table durations before finishing progress
                stats.table_durations = progress.table_durations();

                progress.finish();

                // Log table report: show all server tables with their import disposition
                {
                    use jibs_protocol::TableDisposition;
                    let lines: Vec<String> = table_dispositions.iter().map(|(tid, disp)| {
                        let name = id_to_name.get(tid).map(|s| s.as_str()).unwrap_or("?");
                        let label = match disp {
                            TableDisposition::Aggregate => "aggregate",
                            TableDisposition::Full => "full",
                            TableDisposition::Empty => "full, 0 rows on remote",
                            TableDisposition::Excluded => "excluded",
                        };
                        format!("  {} ({})", name, label)
                    }).collect();

                    info!("Tables ({}):\n{}", table_dispositions.len(), lines.join("\n"));
                }

                break;
            }

            ServerMessage::Error {
                message,
                recoverable,
            } => {
                if recoverable {
                    warn!("Recoverable server error: {}", message);
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
    Checkpoint::cleanup(local_conn)?;

    // Send shutdown
    send_message_writer(&mut writer, &mut encoder, &ClientMessage::Shutdown).await?;

    Ok(())
}

/// Send a message to the server
async fn send_message(
    server: &mut RemoteProcess,
    encoder: &mut MessageWriter<()>,
    msg: &ClientMessage,
) -> Result<()> {
    let bytes = encoder
        .encode_message(msg)
        .map_err(|e| anyhow::anyhow!("Failed to serialize message: {}", e))?;
    server
        .write(bytes)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send message: {}", e))?;
    Ok(())
}

/// Validate message length and return error if too large
fn validate_message_length(len: usize, max_size: usize) -> Result<()> {
    if len > max_size {
        return Err(anyhow::anyhow!(
            "Message too large: {} bytes (max: {} bytes, ~{}MB). \
             Consider using --max-message-size to increase the limit.",
            len,
            max_size,
            max_size / (1024 * 1024)
        ));
    }
    Ok(())
}

/// Decode a server message from a buffer
fn decode_server_message(buffer: &[u8]) -> Result<ServerMessage> {
    let (msg, _) = bincode::decode_from_slice(buffer, jibs_protocol::framing::bincode_config())
        .map_err(|e| anyhow::anyhow!("Failed to decode message: {}", e))?;
    Ok(msg)
}

/// Receive a message from the server (uses unsplit RemoteProcess for pre-protocol exchange)
async fn recv_message(
    server: &mut RemoteProcess,
    max_message_size: usize,
) -> Result<ServerMessage> {
    let mut len_bytes = [0u8; 4];
    server
        .read_exact(&mut len_bytes)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read message length: {}", e))?;
    let raw_len = u32::from_le_bytes(len_bytes);
    let is_raw_chunk = raw_len & jibs_protocol::RAW_CHUNK_FLAG != 0;
    let len = (raw_len & !jibs_protocol::RAW_CHUNK_FLAG) as usize;

    validate_message_length(len, max_message_size)?;

    let mut buffer = vec![0u8; len];
    server
        .read_exact(&mut buffer)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read message body: {}", e))?;

    if is_raw_chunk {
        let chunk = jibs_protocol::decode_data_chunk(buffer)
            .map_err(|e| anyhow::anyhow!("Failed to decode data chunk: {}", e))?;
        Ok(ServerMessage::Data {
            table_id: chunk.table_id,
            row_count: chunk.row_count,
            tsv_data: chunk.tsv_data,
        })
    } else {
        decode_server_message(&buffer)
    }
}

/// Receive a message from a ProcessReader (split read half)
async fn recv_message_from_reader(
    reader: &mut ProcessReader,
    max_message_size: usize,
) -> Result<ServerMessage> {
    let mut len_bytes = [0u8; 4];
    reader
        .read_exact(&mut len_bytes)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read message length: {}", e))?;
    let raw_len = u32::from_le_bytes(len_bytes);
    let is_raw_chunk = raw_len & jibs_protocol::RAW_CHUNK_FLAG != 0;
    let len = (raw_len & !jibs_protocol::RAW_CHUNK_FLAG) as usize;

    validate_message_length(len, max_message_size)?;

    let mut buffer = vec![0u8; len];
    reader
        .read_exact(&mut buffer)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read message body: {}", e))?;

    if is_raw_chunk {
        let chunk = jibs_protocol::decode_data_chunk(buffer)
            .map_err(|e| anyhow::anyhow!("Failed to decode data chunk: {}", e))?;
        Ok(ServerMessage::Data {
            table_id: chunk.table_id,
            row_count: chunk.row_count,
            tsv_data: chunk.tsv_data,
        })
    } else {
        decode_server_message(&buffer)
    }
}

/// Send a message using a ProcessWriter (split write half)
async fn send_message_writer(
    writer: &mut ProcessWriter,
    encoder: &mut MessageWriter<()>,
    msg: &ClientMessage,
) -> Result<()> {
    let bytes = encoder
        .encode_message(msg)
        .map_err(|e| anyhow::anyhow!("Failed to serialize message: {}", e))?;
    writer
        .write(bytes)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send message: {}", e))?;
    Ok(())
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

/// Drop all foreign key constraints in the local database.
///
/// This prevents MySQL ERROR 1822 when tables are recreated in parallel.
/// When we DROP + CREATE a table that was previously referenced by FK constraints
/// from other tables, MySQL re-validates those orphaned FK constraints against the
/// new table. Since we only create a PRIMARY KEY (no secondary indexes), the
/// required index for the FK is missing, causing the error.
fn drop_all_foreign_keys(conn: &mut Conn) -> Result<()> {
    let rows: Vec<(String, String)> = conn.query(
        "SELECT TABLE_NAME, CONSTRAINT_NAME \
         FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS \
         WHERE CONSTRAINT_TYPE = 'FOREIGN KEY' AND TABLE_SCHEMA = DATABASE()",
    )?;

    if rows.is_empty() {
        return Ok(());
    }

    for (table, constraint) in &rows {
        conn.query_drop(format!(
            "ALTER TABLE `{}` DROP FOREIGN KEY `{}`",
            table, constraint
        ))?;
    }

    info!(
        "Dropped {} foreign key constraints from local database",
        rows.len()
    );
    Ok(())
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

/// Display a report of tables sorted by import duration (slowest first)
fn display_report(table_durations: &[(String, u64, Duration)]) {
    if table_durations.is_empty() {
        return;
    }

    let mut sorted: Vec<_> = table_durations.to_vec();
    sorted.sort_by(|a, b| b.2.cmp(&a.2));

    // Find the longest table name for column width
    let max_name_len = sorted.iter().map(|(n, _, _)| n.len()).max().unwrap_or(20);
    let name_width = max_name_len.max(5); // minimum "Table" header width

    eprintln!();
    eprintln!("=== Import Report ===");
    eprintln!();
    eprintln!(
        "  {:<4} {:<width$}  {:>10}  {:>10}  {:>10}",
        "#",
        "Table",
        "Rows",
        "Duration",
        "Rows/s",
        width = name_width
    );
    eprintln!(
        "  {:-<4} {:-<width$}  {:-<10}  {:-<10}  {:-<10}",
        "",
        "",
        "",
        "",
        "",
        width = name_width
    );

    for (i, (name, rows, duration)) in sorted.iter().enumerate() {
        let secs = duration.as_secs_f64();
        let rows_per_sec = if secs > 0.0 {
            (*rows as f64 / secs) as u64
        } else {
            0
        };

        let duration_str = if secs >= 60.0 {
            format!("{:.0}m {:.1}s", (secs / 60.0).floor(), secs % 60.0)
        } else {
            format!("{:.1}s", secs)
        };

        eprintln!(
            "  {:<4} {:<width$}  {:>10}  {:>10}  {:>10}",
            i + 1,
            name,
            format_number(*rows),
            duration_str,
            format_number(rows_per_sec),
            width = name_width
        );
    }

    eprintln!();
}

/// Format a number with thousand separators
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
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
