//! The import protocol loop: message handling, load scheduling,
//! checkpoint-aware finalization, and the client side of the handshake

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use mysql::prelude::*;
use mysql::Conn;
use tracing::{debug, info, warn};

use jibs_protocol::{
    ClientMessage, ColumnDef, CompressionMode, ExecutionPlan, ForeignKeyDef, MessageWriter,
    PreserveRule, ServerMessage, ServerMetrics,
};

use crate::checkpoint::{
    backup_preserved_rows, find_backup_tables, restore_preserved_rows, Checkpoint,
    BACKUP_TABLE_PREFIX,
};
use crate::loader::{
    load_tsv_data, maybe_decompress, warn_dropped_rows, wait_for_load, DdlResult, LoadResult,
    LoaderPool, PendingLoad,
};
use crate::metrics::ClientMetrics;
use crate::progress::ImportProgress;
use crate::sql::{create_table, execute_set_block};
use crate::ssh::{ProtocolRead, ProtocolWrite, RemoteProcess};

// ============================================================================
// Import Configuration and Main Entry Point
// ============================================================================

/// Protocol-specific configuration passed to run_protocol
pub(crate) struct ProtocolConfig {
    pub(crate) compression: CompressionMode,
    pub(crate) is_resume: bool,
    pub(crate) max_message_size: usize,
    pub(crate) fail_after_tables: Option<usize>,
    pub(crate) parallel: u32,
    pub(crate) collect_metrics: bool,
}

struct LoadAccum {
    decompress_ns: u64,
    load_ns: u64,
    /// Rows sent but not inserted (silent duplicate-key skips), summed per table.
    dropped_by_table: HashMap<String, u64>,
}

impl LoadAccum {
    fn new() -> Self {
        Self {
            decompress_ns: 0,
            load_ns: 0,
            dropped_by_table: HashMap::new(),
        }
    }

    fn add(&mut self, table: &str, result: &LoadResult) {
        self.decompress_ns += result.decompress_ns;
        self.load_ns += result.load_ns;
        self.record_dropped(table, result.dropped);
    }

    /// Accumulate silently-dropped rows for `table` (no-op when none dropped).
    /// Also used by the sequential path, which has no [`LoadResult`].
    fn record_dropped(&mut self, table: &str, dropped: u64) {
        if dropped > 0 {
            *self.dropped_by_table.entry(table.to_string()).or_default() += dropped;
        }
    }
}

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
                load_accum.add(&table, &result);
            }
            Ok(Err(e)) => return Err(anyhow::anyhow!("Loader error for {}: {}", table, e)),
            Err(crossbeam_channel::TryRecvError::Empty) => still_pending.push((table, rx)),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                return Err(anyhow::anyhow!("Loader worker died for {}", table))
            }
        }
    }

    // Check if any deferred tables can now be checkpointed
    finalize_completed_tables(
        &still_pending,
        deferred,
        local_conn,
        progress,
        stats,
        table_schemas,
        fail_after_tables,
    )?;

    Ok(still_pending)
}

/// Simulated crash for resume testing (--fail-after-tables). Must be checked
/// immediately after each table checkpoint so it fires deterministically
/// regardless of load timing.
fn check_fail_after(stats: &ImportStats, fail_after_tables: Option<usize>) -> Result<()> {
    if let Some(fail_after) = fail_after_tables {
        if stats.tables_imported >= fail_after {
            return Err(anyhow::anyhow!(
                "[DEBUG] Simulated crash after {} tables (--fail-after-tables)",
                fail_after
            ));
        }
    }
    Ok(())
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
    fail_after_tables: Option<usize>,
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
        check_fail_after(stats, fail_after_tables)?;
    }

    Ok(())
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
    fail_after_tables: Option<usize>,
) -> Result<()> {
    for (table, rx) in pending_loads {
        let result = wait_for_load(&table, &rx)?;
        stats.rows_imported += result.rows;
        load_accum.add(&table, &result);
    }

    // Finalize all remaining deferred tables (no loads left)
    for (table, info) in deferred.drain() {
        progress.finish_table(&table, info.row_count);
        stats.tables_imported += 1;
        stats.tables_imported_names.push(table.clone());
        table_schemas.remove(&table);
        Checkpoint::mark_complete(local_conn, &table, info.row_count)?;
        check_fail_after(stats, fail_after_tables)?;
    }

    Ok(())
}

/// Configuration for an import operation
/// A get function invocation from the CLI

pub(crate) struct ImportStats {
    pub(crate) tables_imported: usize,
    pub(crate) tables_imported_names: Vec<String>,
    pub(crate) rows_imported: u64,
    pub(crate) server_metrics: Option<ServerMetrics>,
    /// Per-table durations: (name, rows, duration)
    pub(crate) table_durations: Vec<(String, u64, Duration)>,
    /// Source-schema foreign keys reported on `Done`, reconstructed after the
    /// import so a fresh target ends up with the source's FKs. Empty on
    /// interruption.
    pub(crate) source_foreign_keys: Vec<ForeignKeyDef>,
}

/// Apply get function invocations for the `get` command
///
/// For each invocation, looks up the get function, resolves parameters,

pub(crate) struct ProtocolOutcome {
    pub(crate) result: Result<()>,
    pub(crate) stats: ImportStats,
    pub(crate) client_metrics: ClientMetrics,
    pub(crate) loader_pool: Option<LoaderPool>,
}

/// Run the import protocol with the remote server
pub(crate) async fn run_protocol(
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
        source_foreign_keys: Vec::new(),
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
        dry_run: false,
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
            let result = recv_message(&mut reader, max_message_size).await;
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
            ServerMessage::Schema {
                table_id,
                columns,
                indexes,
                options,
            } => {
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

                // Store schema for this table (shared with the loader pool via Arc)
                let columns = Arc::new(columns);
                table_schemas.insert(table.clone(), Arc::clone(&columns));

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
                        Arc::clone(&columns),
                        indexes,
                        options,
                        anon_rules,
                    )?;
                    pending_ddls.insert(table.clone(), ddl_rx);
                } else {
                    let ddl_start = Instant::now();
                    create_table(local_conn, table, &columns, &indexes, &options, anon_rules.as_ref())?;
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
                    let result_rx = pool.submit(
                        table.clone(),
                        schema,
                        tsv_data,
                        negotiated_compression,
                        u64::from(row_count),
                    )?;
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
                                load_accum.add(tbl, &result);
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
                    let loaded = load_tsv_data(local_conn, table, &schema, decompressed)?;
                    if config.collect_metrics {
                        client_metrics.add_load_time(load_start.elapsed());
                        client_metrics.add_rows_loaded(loaded);
                    }
                    stats.rows_imported += loaded;
                    // Rows MySQL skipped (duplicate unique/PK key) — surfaced at Done.
                    load_accum.record_dropped(table, u64::from(row_count).saturating_sub(loaded));
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

            ServerMessage::Done { table_dispositions, metrics: server_metrics, foreign_keys } => {
                // Source-schema FKs to reconstruct once loading finishes (applied
                // by the caller, after the loader pool has drained).
                stats.source_foreign_keys = foreign_keys;

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
                        config.fail_after_tables,
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

                // Loudly surface any rows MySQL silently skipped (duplicate
                // unique/PK keys) from both the pool and sequential paths. Emitted
                // only after the progress bar is finished, else it swallows the log.
                warn_dropped_rows(&load_accum.dropped_by_table);

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

            ServerMessage::DryRunReport { .. } => {
                return Err(anyhow::anyhow!(
                    "Unexpected DryRunReport message (dry_run was not requested)"
                ));
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
    send_message(&mut writer, &mut encoder, &ClientMessage::Shutdown).await?;

    Ok(())
}

/// Dry run: start the remote server with dry_run set, print what the import

/// Exchange protocol preambles with the remote server: send ours, then read
/// and validate the server's greeting. Both sides write before reading, so
/// there is no deadlock. Runs before any framed message, so a client/server
/// version mismatch is a clear, actionable error instead of bincode decode
pub(crate) async fn perform_handshake(server: &mut RemoteProcess) -> Result<()> {
    use jibs_protocol::handshake::{self, PREAMBLE_LEN};

    server
        .write(&handshake::encode_preamble())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send protocol preamble: {}", e))?;

    // A server that predates the handshake sends no greeting (it waits for a
    // framed message that never comes) — the timeout turns that into an
    // actionable error instead of a hang.
    let mut buf = [0u8; PREAMBLE_LEN];
    let read_greeting = async {
        let mut filled = 0;
        while filled < PREAMBLE_LEN {
            let n = server
                .read(&mut buf[filled..])
                .await
                .map_err(|e| anyhow::anyhow!("Failed to read server greeting: {}", e))?;
            if n == 0 {
                anyhow::bail!(
                    "Server closed the connection before sending a protocol greeting. \
                     The remote jibs-server is older than this client — \
                     run ./scripts/build.sh to rebuild client and server together, then retry."
                );
            }
            filled += n;
        }
        Ok(())
    };
    match tokio::time::timeout(Duration::from_secs(10), read_greeting).await {
        Err(_) => anyhow::bail!(
            "Timed out waiting for the server protocol greeting. \
             The remote jibs-server is probably older than this client — \
             run ./scripts/build.sh to rebuild client and server together, then retry."
        ),
        Ok(result) => result?,
    }

    handshake::validate_preamble(&buf).map_err(|e| anyhow::anyhow!("{}", e))?;
    debug!("Protocol handshake complete (v{})", handshake::PROTOCOL_VERSION);
    Ok(())
}

/// Send a message to the server (works on both the unsplit process and the
/// split write half)
pub(crate) async fn send_message<W: ProtocolWrite>(
    server: &mut W,
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

/// Receive a message from the server (works on both the unsplit process
/// and the split read half)
pub(crate) async fn recv_message<R: ProtocolRead>(
    server: &mut R,
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


