//! `jibs load <file.jibsdump>`: load a dump produced by `import --dump-to`
//! into a local MySQL database, using the parallel loader pool.
//!
//! The loading strategy mirrors the import protocol loop ([`crate::protocol`]):
//! a `CREATE TABLE` is dispatched to the pool per table, and a table's data is
//! only submitted once its DDL has completed. Loads run concurrently across the
//! pool's workers, bounded by `MAX_PENDING_CHUNKS` to cap memory. Plan-level
//! behaviour that shaped a live import — `preserve` backups, `set` upserts and
//! `after` statements — is replayed here so a load reproduces the same result.

use std::collections::HashMap;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use crossbeam_channel::Receiver;
use mysql::prelude::*;
use mysql::{Conn, Opts};
use tracing::info;

use jibs_protocol::{ColumnDef, PreserveRule, SetRule};

use crate::checkpoint::{
    backup_preserved_rows, find_backup_tables, restore_preserved_rows, Checkpoint,
    BACKUP_TABLE_PREFIX,
};
use crate::dump::{DumpReader, DumpRecord};
use crate::foreign_keys::{preserve_and_drop_foreign_keys, restore_foreign_keys};
use crate::import::redact_mysql_url;
use crate::loader::{wait_for_load, DdlResult, LoaderPool, PendingLoad};
use crate::sql::execute_set_block;

/// Maximum number of in-flight load chunks before we drain completed ones.
/// Bounds memory while keeping the pool busy across tables.
const MAX_PENDING_CHUNKS: usize = 100;

pub(crate) struct LoadConfig {
    pub(crate) dump_path: PathBuf,
    pub(crate) local_mysql: String,
    pub(crate) parallel: usize,
    /// Number of loader workers (defaults to `parallel` when unset).
    pub(crate) client_parallel: Option<usize>,
    /// Maximum size of a single dump record, in bytes.
    pub(crate) max_message_size: usize,
    /// Discard leftover state from a previous interrupted import before loading.
    pub(crate) clean: bool,
}

/// Load a `.jibsdump` file into the local database.
pub(crate) fn run_load(config: LoadConfig) -> Result<()> {
    info!("Loading dump {}", config.dump_path.display());

    let file = std::fs::File::open(&config.dump_path)
        .with_context(|| format!("failed to open dump {}", config.dump_path.display()))?;
    let mut reader =
        DumpReader::with_max_record_size(BufReader::new(file), config.max_message_size)?;

    info!(
        "Connecting to local MySQL: {}",
        redact_mysql_url(&config.local_mysql)
    );
    let opts = Opts::from_url(&config.local_mysql)
        .map_err(|e| anyhow::anyhow!("Invalid local MySQL URL: {}", e))?;
    let mut conn = Conn::new(opts)?;

    conn.query_drop("SET FOREIGN_KEY_CHECKS = 0")?;
    conn.query_drop("SET UNIQUE_CHECKS = 0")?;
    conn.query_drop("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO'")?;

    // Refuse to run against leftover state from an interrupted import — it would
    // otherwise silently discard preserved rows and leave orphan tables that
    // block the next `jibs import`.
    handle_previous_state(&mut conn, config.clean)?;

    // Drop existing FK constraints so the pool's parallel DROP/CREATE TABLE
    // don't hit ERROR 1217/1822. Restored after a successful load (same as import).
    preserve_and_drop_foreign_keys(&mut conn)?;

    let workers = config.client_parallel.unwrap_or(config.parallel).max(1);
    info!("Creating loader pool with {} workers", workers);
    let pool = LoaderPool::new(&config.local_mysql, workers)?;

    let outcome = load_and_finalize(&mut reader, &pool, &mut conn);

    // Always drain the pool before touching FKs so no worker is mid-DDL.
    pool.shutdown();

    match outcome {
        Ok((tables, rows)) => {
            // Re-add FK constraints (still with FK checks disabled), then re-enable.
            restore_foreign_keys(&mut conn)?;
            let _ = conn.query_drop("SET FOREIGN_KEY_CHECKS = 1");
            let _ = conn.query_drop("SET UNIQUE_CHECKS = 1");
            info!("Load complete: {} tables, {} rows", tables, rows);
            Ok(())
        }
        Err(e) => {
            // Leave FK constraints dropped and their persisted definitions
            // intact, matching import's on-failure behaviour: a later successful
            // run re-captures/merges and restores them.
            let _ = conn.query_drop("SET FOREIGN_KEY_CHECKS = 1");
            let _ = conn.query_drop("SET UNIQUE_CHECKS = 1");
            Err(e)
        }
    }
}

/// Detect and (with `--clean`) clear leftover state from a previous interrupted
/// import, mirroring `run_import`'s guard.
fn handle_previous_state(conn: &mut Conn, clean: bool) -> Result<()> {
    let existing_backups = find_backup_tables(conn)?;
    let has_checkpoint = Checkpoint::exists(conn)?;
    if existing_backups.is_empty() && !has_checkpoint {
        return Ok(());
    }

    if clean {
        for backup_table in &existing_backups {
            conn.query_drop(format!("DROP TABLE `{}`", backup_table))?;
            info!("  Dropped {}", backup_table);
        }
        Checkpoint::cleanup(conn)?;
        if has_checkpoint {
            info!("  Dropped checkpoint table");
        }
        return Ok(());
    }

    let mut parts = Vec::new();
    if !existing_backups.is_empty() {
        parts.push(format!("backup tables: {}", existing_backups.join(", ")));
    }
    if has_checkpoint {
        parts.push("checkpoint from an interrupted import".to_string());
    }
    bail!(
        "Found state from a previous interrupted import:\n  {}\n\n\
         Finish or discard it first (e.g. `jibs import --resume`/`--clean`), or pass \
         `jibs load --clean` to discard it before loading (this deletes any preserved \
         rows that only exist in the backup tables).",
        parts.join("\n  ")
    )
}

/// Load all data, then replay preserve restores and post-processing, so the
/// result matches a live import. Returns (tables, rows).
fn load_and_finalize<R: Read>(
    reader: &mut DumpReader<R>,
    pool: &LoaderPool,
    conn: &mut Conn,
) -> Result<(usize, u64)> {
    let loaded = load_records(reader, pool, conn)?;

    // Restore preserved rows before post-processing (import order). Backups were
    // created by load_records just before each table was dropped.
    let backups = find_backup_tables(conn)?;
    if !backups.is_empty() {
        info!("Restoring preserved rows for {} tables", backups.len());
        for backup_table in &backups {
            let table = backup_table
                .strip_prefix(BACKUP_TABLE_PREFIX)
                .unwrap_or(backup_table);
            restore_preserved_rows(conn, table)?;
        }
    }

    // Plan-level post-processing: upsert `set` blocks then `after` SQL.
    if !loaded.sets.is_empty() {
        info!("Executing {} set blocks", loaded.sets.len());
        for set_rule in &loaded.sets {
            execute_set_block(conn, set_rule)?;
        }
    }
    for statement in &loaded.after_statements {
        info!("Running after statement: {}", statement);
        conn.query_drop(statement)?;
    }

    Ok((loaded.tables, loaded.rows))
}

/// Result of consuming a dump stream.
struct Loaded {
    tables: usize,
    rows: u64,
    sets: Vec<SetRule>,
    after_statements: Vec<String>,
}

/// Drive the dump records through the loader pool.
fn load_records<R: Read>(
    reader: &mut DumpReader<R>,
    pool: &LoaderPool,
    conn: &mut Conn,
) -> Result<Loaded> {
    let mut preserves: Vec<PreserveRule> = Vec::new();
    let mut schemas: HashMap<String, Arc<Vec<ColumnDef>>> = HashMap::new();
    let mut pending_ddls: HashMap<String, Receiver<Result<DdlResult>>> = HashMap::new();
    let mut pending_loads: Vec<PendingLoad> = Vec::new();
    let mut sets: Vec<SetRule> = Vec::new();
    let mut after_statements: Vec<String> = Vec::new();
    let mut tables_loaded = 0usize;
    let mut rows_loaded = 0u64;
    let mut saw_end = false;

    while let Some(rec) = reader.next_record()? {
        match rec {
            DumpRecord::Manifest { preserves: p } => {
                preserves = p;
            }
            DumpRecord::Table {
                name,
                columns,
                anon_rules,
            } => {
                // Back up preserved rows on the main connection BEFORE the pool
                // drops and recreates the table.
                let rules: Vec<&PreserveRule> =
                    preserves.iter().filter(|r| r.table == name).collect();
                if !rules.is_empty() {
                    backup_preserved_rows(conn, &name, &rules)?;
                }

                let cols = Arc::new(columns);
                schemas.insert(name.clone(), Arc::clone(&cols));
                let ddl_rx = pool.submit_ddl(name.clone(), cols, anon_rules)?;
                pending_ddls.insert(name, ddl_rx);
            }
            DumpRecord::Chunk {
                table,
                compression,
                row_count: _,
                data,
            } => {
                let schema = schemas.get(&table).cloned().with_context(|| {
                    format!("chunk for unknown table {} (missing schema record)", table)
                })?;

                // Ensure the table exists before loading data into it.
                if let Some(ddl_rx) = pending_ddls.remove(&table) {
                    wait_ddl(&table, ddl_rx)?;
                }

                let rx = pool.submit(table.clone(), schema, data, compression)?;
                pending_loads.push((table, rx));

                if pending_loads.len() > MAX_PENDING_CHUNKS {
                    pending_loads = drain_completed(pending_loads, &mut rows_loaded)?;
                    // If draining freed nothing, block on the oldest to make room.
                    if pending_loads.len() > MAX_PENDING_CHUNKS {
                        let (tbl, rx) = pending_loads.remove(0);
                        rows_loaded += wait_for_load(&tbl, &rx)?.rows;
                    }
                }
            }
            DumpRecord::TableEnd { table, row_count } => {
                // A zero-row table has a Table record but no chunk, so its DDL
                // may still be pending here — force it to complete.
                if let Some(ddl_rx) = pending_ddls.remove(&table) {
                    wait_ddl(&table, ddl_rx)?;
                }
                // Its schema is no longer needed (chunks already submitted, each
                // holding its own Arc); reclaim it like the import path does.
                schemas.remove(&table);
                tables_loaded += 1;
                info!("Loaded {} ({} rows)", table, row_count);
            }
            DumpRecord::PostProcess {
                sets: s,
                after_statements: a,
            } => {
                sets = s;
                after_statements = a;
            }
            DumpRecord::End => {
                saw_end = true;
                break;
            }
        }
    }

    if !saw_end {
        bail!(
            "dump is incomplete: no End terminator found (the export was interrupted \
             or the file is truncated). Re-create the dump before loading."
        );
    }

    // Finish any DDLs that never had a chunk or TableEnd (defensive).
    for (tbl, rx) in pending_ddls.drain() {
        wait_ddl(&tbl, rx)?;
    }
    // Wait for all remaining loads.
    for (tbl, rx) in pending_loads {
        rows_loaded += wait_for_load(&tbl, &rx)?.rows;
    }

    Ok(Loaded {
        tables: tables_loaded,
        rows: rows_loaded,
        sets,
        after_statements,
    })
}

/// Block until a table's CREATE TABLE finishes.
fn wait_ddl(table: &str, rx: Receiver<Result<DdlResult>>) -> Result<()> {
    rx.recv()
        .map_err(|_| anyhow::anyhow!("DDL worker died for {}", table))?
        .map_err(|e| anyhow::anyhow!("DDL error for {}: {}", table, e))?;
    Ok(())
}

/// Non-blocking sweep of completed loads; returns the still-pending ones.
fn drain_completed(pending: Vec<PendingLoad>, rows_loaded: &mut u64) -> Result<Vec<PendingLoad>> {
    let mut still = Vec::new();
    for (table, rx) in pending {
        match rx.try_recv() {
            Ok(Ok(result)) => *rows_loaded += result.rows,
            Ok(Err(e)) => return Err(anyhow::anyhow!("Loader error for {}: {}", table, e)),
            Err(crossbeam_channel::TryRecvError::Empty) => still.push((table, rx)),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                return Err(anyhow::anyhow!("Loader worker died for {}", table))
            }
        }
    }
    Ok(still)
}
