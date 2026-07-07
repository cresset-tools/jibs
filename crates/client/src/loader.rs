//! Loader pool - parallel MySQL connections for LOAD DATA, plus the
//! TSV-loading and decompression helpers shared with the sequential path

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use mysql::prelude::*;
use mysql::{Conn, LocalInfileHandler, Opts};
use tracing::{debug, info};

use jibs_protocol::{ColumnDef, CompressionMode};

use crate::import::redact_mysql_url;
use crate::sql::create_table;

// ============================================================================
// Loader Pool - manages parallel MySQL connections for data loading
// ============================================================================

/// Result from a DDL (CREATE TABLE) operation
pub(crate) struct DdlResult {
    pub(crate) ddl_ns: u64,
}

/// Work item for loader workers
enum LoadWork {
    /// Create (or recreate) a table — must complete before any LoadData for the same table
    CreateTable {
        table: String,
        columns: Arc<Vec<ColumnDef>>,
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
pub(crate) struct LoaderPool {
    work_tx: crossbeam_channel::Sender<LoadWork>,
    worker_handles: Vec<std::thread::JoinHandle<()>>,
}

impl LoaderPool {
    /// Create a new loader pool with N workers
    /// Returns an error if any worker fails to initialize
    pub(crate) fn new(mysql_url: &str, num_workers: usize) -> Result<Self> {
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
                                let rows = load_tsv_data(
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
                        redact_mysql_url(mysql_url)
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
    pub(crate) fn submit_ddl(
        &self,
        table: String,
        columns: Arc<Vec<ColumnDef>>,
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
    pub(crate) fn submit(
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
    pub(crate) fn shutdown(self) {
        // Drop the sender to signal workers to stop
        drop(self.work_tx);

        // Wait for all workers
        for handle in self.worker_handles {
            let _ = handle.join();
        }
    }
}

/// Build the LOAD DATA LOCAL INFILE statement for a table.
///
/// Binary-typed columns arrive hex-encoded in the TSV stream (the server
/// cannot put raw bytes in a text stream safely), so they are read into
/// user variables and decoded with UNHEX() in a SET clause.
fn build_load_data_sql(table: &str, columns: &[ColumnDef]) -> String {
    let mut col_list: Vec<String> = Vec::with_capacity(columns.len());
    let mut set_clauses: Vec<String> = Vec::new();

    for (i, col) in columns.iter().enumerate() {
        if col.is_binary_type() {
            col_list.push(format!("@jibs_hex_{}", i));
            set_clauses.push(format!("`{}` = UNHEX(@jibs_hex_{})", col.name, i));
        } else {
            col_list.push(format!("`{}`", col.name));
        }
    }

    let mut sql = format!(
        r"LOAD DATA LOCAL INFILE 'data.tsv' INTO TABLE `{}` FIELDS TERMINATED BY '\t' ESCAPED BY '\\' LINES TERMINATED BY '\n' ({})",
        table,
        col_list.join(", ")
    );
    if !set_clauses.is_empty() {
        sql.push_str(" SET ");
        sql.push_str(&set_clauses.join(", "));
    }
    sql
}

/// Load TSV data into a table using LOAD DATA LOCAL INFILE
pub(crate) fn load_tsv_data(
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

    // Execute LOAD DATA LOCAL INFILE
    let load_sql = build_load_data_sql(table, columns);

    debug!("LOAD DATA SQL (worker): {}", load_sql);
    let result = conn.query_iter(&load_sql)?;
    let affected = result.affected_rows();

    Ok(affected)
}

pub(crate) struct LoadResult {
    pub(crate) rows: u64,
    pub(crate) decompress_ns: u64,
    pub(crate) load_ns: u64,
}

/// An in-flight load: the table it belongs to and the channel its worker will
/// report completion on. Shared by the import protocol and the dump loader.
pub(crate) type PendingLoad = (String, crossbeam_channel::Receiver<Result<LoadResult>>);

/// Block until a specific load finishes, mapping channel/worker failures to a
/// descriptive error.
pub(crate) fn wait_for_load(
    table: &str,
    rx: &crossbeam_channel::Receiver<Result<LoadResult>>,
) -> Result<LoadResult> {
    match rx.recv() {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(e)) => Err(anyhow::anyhow!("Loader error for {}: {}", table, e)),
        Err(_) => Err(anyhow::anyhow!("Loader worker died for {}", table)),
    }
}


pub(crate) fn maybe_decompress(data: Vec<u8>, compression: CompressionMode) -> Result<Vec<u8>> {
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


#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, type_name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            type_name: type_name.to_string(),
            full_type: type_name.to_lowercase(),
            max_length: None,
            nullable: true,
            is_primary_key: false,
            charset: None,
            collation: None,
            flags: Default::default(),
        }
    }
    #[test]
    fn load_data_sql_plain_columns() {
        let columns = vec![col("id", "INT"), col("name", "VARCHAR")];
        assert_eq!(
            build_load_data_sql("users", &columns),
            r"LOAD DATA LOCAL INFILE 'data.tsv' INTO TABLE `users` FIELDS TERMINATED BY '\t' ESCAPED BY '\\' LINES TERMINATED BY '\n' (`id`, `name`)"
        );
    }
    #[test]
    fn load_data_sql_binary_columns_use_unhex() {
        let columns = vec![col("id", "INT"), col("data", "BLOB"), col("tag", "VARBINARY")];
        assert_eq!(
            build_load_data_sql("files", &columns),
            r"LOAD DATA LOCAL INFILE 'data.tsv' INTO TABLE `files` FIELDS TERMINATED BY '\t' ESCAPED BY '\\' LINES TERMINATED BY '\n' (`id`, @jibs_hex_1, @jibs_hex_2) SET `data` = UNHEX(@jibs_hex_1), `tag` = UNHEX(@jibs_hex_2)"
        );
    }
}
