//! `jibs import --dump-to <file>`: stream an import into a `.jibsdump` file
//! instead of loading it into a local database.
//!
//! This drives the same remote server and protocol as a normal import — the
//! only difference is the sink. Because anonymization, aggregate traversal and
//! compression all happen on the server side, the dump captures exactly what a
//! live import would have loaded (already anonymized).
//!
//! The dump is written to a `<file>.part` temp path and atomically renamed to
//! `<file>` only after the terminating `End` record is flushed, so an
//! interrupted export never leaves a half-written dump that would load as a
//! silently-incomplete database.

use std::ffi::OsString;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tracing::{info, warn};

use jibs_protocol::{ClientMessage, ExecutionPlan, MessageWriter, ServerMessage};

use crate::dump::DumpWriter;
use crate::import::ImportConfig;
use crate::protocol::{perform_handshake, recv_message, send_message};
use crate::ssh::{RemoteProcess, SshSession};

/// Run an import against the remote server and write the stream to `dump_path`.
pub(crate) async fn run_export(
    session: &SshSession,
    server_path: &str,
    plan: ExecutionPlan,
    config: &ImportConfig,
    dump_path: &Path,
) -> Result<()> {
    // Flags that only make sense when loading into a live database.
    if config.collect_metrics || config.show_report || config.client_parallel.is_some() {
        warn!(
            "--metrics, --report and --client-parallel have no effect with --dump-to \
             (they apply when loading; use them on `jibs load`)"
        );
    }

    // Write to a sibling temp file and rename on success (same directory, so
    // the rename is atomic on the same filesystem).
    let tmp_path = part_path(dump_path);
    let file = File::create(&tmp_path)
        .with_context(|| format!("failed to create dump file {}", tmp_path.display()))?;
    let mut dump = DumpWriter::new(BufWriter::new(file))?;

    // Manifest carries plan-level rules the loader needs up front (preserve).
    dump.write_manifest(&plan.preserves)?;

    // Start the remote server and complete the version handshake.
    info!("Starting remote server: {}", server_path);
    let mut server = session.start_process(server_path).await?;

    // On any failure, remove the partial temp file so it can't later be loaded
    // as if it were complete.
    match stream_to_dump(&mut server, &mut dump, &plan, config).await {
        Ok((tables, rows)) => {
            // Terminator + flush, then publish the file atomically.
            dump.finish()?;
            std::fs::rename(&tmp_path, dump_path).with_context(|| {
                format!("failed to finalize dump {}", dump_path.display())
            })?;
            // Best-effort clean shutdown so the server exits promptly.
            let mut encoder: MessageWriter<()> = MessageWriter::with_capacity(64, ());
            let _ = send_message(&mut server, &mut encoder, &ClientMessage::Shutdown).await;
            info!(
                "Wrote {} ({} tables, {} rows). Load it with: jibs load {}",
                dump_path.display(),
                tables,
                rows,
                dump_path.display()
            );
            Ok(())
        }
        Err(e) => {
            drop(dump);
            if let Err(rm) = std::fs::remove_file(&tmp_path) {
                warn!("could not remove partial dump {}: {}", tmp_path.display(), rm);
            }
            Err(e)
        }
    }
}

/// Drive the protocol and write each message to the dump. Returns (tables, rows).
async fn stream_to_dump(
    server: &mut RemoteProcess,
    dump: &mut DumpWriter<BufWriter<File>>,
    plan: &ExecutionPlan,
    config: &ImportConfig,
) -> Result<(usize, u64)> {
    perform_handshake(server).await?;

    // Credentials go in their own message so they never appear in a process
    // listing (same as the import path).
    let mut encoder: MessageWriter<()> = MessageWriter::with_capacity(4096, ());
    send_message(
        server,
        &mut encoder,
        &ClientMessage::Credentials {
            mysql_url: config.remote_mysql.clone(),
        },
    )
    .await?;

    // Send the plan. Never dry-run here; never collect metrics.
    send_message(
        server,
        &mut encoder,
        &ClientMessage::Init {
            plan: plan.clone(),
            compression: config.compression,
            parallel: config.parallel as u32,
            collect_metrics: false,
            dry_run: false,
        },
    )
    .await?;

    // Wait for Ready: interned table ids + the compression the server negotiated.
    let (tables, negotiated_compression) =
        match recv_message(server, config.max_message_size).await? {
            ServerMessage::Ready { tables, compression } => (tables, compression),
            ServerMessage::Error { message, .. } => {
                return Err(anyhow::anyhow!("Server error: {}", message))
            }
            other => {
                return Err(anyhow::anyhow!(
                    "Unexpected message while waiting for Ready: {:?}",
                    other
                ))
            }
        };
    let id_to_name: std::collections::HashMap<u16, String> =
        tables.iter().map(|t| (t.table_id, t.name.clone())).collect();

    info!("Dumping {} tables", tables.len());
    send_message(server, &mut encoder, &ClientMessage::Start).await?;

    let mut tables_written = 0usize;
    let mut rows_written = 0u64;

    loop {
        match recv_message(server, config.max_message_size).await? {
            ServerMessage::Schema { table_id, columns } => {
                let table = lookup(&id_to_name, table_id, "Schema")?;
                let anon = plan.anonymization.get(table).map(Vec::as_slice);
                dump.write_table(table, &columns, anon)?;
            }
            ServerMessage::Data {
                table_id,
                row_count,
                tsv_data,
            } => {
                let table = lookup(&id_to_name, table_id, "Data")?;
                dump.write_chunk(table, negotiated_compression, row_count, tsv_data)?;
            }
            ServerMessage::TableDone {
                table_id, row_count, ..
            } => {
                let table = lookup(&id_to_name, table_id, "TableDone")?;
                dump.write_table_end(table, row_count)?;
                tables_written += 1;
                rows_written += row_count;
            }
            ServerMessage::Done { .. } => break,
            ServerMessage::Error {
                message,
                recoverable,
            } => {
                // Mirror the import path: recoverable errors warn and continue
                // (the dump ends up exactly as complete as a live import would).
                // The `End` terminator is only written on a clean `Done`, so a
                // stream that never completes is discarded rather than published.
                if recoverable {
                    warn!("Recoverable server error: {}", message);
                } else {
                    return Err(anyhow::anyhow!("Server error during export: {}", message));
                }
            }
            ServerMessage::Ready { .. } => {
                return Err(anyhow::anyhow!("Unexpected Ready message during streaming"))
            }
            ServerMessage::DryRunReport { .. } => {
                return Err(anyhow::anyhow!("Unexpected DryRunReport (dry_run not requested)"))
            }
        }
    }

    // Capture plan-level post-processing so a later `load` reproduces exactly
    // what a live import would have produced (set/upsert blocks + after SQL).
    dump.write_post_process(&plan.sets, &plan.after_statements)?;

    Ok((tables_written, rows_written))
}

/// Sibling temp path for atomic publish: `<path>.part`.
fn part_path(dump_path: &Path) -> PathBuf {
    let mut s: OsString = dump_path.as_os_str().to_owned();
    s.push(".part");
    PathBuf::from(s)
}

fn lookup<'a>(
    id_to_name: &'a std::collections::HashMap<u16, String>,
    table_id: u16,
    context: &str,
) -> Result<&'a String> {
    id_to_name
        .get(&table_id)
        .ok_or_else(|| anyhow::anyhow!("Unknown table_id {} in {}", table_id, context))
}
