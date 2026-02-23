//! Jibs Server - Remote component for database imports
//!
//! This binary runs on the remote host and handles:
//! - Connecting to the source MySQL database
//! - Building dependency graphs from relations
//! - Traversing relations to find dependent rows
//! - Streaming data in TSV format for LOAD DATA LOCAL INFILE
//! - Applying anonymization during streaming

mod error;
mod metrics;
mod mysql;
mod traversal;
mod tsv;

use std::collections::{HashMap, HashSet};
use std::io::{self, BufReader};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use jibs_protocol::{
    framing::read_message,
    ClientMessage, CompressionMode, MessageWriter, ServerMessage,
};

use crate::error::{Result, ServerError};
use crate::mysql::MySqlConnection;
use crate::traversal::DependencyTraverser;

fn main() {
    // Parse simple command line args
    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!("jibs-server {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    if args.iter().any(|a| a == "--help" || a == "-h") {
        eprintln!("jibs-server - Remote database import server");
        eprintln!();
        eprintln!("USAGE:");
        eprintln!("    jibs-server [OPTIONS]");
        eprintln!();
        eprintln!("OPTIONS:");
        eprintln!("    -h, --help       Print help information");
        eprintln!("    -V, --version    Print version information");
        eprintln!("    --echo           Echo mode: read Init, print plan summary, exit");
        eprintln!();
        eprintln!("CREDENTIALS:");
        eprintln!("    MySQL credentials are received via the protocol (Credentials message).");
        eprintln!("    Fallback: JIBS_MYSQL_URL environment variable (for backward compatibility).");
        return;
    }

    if args.iter().any(|a| a == "--echo") {
        if let Err(e) = run_echo_mode() {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
        return;
    }

    if let Err(e) = run() {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}

/// Echo mode for testing: read Init message and print plan summary
fn run_echo_mode() -> Result<()> {
    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin.lock());

    // Read initial message
    let init_msg: ClientMessage = read_message(&mut reader)?;

    match init_msg {
        ClientMessage::Init { plan, compression, parallel, collect_metrics } => {
            eprintln!("Received Init message:");
            eprintln!("  Compression: {:?}", compression);
            eprintln!("  Parallel: {}", parallel);
            eprintln!("  Collect metrics: {}", collect_metrics);
            eprintln!("  Variables: {}", plan.variables.len());
            eprintln!("  Relations: {}", plan.relations.len());
            eprintln!("  Aggregates: {}", plan.aggregates.len());
            for agg in &plan.aggregates {
                eprintln!("    - {} (root: {}, where: {:?}, limit: {:?})",
                    agg.name, agg.root_table, agg.where_clause, agg.limit);
            }
            eprintln!("  Excluded tables: {:?}", plan.excluded_tables);
            eprintln!("  Anonymization rules: {} tables", plan.anonymization.len());
            eprintln!("  Fakers: {}", plan.fakers.len());
            eprintln!("  After statements: {}", plan.after_statements.len());
            Ok(())
        }
        _ => Err(ServerError::Protocol("Expected Init message".to_string())),
    }
}

fn run() -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();

    let mut reader = BufReader::new(stdin);
    let mut writer = MessageWriter::with_capacity(1024 * 1024, stdout.lock());

    // Read first message - could be Credentials or Init (backward compatibility)
    let first_msg: ClientMessage = read_message(&mut reader)?;

    let (mysql_url, init_msg) = match first_msg {
        ClientMessage::Credentials { mysql_url } => {
            // New protocol: Credentials followed by Init
            let init_msg: ClientMessage = read_message(&mut reader)?;
            (mysql_url, init_msg)
        }
        init @ ClientMessage::Init { .. } => {
            // Backward compatibility: Init without Credentials, use env var
            let mysql_url = std::env::var("JIBS_MYSQL_URL")
                .unwrap_or_else(|_| "mysql://root@localhost:3306".to_string());
            (mysql_url, init)
        }
        _ => {
            return Err(ServerError::Protocol(
                "Expected Credentials or Init message".to_string(),
            ));
        }
    };

    let (mut plan, client_compression, parallel, collect_metrics) = match init_msg {
        ClientMessage::Init { plan, compression, parallel, collect_metrics } => (plan, compression, parallel, collect_metrics),
        _ => {
            return Err(ServerError::Protocol(
                "Expected Init message".to_string(),
            ));
        }
    };

    // Connect to MySQL using credentials received via protocol
    let mut conn = MySqlConnection::connect(&mysql_url)?;

    // Discover tables and build table info (assigns u16 table IDs)
    let tables = conn.discover_tables(&mut plan)?;

    // Build table name → u16 ID mapping for the wire protocol
    let table_name_to_id: Arc<HashMap<String, u16>> = Arc::new(
        tables.iter().map(|t| (t.name.clone(), t.table_id)).collect(),
    );

    // Discover and merge database FK relations
    let explicit_count = plan.relations.len();
    let db_relations = conn.discover_foreign_keys()?;
    let existing: HashSet<(String, String, String, String)> = plan
        .relations
        .iter()
        .map(|r| {
            (
                r.from_table.clone(),
                r.from_column.clone(),
                r.to_table.clone(),
                r.to_column.clone(),
            )
        })
        .collect();
    let mut added = 0usize;
    for rel in db_relations {
        let key = (
            rel.from_table.clone(),
            rel.from_column.clone(),
            rel.to_table.clone(),
            rel.to_column.clone(),
        );
        if !existing.contains(&key) {
            plan.relations.push(rel);
            added += 1;
        }
    }
    // Filter out ignored relations
    let ignored_count = if !plan.ignored_relations.is_empty() {
        let before = plan.relations.len();
        let ignored = &plan.ignored_relations;
        plan.relations.retain(|r| {
            !ignored.iter().any(|ir| {
                ir.from_table == r.from_table
                    && ir.from_column == r.from_column
                    && ir.to_table == r.to_table
                    && ir.to_column == r.to_column
            })
        });
        before - plan.relations.len()
    } else {
        0
    };

    eprintln!(
        "Relations: {} explicit, {} discovered from FK constraints, {} ignored",
        explicit_count, added, ignored_count
    );

    // Negotiate compression
    let compression = negotiate_compression(client_compression);

    // Send Ready message
    let ready_msg = ServerMessage::Ready {
        tables: tables.clone(),
        compression,
    };
    writer.write_message(&ready_msg)?;

    // Wait for client to start
    let msg: ClientMessage = read_message(&mut reader)?;

    match msg {
        ClientMessage::Start => {
            // Spawn interrupt listener thread — reads from stdin for Interrupt/Shutdown
            let interrupt = Arc::new(AtomicBool::new(false));
            let interrupt_clone = Arc::clone(&interrupt);
            let listener_handle = std::thread::spawn(move || {
                loop {
                    match read_message::<_, ClientMessage>(&mut reader) {
                        Ok(ClientMessage::Interrupt) | Ok(ClientMessage::Shutdown) => {
                            interrupt_clone.store(true, Ordering::SeqCst);
                            break;
                        }
                        Ok(_) => continue,
                        Err(_) => {
                            // Connection lost — treat as interrupt
                            interrupt_clone.store(true, Ordering::SeqCst);
                            break;
                        }
                    }
                }
            });

            let mut traverser = DependencyTraverser::new(&mut conn, &plan, collect_metrics, Arc::clone(&table_name_to_id))?;

            let table_dispositions = match traverser.stream_all_tables(compression, &mut writer, parallel, &mysql_url, &interrupt) {
                Ok(dispositions) => dispositions,
                Err(e) => {
                    // On interrupt, still send Done with partial metrics
                    if interrupt.load(Ordering::SeqCst) {
                        let metrics = traverser.get_metrics();
                        let _ = writer.write_message(&ServerMessage::Done {
                            table_dispositions: Vec::new(),
                            metrics,
                        });
                        let _ = listener_handle.join();
                        return Ok(());
                    }
                    let error_msg = ServerMessage::Error {
                        message: e.to_string(),
                        recoverable: e.is_recoverable(),
                    };
                    writer.write_message(&error_msg)?;
                    return Err(e);
                }
            };

            // Get metrics if enabled
            let metrics = traverser.get_metrics();

            // Send completion message
            writer.write_message(&ServerMessage::Done { table_dispositions, metrics })?;

            // Wait for listener thread (will get Shutdown from client)
            let _ = listener_handle.join();
        }
        ClientMessage::Shutdown => {
            return Ok(());
        }
        _ => {
            return Err(ServerError::Protocol(
                "Expected Start or Shutdown".to_string(),
            ));
        }
    }

    Ok(())
}

fn negotiate_compression(client_pref: CompressionMode) -> CompressionMode {
    match client_pref {
        CompressionMode::Auto => CompressionMode::Zstd,
        other => other,
    }
}
