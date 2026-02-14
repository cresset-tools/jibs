//! Jibs Server - Remote component for database imports
//!
//! This binary runs on the remote host and handles:
//! - Connecting to the source MySQL database
//! - Building dependency graphs from relations
//! - Traversing relations to find dependent rows
//! - Streaming data in TSV format for LOAD DATA LOCAL INFILE
//! - Applying anonymization during streaming

mod error;
mod mysql;
mod traversal;
mod tsv;

use std::io::{self, BufReader, BufWriter, Write};

use jibs_protocol::{
    framing::{read_message, write_message},
    ClientMessage, CompressionMode, ServerMessage,
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
        eprintln!("ENVIRONMENT:");
        eprintln!("    JIBS_MYSQL_URL   MySQL connection URL (default: mysql://root@localhost:3306)");
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
        ClientMessage::Init { plan, compression } => {
            eprintln!("Received Init message:");
            eprintln!("  Compression: {:?}", compression);
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

    let mut reader = BufReader::new(stdin.lock());
    let mut writer = BufWriter::new(stdout.lock());

    // Read initial message
    let init_msg: ClientMessage = read_message(&mut reader)?;

    let (plan, client_compression) = match init_msg {
        ClientMessage::Init { plan, compression } => (plan, compression),
        _ => {
            return Err(ServerError::Protocol(
                "Expected Init message".to_string(),
            ));
        }
    };

    // Connect to MySQL (using environment variables for credentials)
    let mysql_url = std::env::var("JIBS_MYSQL_URL")
        .unwrap_or_else(|_| "mysql://root@localhost:3306".to_string());

    let mut conn = MySqlConnection::connect(&mysql_url)?;

    // Discover tables and build table info
    let tables = conn.discover_tables(&plan)?;

    // Negotiate compression
    let compression = negotiate_compression(client_compression);

    // Send Ready message
    let ready_msg = ServerMessage::Ready {
        tables: tables.clone(),
        compression,
    };
    write_message(&mut writer, &ready_msg)?;

    // Wait for client to start
    let msg: ClientMessage = read_message(&mut reader)?;

    match msg {
        ClientMessage::Start { resume_from: _ } => {
            // TODO: handle resume_from for resumable imports
            let mut traverser = DependencyTraverser::new(&mut conn, &plan)?;

            if let Err(e) = traverser.stream_all_tables(compression, &mut writer) {
                let error_msg = ServerMessage::Error {
                    message: e.to_string(),
                    recoverable: e.is_recoverable(),
                };
                write_message(&mut writer, &error_msg)?;
                return Err(e);
            }

            // Send completion message
            write_message(&mut writer, &ServerMessage::Done)?;
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

    // Wait for shutdown (client may send Acks during streaming)
    loop {
        match read_message(&mut reader)? {
            ClientMessage::Shutdown => break,
            ClientMessage::Ack { .. } => continue,
            _ => {
                return Err(ServerError::Protocol(
                    "Expected Ack or Shutdown".to_string(),
                ));
            }
        }
    }

    Ok(())
}

fn negotiate_compression(client_pref: CompressionMode) -> CompressionMode {
    match client_pref {
        CompressionMode::Auto => {
            // For now, default to no compression
            // TODO: benchmark and decide
            CompressionMode::None
        }
        other => other,
    }
}
