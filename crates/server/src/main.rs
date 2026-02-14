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
    ClientMessage, Checkpoint, CompressionMode, ExecutionPlan, ServerMessage,
};

use crate::error::{Result, ServerError};
use crate::mysql::MySqlConnection;
use crate::traversal::DependencyTraverser;

fn main() {
    if let Err(e) = run() {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
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
        tables,
        compression,
    };
    write_message(&mut writer, &ready_msg)?;

    // Main message loop
    loop {
        let msg: ClientMessage = read_message(&mut reader)?;

        match msg {
            ClientMessage::FetchAggregate { name, resume_from } => {
                if let Err(e) = handle_fetch_aggregate(
                    &mut conn,
                    &plan,
                    &name,
                    resume_from,
                    compression,
                    &mut writer,
                ) {
                    let error_msg = ServerMessage::Error {
                        message: e.to_string(),
                        recoverable: e.is_recoverable(),
                    };
                    write_message(&mut writer, &error_msg)?;
                }
            }
            ClientMessage::Ack { checkpoint: _ } => {
                // Flow control acknowledgment - for now just continue
            }
            ClientMessage::Shutdown => {
                break;
            }
            ClientMessage::Init { .. } => {
                return Err(ServerError::Protocol(
                    "Unexpected Init message".to_string(),
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

fn handle_fetch_aggregate<W: Write>(
    conn: &mut MySqlConnection,
    plan: &ExecutionPlan,
    aggregate_name: &str,
    resume_from: Option<Checkpoint>,
    compression: CompressionMode,
    writer: &mut W,
) -> Result<()> {
    // Find the aggregate in the plan
    let aggregate = plan
        .aggregates
        .iter()
        .find(|a| a.name == aggregate_name)
        .ok_or_else(|| ServerError::NotFound(format!("Aggregate '{}'", aggregate_name)))?;

    // Build dependency traverser
    let mut traverser = DependencyTraverser::new(conn, plan)?;

    // Traverse and stream data
    traverser.traverse_and_stream(aggregate, resume_from, compression, writer)?;

    // Send AggregateDone
    let done_msg = ServerMessage::AggregateDone {
        name: aggregate_name.to_string(),
    };
    write_message(writer, &done_msg)?;

    Ok(())
}
