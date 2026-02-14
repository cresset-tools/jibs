//! Import orchestration - coordinates the entire import process

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use mysql::prelude::*;
use mysql::{Conn, LocalInfileHandler, Opts};
use tracing::{debug, info, warn};

use jibs_protocol::{
    framing::{read_message, write_message},
    Checkpoint, ClientMessage, ColumnDef, CompressionMode, ExecutionPlan, ServerMessage,
};

use crate::error::ClientError;
use crate::resolver;
use crate::ssh::{get_server_path, RemoteProcess, SshConfig, SshSession};

/// Configuration for an import operation
pub struct ImportConfig {
    pub config_path: PathBuf,
    pub remote_host: String,
    pub local_mysql: String,
    pub vars: HashMap<String, String>,
    pub var_file: Option<PathBuf>,
    pub resume: bool,
    pub parallel: usize,
    pub compression: CompressionMode,
    pub identity_file: Option<PathBuf>,
    pub ssh_port: u16,
}

/// Run the import process
pub async fn run_import(config: ImportConfig) -> Result<()> {
    info!("Starting import from {}", config.remote_host);

    // Parse the .jibs file
    let source = std::fs::read_to_string(&config.config_path)?;
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

    // Resolve the execution plan
    let plan = resolver::resolve(&source, &program, &vars)
        .map_err(|e| anyhow::anyhow!("Resolution failed: {}", e))?;

    info!(
        "Resolved plan: {} aggregates, {} relations, {} excluded tables",
        plan.aggregates.len(),
        plan.relations.len(),
        plan.excluded_tables.len()
    );

    // Connect to SSH
    let ssh_config =
        SshConfig::parse(&config.remote_host, config.ssh_port, config.identity_file.clone())?;
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

    // Disable foreign key checks for import
    local_conn.query_drop("SET FOREIGN_KEY_CHECKS = 0")?;
    local_conn.query_drop("SET UNIQUE_CHECKS = 0")?;

    // Start the remote server
    info!("Starting remote server: {}", server_path);
    let mut server = session.start_process(&server_path).await?;

    // Run the import protocol
    let result = run_protocol(&mut server, &mut local_conn, plan, config.compression).await;

    // Re-enable checks
    local_conn.query_drop("SET FOREIGN_KEY_CHECKS = 1")?;
    local_conn.query_drop("SET UNIQUE_CHECKS = 1")?;

    // Handle result
    match result {
        Ok(stats) => {
            info!(
                "Import complete: {} tables, {} rows",
                stats.tables_imported, stats.rows_imported
            );
            Ok(())
        }
        Err(e) => {
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

/// Run the import protocol with the remote server
async fn run_protocol(
    server: &mut RemoteProcess,
    local_conn: &mut Conn,
    plan: ExecutionPlan,
    compression: CompressionMode,
) -> Result<ImportStats> {
    let mut stats = ImportStats {
        tables_imported: 0,
        rows_imported: 0,
    };

    // Send Init message
    info!("Sending execution plan to server");
    let init_msg = ClientMessage::Init {
        plan: plan.clone(),
        compression,
    };
    send_message(server, &init_msg).await?;

    // Wait for Ready
    let ready_msg: ServerMessage = recv_message(server).await?;
    let (tables, negotiated_compression) = match ready_msg {
        ServerMessage::Ready {
            tables,
            compression,
        } => {
            info!("Server ready: {} tables discovered", tables.len());
            (tables, compression)
        }
        ServerMessage::Error { message, .. } => {
            return Err(anyhow::anyhow!("Server error: {}", message));
        }
        other => {
            return Err(anyhow::anyhow!("Unexpected message: {:?}", other));
        }
    };

    // Log discovered tables
    for table in &tables {
        debug!(
            "  {} (~{} rows)",
            table.name, table.estimated_rows
        );
    }

    // Send Start message
    info!("Starting data transfer");
    let start_msg = ClientMessage::Start { resume_from: None };
    send_message(server, &start_msg).await?;

    // Process incoming messages
    let mut current_table: Option<String> = None;
    let mut current_schema: Vec<ColumnDef> = Vec::new();

    loop {
        let msg: ServerMessage = recv_message(server).await?;

        match msg {
            ServerMessage::Schema { table, columns } => {
                info!("Receiving table: {}", table);
                current_table = Some(table.clone());
                current_schema = columns.clone();

                // Create table in local MySQL
                create_table(local_conn, &table, &columns)?;
            }

            ServerMessage::Data {
                table,
                row_count,
                tsv_data,
                checkpoint,
            } => {
                let decompressed = maybe_decompress(tsv_data, negotiated_compression)?;

                debug!(
                    "Data chunk: {} rows, {} bytes for {}",
                    row_count,
                    decompressed.len(),
                    table
                );

                // Load data into MySQL
                let loaded = load_tsv_data(local_conn, &table, &current_schema, &decompressed)?;
                stats.rows_imported += loaded;

                // Send ack
                let ack_msg = ClientMessage::Ack { checkpoint };
                send_message(server, &ack_msg).await?;
            }

            ServerMessage::TableDone { table, row_count } => {
                info!("Table {} complete: {} rows", table, row_count);
                stats.tables_imported += 1;
                current_table = None;
            }

            ServerMessage::Done => {
                info!("All tables transferred");
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

    // Run after statements
    for statement in &plan.after_statements {
        info!("Running after statement: {}", statement);
        local_conn.query_drop(statement)?;
    }

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
async fn recv_message(server: &mut RemoteProcess) -> Result<ServerMessage> {
    // Read length prefix
    let mut len_bytes = [0u8; 4];
    server
        .read_exact(&mut len_bytes)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read message length: {}", e))?;
    let len = u32::from_le_bytes(len_bytes) as usize;

    if len > 100 * 1024 * 1024 {
        return Err(anyhow::anyhow!("Message too large: {} bytes", len));
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
    // Get the embedded server binary
    // For now, we assume the server is already on the remote host
    // In production, we'd embed the binary and upload it

    // Check if jibs-server exists in PATH
    let (code, stdout, _) = session.exec("which jibs-server").await
        .map_err(|e| anyhow::anyhow!("Failed to check for server: {}", e))?;

    if code == 0 {
        let path = stdout.trim().to_string();
        info!("Using existing server: {}", path);
        return Ok(path);
    }

    // Check for server in /tmp
    let (code, _, _) = session.exec("test -x /tmp/jibs-server").await
        .map_err(|e| anyhow::anyhow!("Failed to check for server: {}", e))?;

    if code == 0 {
        info!("Using existing server: /tmp/jibs-server");
        return Ok("/tmp/jibs-server".to_string());
    }

    // For now, fail if server isn't available
    // TODO: embed and upload server binary
    Err(anyhow::anyhow!(
        "jibs-server not found on remote host. \
         Please copy the server binary to the remote host first."
    ))
}

/// Create a table in local MySQL based on schema
fn create_table(conn: &mut Conn, table: &str, columns: &[ColumnDef]) -> Result<()> {
    // Drop existing table
    conn.query_drop(format!("DROP TABLE IF EXISTS `{}`", table))?;

    let mut column_defs = Vec::new();

    for col in columns {
        let mut def = format!("`{}` {}", col.name, col.type_name);

        if let Some(len) = col.max_length {
            if col.type_name == "VARCHAR" || col.type_name == "CHAR" {
                def.push_str(&format!("({})", len));
            }
        }

        if col.flags.unsigned {
            def.push_str(" UNSIGNED");
        }

        if !col.nullable {
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
    let load_sql = format!(
        "LOAD DATA LOCAL INFILE 'data.tsv' INTO TABLE `{}` \
         FIELDS TERMINATED BY '\\t' \
         LINES TERMINATED BY '\\n' \
         ({})",
        table,
        col_list.join(", ")
    );

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
