//! Import orchestration - coordinates the entire import process

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use mysql::prelude::*;
use mysql::{Conn, LocalInfileHandler, Opts};
use tracing::info;

use jibs_protocol::{ColumnDef, CompressionMode};

use crate::resolver;
use crate::ssh::{get_server_path, SshConfig, SshSession};

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

    info!("Resolved {} aggregates", plan.aggregates.len());

    // Connect to SSH
    let ssh_config = SshConfig::parse(&config.remote_host, config.ssh_port, config.identity_file)?;
    info!("Connecting to {}@{}:{}", ssh_config.user, ssh_config.host, ssh_config.port);
    let session = SshSession::connect(ssh_config).await?;

    // Deploy server binary if needed
    deploy_server(&session).await?;

    // Connect to local MySQL
    let local_opts = Opts::from_url(&config.local_mysql)
        .map_err(|e| anyhow::anyhow!("Invalid local MySQL URL: {}", e))?;
    let mut local_conn = Conn::new(local_opts)?;

    // Start the server on remote host
    let server_path = get_server_path(&[]); // TODO: use actual binary
    info!("Starting remote server at {}", server_path);

    // For now, we'll implement a simplified version that doesn't use the remote server
    // This is a placeholder for the full implementation
    info!("Import process would run here");
    info!("Plan has {} aggregates, {} relations", plan.aggregates.len(), plan.relations.len());

    // Run after statements
    for statement in &plan.after_statements {
        info!("Running after statement: {}", statement);
        local_conn.query_drop(statement)?;
    }

    info!("Import complete");
    Ok(())
}

/// Deploy the server binary to the remote host if needed
async fn deploy_server(_session: &SshSession) -> Result<()> {
    // For now, we'll skip actual binary deployment since we need cross-compilation
    // In a full implementation, we'd:
    // 1. Compute hash of the server binary
    // 2. Check if binary exists at /tmp/jibs-{hash}
    // 3. Upload if missing

    info!("Server deployment skipped (not yet implemented)");
    Ok(())
}

/// State for receiving data from the server
struct ImportState {
    /// Current table being imported
    current_table: Option<String>,
    /// Schema for the current table
    current_schema: Vec<ColumnDef>,
    /// Buffer for TSV data
    tsv_buffer: Vec<u8>,
    /// Total rows imported
    total_rows: u64,
}

impl ImportState {
    fn new() -> Self {
        Self {
            current_table: None,
            current_schema: Vec::new(),
            tsv_buffer: Vec::new(),
            total_rows: 0,
        }
    }
}

/// Create a table in local MySQL based on schema
fn create_table(conn: &mut Conn, table: &str, columns: &[ColumnDef]) -> Result<()> {
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
        "CREATE TABLE IF NOT EXISTS `{}` (\n  {}\n)",
        table,
        column_defs.join(",\n  ")
    );

    conn.query_drop(&create_sql)?;
    Ok(())
}

/// Load TSV data into a table using LOAD DATA LOCAL INFILE
fn load_tsv_data(conn: &mut Conn, table: &str, columns: &[ColumnDef], tsv_data: &[u8]) -> Result<u64> {
    use std::io::Write;

    // Set up the local infile handler
    let data = tsv_data.to_vec();

    // Create a handler that writes our data to the LocalInfile
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

    // Get affected rows
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
