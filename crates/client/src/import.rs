//! Import orchestration - coordinates the entire import process

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use mysql::prelude::*;
use mysql::{Conn, Opts};
use tracing::{debug, info};

use jibs_protocol::{
    ClientMessage, CompressionMode, ExecutionPlan, MessageWriter,
};

use crate::checkpoint::{find_backup_tables, Checkpoint};
use crate::dry_run::run_dry_run;
use crate::foreign_keys::{
    apply_foreign_keys, discard_preserved_foreign_keys, preserve_and_drop_foreign_keys,
    restore_foreign_keys,
};
use crate::loader::LoaderPool;
use crate::protocol::{perform_handshake, run_protocol, send_message, ProtocolConfig};
use crate::report::display_report;
use crate::resolver::{self, LimitOverride, ResolvedGetFunction};
use crate::server_binary;
use crate::ssh::{get_server_path, SshConfig, SshSession};
pub struct GetInvocation {
    pub func_name: String,
    pub args: HashMap<String, String>,
}

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
    /// Report what would be imported without touching the local database
    pub dry_run: bool,
    /// Write the stream to a `.jibsdump` file instead of loading into a local
    /// database. When set, the local MySQL connection is never opened.
    pub dump_to: Option<PathBuf>,
    pub parallel: usize,
    /// Number of client-side loader pool workers (None = use `parallel` value)
    pub client_parallel: Option<usize>,
    pub compression: CompressionMode,
    pub identity_file: Option<PathBuf>,
    pub ssh_port: u16,
    /// For `get` command: function invocations with named arguments
    pub get_invocations: Option<Vec<GetInvocation>>,
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
    let (mut plan, get_functions) = if let Some(config_path) = &config.config_path {
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
            // Parse as JSON config (no get functions in JSON configs)
            let plan = crate::json_config::parse_json_config(config_path, &vars)
                .map_err(|e| anyhow::anyhow!("JSON config error: {}", e))?;
            (plan, Vec::new())
        } else {
            // Parse as .jibs DSL
            let source = std::fs::read_to_string(config_path)?;
            let program = jibs_parser::parse(&source).map_err(|errors| {
                anyhow::anyhow!(
                    "{} parse error{} in {}:\n{}",
                    errors.len(),
                    if errors.len() == 1 { "" } else { "s" },
                    config_path.display(),
                    jibs_parser::render_errors(
                        &config_path.display().to_string(),
                        &source,
                        &errors,
                        false,
                    )
                )
            })?;

            // Resolve the execution plan
            let resolved = resolver::resolve(config_path, &program, &vars)
                .map_err(|e| anyhow::anyhow!("Resolution failed: {}", e))?;
            (resolved.plan, resolved.get_functions)
        }
    } else {
        // No config file - import all tables
        info!("No config file specified, importing all tables");
        (ExecutionPlan::default(), Vec::new())
    };

    // Apply get function invocations if this is a `get` command
    if let Some(invocations) = &config.get_invocations {
        plan = apply_get_invocations(plan, &get_functions, invocations)?;
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

    // Dry run: ask the server what would happen; never touch the local DB
    if config.dry_run {
        return run_dry_run(&config, &session, &server_path, plan).await;
    }

    // Export mode: stream to a .jibsdump file instead of a local database.
    if let Some(dump_path) = &config.dump_to {
        return crate::export::run_export(&session, &server_path, plan, &config, dump_path).await;
    }

    // Connect to local MySQL
    info!(
        "Connecting to local MySQL: {}",
        redact_mysql_url(&config.local_mysql)
    );
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
            let hint = if config.get_invocations.is_some() {
                "Use `jibs import --resume` to finish the interrupted import first, or\n\
                 pass --clean to discard the state (this deletes any preserved rows\n\
                 that only exist in the backup tables)."
            } else {
                "Use --resume to continue the interrupted import, or\n\
                 Use --clean to discard the state and start fresh."
            };
            return Err(anyhow::anyhow!(
                "Found state from a previous interrupted import:\n  {}\n\n{}",
                state_parts.join("\n  "),
                hint
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

    // Capture all FK constraints in the local database, then drop them to
    // prevent MySQL ERROR 1822 during parallel table recreation. They are
    // restored after the import completes (see restore_foreign_keys below).
    preserve_and_drop_foreign_keys(&mut local_conn)?;

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

    // Protocol version handshake — before any framed message
    perform_handshake(&mut server).await?;

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

    // Reconstruct FK constraints on success (while FOREIGN_KEY_CHECKS is still 0).
    // Prefer the authoritative source-schema FKs the server reported — they
    // rebuild constraints even when the local database started empty, which the
    // target-preserve path cannot. Fall back to the target's own captured FKs
    // when the server reported none (e.g. an interrupted run yields an empty set,
    // but that path also leaves result Err, so this only applies on a clean run
    // against an older server). On failure the persisted definitions are kept so
    // a later successful run (e.g. after --resume) restores them.
    if outcome.result.is_ok() {
        if outcome.stats.source_foreign_keys.is_empty() {
            restore_foreign_keys(&mut local_conn)?;
        } else {
            apply_foreign_keys(&mut local_conn, &outcome.stats.source_foreign_keys)?;
            discard_preserved_foreign_keys(&mut local_conn)?;
        }
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

fn apply_get_invocations(
    mut plan: ExecutionPlan,
    get_functions: &[ResolvedGetFunction],
    invocations: &[GetInvocation],
) -> Result<ExecutionPlan> {
    let mut new_aggregates = Vec::new();

    for (idx, invocation) in invocations.iter().enumerate() {
        // Look up the get function by name
        let func = get_functions
            .iter()
            .find(|f| f.name == invocation.func_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Get function '{}' not found in config. Available get functions: {}",
                    invocation.func_name,
                    if get_functions.is_empty() {
                        "(none)".to_string()
                    } else {
                        get_functions
                            .iter()
                            .map(|f| f.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    }
                )
            })?;

        // Resolve parameters: merge CLI args with defaults
        let mut resolved_params: HashMap<String, String> = HashMap::new();
        for param in &func.params {
            if let Some(cli_value) = invocation.args.get(&param.name) {
                resolved_params.insert(param.name.clone(), cli_value.clone());
            } else if let Some(default) = &param.default {
                resolved_params.insert(param.name.clone(), default.as_string());
            } else {
                anyhow::bail!(
                    "Get function '{}' requires parameter '--{}' (type: {:?})",
                    invocation.func_name,
                    param.name,
                    param.param_type,
                );
            }
        }

        // Find the base aggregate
        let base = plan
            .aggregates
            .iter()
            .find(|a| a.name == func.aggregate_name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Get function '{}' references aggregate '{}' which was not found",
                    func.name,
                    func.aggregate_name
                )
            })?;

        // Clone and apply overrides
        let mut modified = base.clone();
        modified.name = format!("{}_{}", func.name, idx);

        // Apply WHERE template with parameter substitution
        if let Some(template) = &func.where_template {
            let mut where_clause = template.clone();
            for (param_name, value) in &resolved_params {
                where_clause = where_clause.replace(
                    &format!("{{{}}}", param_name),
                    value,
                );
            }
            modified.where_clause = Some(where_clause);
        }

        // Apply order_by override
        if let Some(order_by) = &func.order_by {
            modified.order_by = Some(order_by.clone());
        }
        if let Some(direction) = &func.order_direction {
            modified.order_direction = Some(*direction);
        }

        // Apply limit override
        if let Some(limit) = &func.limit {
            match limit {
                LimitOverride::Concrete(n) => {
                    modified.limit = Some(*n);
                }
                LimitOverride::Param(param_name) => {
                    let value_str = resolved_params.get(param_name).ok_or_else(|| {
                        anyhow::anyhow!(
                            "Limit references parameter '{}' which was not resolved",
                            param_name
                        )
                    })?;
                    let limit_val: i64 = value_str.parse().map_err(|_| {
                        anyhow::anyhow!(
                            "Parameter '{}' value '{}' is not a valid integer for limit",
                            param_name,
                            value_str
                        )
                    })?;
                    modified.limit = Some(limit_val);
                }
            }
        }

        // Apply exclude overrides
        if !func.exclude_tables.is_empty() {
            modified.exclude_tables = func.exclude_tables.clone();
        }
        if !func.exclude_patterns.is_empty() {
            modified.exclude_patterns = func.exclude_patterns.clone();
        }

        // Apply root_only override
        if let Some(root_only) = func.root_only {
            modified.root_only = root_only;
        }

        new_aggregates.push(modified);
    }

    // For `get`, strip the plan down to only what's needed:
    // - The new aggregates from get functions
    // - Relations (needed for BFS traversal)
    // - Excluded/ignored tables and patterns (still relevant for BFS)
    // - Full tables (kept as BFS dead-ends, but not imported)
    // - Anonymization and fakers (applied server-side; must stay in the plan
    //   or `get` would stream raw, un-anonymized production data)
    // Post-processing (preserves, sets, after_statements) is not relevant for `get`.
    plan.aggregates = new_aggregates;
    plan.aggregates_only = true;
    plan.preserves.clear();
    plan.sets.clear();
    plan.after_statements.clear();
    Ok(plan)
}

/// Mask the password portion of a MySQL URL for safe logging.
pub(crate) fn redact_mysql_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let rest = &url[scheme_end + 3..];
    // Userinfo ends at the last '@' before the path
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let Some(at) = rest[..authority_end].rfind('@') else {
        return url.to_string();
    };
    let userinfo = &rest[..at];
    let Some(colon) = userinfo.find(':') else {
        return url.to_string();
    };
    format!(
        "{}{}:***{}",
        &url[..scheme_end + 3],
        &userinfo[..colon],
        &rest[at..]
    )
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_masks_password() {
        assert_eq!(
            redact_mysql_url("mysql://root:s3cret@127.0.0.1:3308/imported"),
            "mysql://root:***@127.0.0.1:3308/imported"
        );
    }

    #[test]
    fn redact_keeps_url_without_password() {
        assert_eq!(
            redact_mysql_url("mysql://root@localhost:3306"),
            "mysql://root@localhost:3306"
        );
    }

    #[test]
    fn redact_handles_at_and_colon_in_password() {
        // Password containing ':' — everything after the first ':' is masked
        assert_eq!(
            redact_mysql_url("mysql://user:pa:ss@host/db"),
            "mysql://user:***@host/db"
        );
    }

    #[test]
    fn redact_passes_through_non_urls() {
        assert_eq!(redact_mysql_url("not a url"), "not a url");
    }





}
