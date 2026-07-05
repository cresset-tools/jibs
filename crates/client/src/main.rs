//! Jibs Client - CLI for database imports
//!
//! This binary handles:
//! - Parsing .jibs DSL files or JSON configuration files
//! - Resolving variables and evaluating conditions
//! - Managing SSH connections to remote hosts
//! - Uploading server binary (CAS-based)
//! - Coordinating data transfer
//! - Loading data into local MySQL

mod error;
mod import;
mod json_config;
mod metrics;
mod progress;
mod resolver;
mod server_binary;
mod ssh;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use crate::import::ImportConfig;

#[derive(Parser)]
#[command(name = "jibs")]
#[command(about = "Jelle's Importer with Better Speed - A MySQL database import tool")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Import data from a remote database
    Import(ImportArgs),
    /// Fetch specific aggregates with custom where clauses
    Get(GetArgs),
    /// Parse and validate a config file (.jibs or .json)
    Check(CheckArgs),
    /// Show resolved execution plan (for debugging)
    Plan(PlanArgs),
}

/// Common connection arguments shared between import and get
#[derive(Args)]
struct ConnectionArgs {
    /// Remote host in format user@host[:port]
    #[arg(long)]
    host: String,

    /// Remote MySQL connection URL (on the remote server)
    #[arg(long, default_value = "mysql://root@localhost:3306")]
    remote_mysql: String,

    /// Local MySQL connection URL
    #[arg(long, default_value = "mysql://root@localhost:3306")]
    local_mysql: String,

    /// Variable assignments (can be repeated)
    #[arg(long = "var", value_parser = parse_var)]
    vars: Vec<(String, String)>,

    /// Path to JSON file with variable values
    #[arg(long = "var-file")]
    var_file: Option<PathBuf>,

    /// Number of parallel server-side workers
    #[arg(long, default_value = "1")]
    parallel: usize,

    /// Number of parallel local MySQL loader workers (defaults to --parallel if not set)
    #[arg(long)]
    client_parallel: Option<usize>,

    /// Disable compression (compression is enabled by default)
    #[arg(long)]
    no_compress: bool,

    /// Path to SSH private key
    #[arg(long)]
    identity: Option<PathBuf>,

    /// SSH port (default: 22)
    #[arg(long, default_value = "22")]
    port: u16,

    /// Strict host key checking: reject unknown host keys
    #[arg(long, conflicts_with_all = ["accept_new_host_keys", "no_host_key_checking"])]
    strict_host_key_checking: bool,

    /// Automatically accept and save new host keys (but reject mismatches)
    #[arg(long, conflicts_with_all = ["strict_host_key_checking", "no_host_key_checking"])]
    accept_new_host_keys: bool,

    /// Disable host key checking entirely (insecure, not recommended)
    #[arg(long, conflicts_with_all = ["strict_host_key_checking", "accept_new_host_keys"])]
    no_host_key_checking: bool,

    /// Maximum message size in bytes (default: 100MB)
    #[arg(long, default_value = "104857600")]
    max_message_size: usize,

    /// Print detailed timing metrics after import
    #[arg(long)]
    metrics: bool,

    /// Print a report of slowest tables after import
    #[arg(long)]
    report: bool,
}

#[derive(Args)]
struct ImportArgs {
    /// Path to the configuration file (.jibs or .json) - imports all tables if not provided
    config: Option<PathBuf>,

    #[command(flatten)]
    connection: ConnectionArgs,

    /// Resume a previously interrupted import
    #[arg(long, conflicts_with = "clean")]
    resume: bool,

    /// Clean up backup tables from a previous interrupted import and start fresh
    #[arg(long, conflicts_with = "resume")]
    clean: bool,

    /// [TEST] Simulate crash after N tables imported (for testing resume)
    /// Only available with --features test-utils
    #[cfg(feature = "test-utils")]
    #[arg(long)]
    fail_after_tables: Option<usize>,
}

#[derive(Args)]
struct GetArgs {
    /// Path to the configuration file (.jibs or .json)
    config: PathBuf,

    #[command(flatten)]
    connection: ConnectionArgs,

    /// Discard state (backup tables, checkpoint) left by a previous interrupted import
    #[arg(long)]
    clean: bool,

    /// Get function invocations: func_name --param1 value1 [func_name2 --param2 value2 ...]
    #[arg(last = true, required = true)]
    queries: Vec<String>,
}

#[derive(Args)]
struct CheckArgs {
    /// Path to the configuration file (.jibs or .json)
    config: PathBuf,
}

#[derive(Args)]
struct PlanArgs {
    /// Path to the configuration file (.jibs or .json)
    config: PathBuf,

    /// Variable assignments (can be repeated)
    #[arg(long = "var", value_parser = parse_var)]
    vars: Vec<(String, String)>,

    /// Path to JSON file with variable values
    #[arg(long = "var-file")]
    var_file: Option<PathBuf>,
}

fn parse_var(s: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = s.splitn(2, '=').collect();
    if parts.len() != 2 {
        return Err("Variable must be in format name=value".to_string());
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

/// Determine host key verification mode from CLI args
fn get_host_key_verification(args: &ConnectionArgs) -> ssh::HostKeyVerification {
    if args.no_host_key_checking {
        ssh::HostKeyVerification::AcceptAll
    } else if args.strict_host_key_checking {
        ssh::HostKeyVerification::Strict
    } else if args.accept_new_host_keys {
        ssh::HostKeyVerification::AcceptNew
    } else {
        ssh::HostKeyVerification::WarnUnknown
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging (routes through MultiProgress when progress bars are active)
    tracing_subscriber::fmt()
        .with_writer(progress::ProgressWriter)
        .with_env_filter(EnvFilter::from_default_env().add_directive("jibs=info".parse()?))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Import(args) => run_import(args).await,
        Commands::Get(args) => run_get(args).await,
        Commands::Check(args) => run_check(args),
        Commands::Plan(args) => run_plan(args),
    }
}

async fn run_import(args: ImportArgs) -> Result<()> {
    use jibs_protocol::CompressionMode;

    let compression = if args.connection.no_compress {
        CompressionMode::None
    } else {
        CompressionMode::Auto
    };

    let host_key_verification = get_host_key_verification(&args.connection);

    let config = ImportConfig {
        config_path: args.config, // None = import all tables
        remote_host: args.connection.host,
        remote_mysql: args.connection.remote_mysql,
        local_mysql: args.connection.local_mysql,
        vars: args.connection.vars.into_iter().collect(),
        var_file: args.connection.var_file,
        resume: args.resume,
        clean: args.clean,
        parallel: args.connection.parallel,
        client_parallel: args.connection.client_parallel,
        compression,
        identity_file: args.connection.identity,
        ssh_port: args.connection.port,
        get_invocations: None,
        host_key_verification,
        max_message_size: args.connection.max_message_size,
        collect_metrics: args.connection.metrics,
        show_report: args.connection.report,
        #[cfg(feature = "test-utils")]
        fail_after_tables: args.fail_after_tables,
    };

    import::run_import(config).await
}

async fn run_get(args: GetArgs) -> Result<()> {
    use jibs_protocol::CompressionMode;

    // Parse get function invocations from trailing args
    let get_invocations = parse_get_invocations(&args.queries)?;

    let compression = if args.connection.no_compress {
        CompressionMode::None
    } else {
        CompressionMode::Auto
    };

    let host_key_verification = get_host_key_verification(&args.connection);

    let config = ImportConfig {
        config_path: Some(args.config), // Required for `get` command
        remote_host: args.connection.host,
        remote_mysql: args.connection.remote_mysql,
        local_mysql: args.connection.local_mysql,
        vars: args.connection.vars.into_iter().collect(),
        var_file: args.connection.var_file,
        resume: false,
        clean: args.clean,
        parallel: args.connection.parallel,
        client_parallel: args.connection.client_parallel,
        compression,
        identity_file: args.connection.identity,
        ssh_port: args.connection.port,
        get_invocations: Some(get_invocations),
        host_key_verification,
        max_message_size: args.connection.max_message_size,
        collect_metrics: args.connection.metrics,
        show_report: args.connection.report,
        #[cfg(feature = "test-utils")]
        fail_after_tables: None,
    };

    import::run_import(config).await
}

/// Parse get function invocations from command line args.
///
/// Format: func_name [--param1 value1 --param2 value2] [func_name2 ...]
/// Function names don't start with "--", so they act as separators.
fn parse_get_invocations(args: &[String]) -> Result<Vec<import::GetInvocation>> {
    use std::collections::HashMap;

    if args.is_empty() {
        anyhow::bail!("Expected at least one get function name");
    }

    let mut invocations = Vec::new();
    let mut i = 0;

    while i < args.len() {
        // First token should be a function name (not starting with --)
        let func_name = &args[i];
        if func_name.starts_with("--") {
            anyhow::bail!(
                "Expected get function name, got flag '{}'. Function names must not start with '--'",
                func_name
            );
        }
        i += 1;

        // Collect --key value pairs until next function name or end
        let mut params = HashMap::new();
        while i < args.len() && args[i].starts_with("--") {
            let key = args[i]
                .strip_prefix("--")
                .unwrap()
                .to_string();
            i += 1;
            if i >= args.len() {
                anyhow::bail!("Expected value for --{}", key);
            }
            let value = args[i].clone();
            i += 1;
            params.insert(key, value);
        }

        invocations.push(import::GetInvocation {
            func_name: func_name.clone(),
            args: params,
        });
    }

    Ok(invocations)
}

fn run_check(args: CheckArgs) -> Result<()> {
    let extension = args
        .config
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    if extension == "json" {
        // Validate JSON config
        let content = std::fs::read_to_string(&args.config)?;
        let _: serde_json::Value = serde_json::from_str(&content)?;
        println!("Valid JSON config file");
        Ok(())
    } else {
        // Validate .jibs DSL
        let source = std::fs::read_to_string(&args.config)?;

        match jibs_parser::parse(&source) {
            Ok(program) => {
                println!(
                    "Parsed {} statements successfully",
                    program.statements.len()
                );
                Ok(())
            }
            Err(errors) => {
                for error in &errors {
                    eprintln!("Error: {}", error);
                }
                anyhow::bail!("Parse failed with {} errors", errors.len());
            }
        }
    }
}

fn run_plan(args: PlanArgs) -> Result<()> {
    // Load variables from file if specified
    let mut vars: std::collections::HashMap<String, String> = args.vars.into_iter().collect();
    if let Some(var_file) = &args.var_file {
        let content = std::fs::read_to_string(var_file)?;
        let file_vars: std::collections::HashMap<String, serde_json::Value> =
            serde_json::from_str(&content)?;
        for (k, v) in file_vars {
            vars.entry(k).or_insert_with(|| match v {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            });
        }
    }

    // Detect file type by extension
    let extension = args
        .config
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

    if extension == "json" {
        // Parse as JSON config
        let plan = json_config::parse_json_config(&args.config, &vars)
            .map_err(|e| anyhow::anyhow!("JSON config error: {}", e))?;
        println!("{}", serde_json::to_string_pretty(&plan)?);
    } else {
        // Parse as .jibs DSL
        let source = std::fs::read_to_string(&args.config)?;

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

        // Resolve the plan
        let resolved = resolver::resolve(&args.config, &program, &vars)?;

        // Print the plan as JSON
        println!("{}", serde_json::to_string_pretty(&resolved.plan)?);

        // Print get functions if any
        if !resolved.get_functions.is_empty() {
            println!("\nGet functions:");
            for func in &resolved.get_functions {
                let params: Vec<String> = func
                    .params
                    .iter()
                    .map(|p| {
                        if let Some(default) = &p.default {
                            format!("{}: {:?} = {}", p.name, p.param_type, default.as_string())
                        } else {
                            format!("{}: {:?}", p.name, p.param_type)
                        }
                    })
                    .collect();
                println!("  {}({}) -> {}", func.name, params.join(", "), func.aggregate_name);
                if let Some(where_template) = &func.where_template {
                    println!("    where \"{}\"", where_template);
                }
                if let Some(limit) = &func.limit {
                    match limit {
                        resolver::LimitOverride::Concrete(n) => println!("    limit {}", n),
                        resolver::LimitOverride::Param(p) => println!("    limit ${}", p),
                    }
                }
            }
        }
    };

    Ok(())
}
