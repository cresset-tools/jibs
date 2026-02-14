//! Jibs Client - CLI for database imports
//!
//! This binary handles:
//! - Parsing .jibs DSL files
//! - Resolving variables and evaluating conditions
//! - Managing SSH connections to remote hosts
//! - Uploading server binary (CAS-based)
//! - Coordinating data transfer
//! - Loading data into local MySQL

mod error;
mod import;
mod resolver;
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
    /// Parse and validate a .jibs file
    Check(CheckArgs),
    /// Show resolved execution plan (for debugging)
    Plan(PlanArgs),
}

#[derive(Args)]
struct ImportArgs {
    /// Path to the .jibs configuration file
    config: PathBuf,

    /// Remote host in format user@host[:port]
    #[arg(long)]
    host: String,

    /// Local MySQL connection URL
    #[arg(long, default_value = "mysql://root@localhost:3306")]
    local_mysql: String,

    /// Variable assignments (can be repeated)
    #[arg(long = "var", value_parser = parse_var)]
    vars: Vec<(String, String)>,

    /// Path to JSON file with variable values
    #[arg(long = "var-file")]
    var_file: Option<PathBuf>,

    /// Resume a previously interrupted import
    #[arg(long)]
    resume: bool,

    /// Number of parallel SSH sessions
    #[arg(long, default_value = "1")]
    parallel: usize,

    /// Force compression
    #[arg(long, conflicts_with = "no_compress")]
    compress: bool,

    /// Disable compression
    #[arg(long, conflicts_with = "compress")]
    no_compress: bool,

    /// Path to SSH private key
    #[arg(long)]
    identity: Option<PathBuf>,

    /// SSH port (default: 22)
    #[arg(long, default_value = "22")]
    port: u16,
}

#[derive(Args)]
struct CheckArgs {
    /// Path to the .jibs configuration file
    config: PathBuf,
}

#[derive(Args)]
struct PlanArgs {
    /// Path to the .jibs configuration file
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

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("jibs=info".parse()?))
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Import(args) => run_import(args).await,
        Commands::Check(args) => run_check(args),
        Commands::Plan(args) => run_plan(args),
    }
}

async fn run_import(args: ImportArgs) -> Result<()> {
    use jibs_protocol::CompressionMode;

    let compression = if args.compress {
        CompressionMode::Zstd
    } else if args.no_compress {
        CompressionMode::None
    } else {
        CompressionMode::Auto
    };

    let config = ImportConfig {
        config_path: args.config,
        remote_host: args.host,
        local_mysql: args.local_mysql,
        vars: args.vars.into_iter().collect(),
        var_file: args.var_file,
        resume: args.resume,
        parallel: args.parallel,
        compression,
        identity_file: args.identity,
        ssh_port: args.port,
    };

    import::run_import(config).await
}

fn run_check(args: CheckArgs) -> Result<()> {
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

fn run_plan(args: PlanArgs) -> Result<()> {
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

    // Load variables from file if specified
    let mut vars: std::collections::HashMap<String, String> = args.vars.into_iter().collect();
    if let Some(var_file) = args.var_file {
        let content = std::fs::read_to_string(&var_file)?;
        let file_vars: std::collections::HashMap<String, serde_json::Value> =
            serde_json::from_str(&content)?;
        for (k, v) in file_vars {
            vars.entry(k).or_insert_with(|| match v {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            });
        }
    }

    // Resolve the plan
    let plan = resolver::resolve(&source, &program, &vars)?;

    // Print the plan as JSON
    println!("{}", serde_json::to_string_pretty(&plan)?);

    Ok(())
}
