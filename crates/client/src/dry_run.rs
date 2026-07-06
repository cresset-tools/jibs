//! Dry run: report what an import would do without touching the local
//! database

use std::collections::HashMap;

use anyhow::Result;
use tracing::info;

use jibs_protocol::{
    AggregateRootCount, ClientMessage, ExecutionPlan, MessageWriter, ServerMessage,
    TableDisposition, TableInfo,
};

use crate::import::ImportConfig;
use crate::protocol::{perform_handshake, recv_message, send_message};
use crate::ssh::SshSession;

pub(crate) async fn run_dry_run(
    config: &ImportConfig,
    session: &SshSession,
    server_path: &str,
    plan: ExecutionPlan,
) -> Result<()> {
    info!("Starting remote server (dry run): {}", server_path);
    let mut server = session.start_process(server_path).await?;

    perform_handshake(&mut server).await?;

    let mut encoder: MessageWriter<()> = MessageWriter::with_capacity(4096, ());
    send_message(
        &mut server,
        &mut encoder,
        &ClientMessage::Credentials { mysql_url: config.remote_mysql.clone() },
    )
    .await?;
    send_message(
        &mut server,
        &mut encoder,
        &ClientMessage::Init {
            plan: plan.clone(),
            compression: config.compression,
            parallel: 1,
            collect_metrics: false,
            dry_run: true,
        },
    )
    .await?;

    let tables = match recv_message(&mut server, config.max_message_size).await? {
        ServerMessage::Ready { tables, .. } => tables,
        ServerMessage::Error { message, .. } => {
            return Err(anyhow::anyhow!("Server error: {}", message))
        }
        other => return Err(anyhow::anyhow!("Unexpected message: {:?}", other)),
    };

    let (dispositions, root_counts) = match recv_message(&mut server, config.max_message_size)
        .await?
    {
        ServerMessage::DryRunReport { table_dispositions, root_counts } => {
            (table_dispositions, root_counts)
        }
        ServerMessage::Error { message, .. } => {
            return Err(anyhow::anyhow!("Server error: {}", message))
        }
        other => return Err(anyhow::anyhow!("Unexpected message: {:?}", other)),
    };

    print_dry_run_report(&plan, &tables, &dispositions, &root_counts);
    Ok(())
}

/// Print what an import would do, based on the server's dry-run report
fn print_dry_run_report(
    plan: &ExecutionPlan,
    tables: &[TableInfo],
    dispositions: &[(u16, TableDisposition)],
    root_counts: &[AggregateRootCount],
) {
    let by_id: HashMap<u16, &TableInfo> = tables.iter().map(|t| (t.table_id, t)).collect();

    println!();
    println!("DRY RUN — no changes were made to the local database");

    if !root_counts.is_empty() {
        println!();
        println!("Aggregates:");
        for count in root_counts {
            let agg = plan.aggregates.iter().find(|a| a.name == count.aggregate);
            let mut line = format!("  {}", count.aggregate);
            if let Some(agg) = agg {
                line.push_str(&format!(" (root {})", agg.root_table));
            }
            line.push_str(&format!(": {} matching root row(s)", count.matching_rows));
            if let Some(limit) = agg.and_then(|a| a.limit) {
                if limit >= 0 && (limit as u64) < count.matching_rows {
                    line.push_str(&format!(", limited to {}", limit));
                }
            }
            println!("{}", line);
            if let Some(where_clause) = agg.and_then(|a| a.where_clause.as_ref()) {
                println!("      where {}", where_clause);
            }
        }
    }

    let mut aggregate_tables: Vec<&TableInfo> = Vec::new();
    let mut full_tables: Vec<&TableInfo> = Vec::new();
    let mut excluded_tables: Vec<&TableInfo> = Vec::new();
    for (tid, disposition) in dispositions {
        let Some(info) = by_id.get(tid) else { continue };
        match disposition {
            TableDisposition::Aggregate => aggregate_tables.push(info),
            TableDisposition::Full | TableDisposition::Empty => full_tables.push(info),
            TableDisposition::Excluded => excluded_tables.push(info),
        }
    }
    aggregate_tables.sort_by(|a, b| a.name.cmp(&b.name));
    full_tables.sort_by(|a, b| a.name.cmp(&b.name));
    excluded_tables.sort_by(|a, b| a.name.cmp(&b.name));

    if !aggregate_tables.is_empty() {
        println!();
        println!(
            "Aggregate tables — rows selected by traversal ({}):",
            aggregate_tables.len()
        );
        for table in &aggregate_tables {
            println!("  {:<42} up to ~{} rows", table.name, table.estimated_rows);
        }
    }

    if !full_tables.is_empty() {
        let total: u64 = full_tables.iter().map(|t| t.estimated_rows).sum();
        println!();
        println!(
            "Full tables — imported completely ({}, ~{} rows):",
            full_tables.len(),
            total
        );
        for table in &full_tables {
            println!("  {:<42} ~{} rows", table.name, table.estimated_rows);
        }
    }

    if !excluded_tables.is_empty() {
        let names: Vec<&str> = excluded_tables.iter().map(|t| t.name.as_str()).collect();
        println!();
        println!(
            "Excluded — structure only, no data ({}):",
            excluded_tables.len()
        );
        println!("  {}", names.join(", "));
    }

    println!();
    println!(
        "Post-import: {} preserve rule(s), {} set block(s), {} after statement(s)",
        plan.preserves.len(),
        plan.sets.len(),
        plan.after_statements.len()
    );
    if !plan.anonymization.is_empty() {
        let mut summary: Vec<String> = plan
            .anonymization
            .iter()
            .map(|(table, rules)| format!("{} ({} column(s))", table, rules.len()))
            .collect();
        summary.sort();
        println!("Anonymization: {}", summary.join(", "));
    }
    println!();
    println!("Row counts are estimates. Run again without --dry-run to import.");
}

