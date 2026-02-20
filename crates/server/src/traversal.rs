//! Dependency graph traversal for collecting related rows

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::{mpsc, Arc};
use std::time::Instant;

use jibs_protocol::framing::write_message_noflush;
use mysql::{Row, Value as MySqlValue};

use jibs_protocol::{
    AnonymizeRule, ColumnDef, CompressionMode, ExecutionPlan, Relation, ResolvedAggregate,
    ServerMessage, ServerMetrics, SortDirection, TableDisposition,
};

use crate::error::{Result, ServerError};
use crate::metrics::MetricsCollector;
use crate::mysql::MySqlConnection;
use crate::tsv::TsvWriter;

/// Maximum rows per chunk
const CHUNK_ROW_LIMIT: usize = 10_000;
/// Maximum bytes per chunk
const CHUNK_BYTE_LIMIT: usize = 10 * 1024 * 1024; // 10MB
/// Maximum number of values in a single IN clause to stay under MySQL's max_allowed_packet
const MAX_IN_VALUES: usize = 10_000;

/// A table task for parallel streaming
struct TableTask {
    table: String,
    columns: Vec<ColumnDef>,
    anonymization: Vec<AnonymizeRule>,
}

// ============================================================================
// Streaming helpers
// ============================================================================

/// Send a TSV buffer as a Data message through a channel if non-empty.
fn send_chunk(
    table: &str,
    tsv_writer: &mut TsvWriter,
    chunk_row_count: u32,
    compression: CompressionMode,
    tx: &mpsc::SyncSender<ServerMessage>,
    metrics: &MetricsCollector,
) -> Result<()> {
    if tsv_writer.is_empty() {
        return Ok(());
    }

    let tsv_data = tsv_writer.take_buffer();
    let bytes = tsv_data.len() as u64;

    metrics.add_bytes_sent(bytes);
    metrics.add_rows_sent(chunk_row_count as u64);

    let compress_start = Instant::now();
    let compressed = maybe_compress(tsv_data, compression);
    metrics.add_compress_time(compress_start.elapsed());

    let compressed_len = compressed.len() as u64;
    let msg = ServerMessage::Data {
        table: table.to_string(),
        row_count: chunk_row_count,
        tsv_data: compressed,
    };

    tx.send(msg)
        .map_err(|_| ServerError::Protocol("Channel closed".to_string()))?;
    metrics.add_message(compressed_len);

    Ok(())
}

/// Extract a primary key from a single row given the PK column names
fn extract_pk_from_row(row: &Row, pk_columns: &[String]) -> Vec<MySqlValue> {
    pk_columns
        .iter()
        .map(|col| {
            row.get_opt::<MySqlValue, _>(col as &str)
                .and_then(|r| r.ok())
                .unwrap_or(MySqlValue::NULL)
        })
        .collect()
}

// ============================================================================
// Relation graph helpers
// ============================================================================

/// Get forward relations from a table, filtered by valid tables and exclusions
fn get_forward_relations<'a>(
    table: &str,
    relations_by_source: &'a HashMap<String, Vec<&'a Relation>>,
    all_tables: &HashSet<String>,
    exclude_tables: &HashSet<String>,
) -> Vec<&'a Relation> {
    relations_by_source
        .get(table)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|r| {
            all_tables.contains(&r.from_table)
                && all_tables.contains(&r.to_table)
                && !exclude_tables.contains(&r.to_table)
        })
        .collect()
}

/// Get backward relations to a table, filtered by valid tables and exclusions
fn get_backward_relations<'a>(
    table: &str,
    relations_by_target: &'a HashMap<String, Vec<&'a Relation>>,
    all_tables: &HashSet<String>,
    exclude_tables: &HashSet<String>,
) -> Vec<&'a Relation> {
    relations_by_target
        .get(table)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|r| {
            all_tables.contains(&r.from_table)
                && all_tables.contains(&r.to_table)
                && !exclude_tables.contains(&r.from_table)
        })
        .collect()
}

// ============================================================================
// Static graph analysis
// ============================================================================

/// Compute the set of tables that could potentially be touched by aggregate BFS.
///
/// Walks the relation graph statically (no MySQL queries) from each aggregate root,
/// following the same directional rules as the data BFS:
/// - From root/backward-reached tables: follow both forward and backward relations
/// - From forward-reached tables: follow forward relations only
///
/// Tables in `full_tables` and globally excluded tables are treated as dead ends
/// (matching data BFS behavior where they don't produce FK values for further traversal).
fn compute_potential_aggregate_tables(
    plan: &ExecutionPlan,
    all_tables: &HashSet<String>,
) -> HashSet<String> {
    let mut potential = HashSet::new();

    // Build relation indices
    let mut by_source: HashMap<&str, Vec<&Relation>> = HashMap::new();
    let mut by_target: HashMap<&str, Vec<&Relation>> = HashMap::new();
    for rel in &plan.relations {
        by_source.entry(&rel.from_table).or_default().push(rel);
        by_target.entry(&rel.to_table).or_default().push(rel);
    }

    for aggregate in &plan.aggregates {
        // Build exclude set for this aggregate (per-aggregate + global excluded as dead ends)
        let mut exclude: HashSet<String> = aggregate.exclude_tables.iter().cloned().collect();
        if !aggregate.exclude_patterns.is_empty() {
            let regexes: Vec<regex::Regex> = aggregate
                .exclude_patterns
                .iter()
                .filter_map(|p| regex::Regex::new(p).ok())
                .collect();
            for table in all_tables {
                if regexes.iter().any(|re| re.is_match(table)) {
                    exclude.insert(table.clone());
                }
            }
        }
        exclude.extend(plan.excluded_tables.iter().cloned());

        if aggregate.root_only {
            if all_tables.contains(&aggregate.root_table) {
                potential.insert(aggregate.root_table.clone());
            }
            continue;
        }

        // Static BFS from root: (table_name, can_go_backward)
        let mut queue: Vec<(String, bool)> = Vec::new();
        let mut visited: HashSet<String> = HashSet::new();

        if all_tables.contains(&aggregate.root_table) {
            queue.push((aggregate.root_table.clone(), true));
            visited.insert(aggregate.root_table.clone());
        }

        while let Some((table, can_go_backward)) = queue.pop() {
            potential.insert(table.clone());

            // Forward relations (from_table=table -> to_table)
            if let Some(rels) = by_source.get(table.as_str()) {
                for rel in rels {
                    if all_tables.contains(&rel.to_table)
                        && !plan.full_tables.contains(&rel.to_table)
                        && !exclude.contains(&rel.to_table)
                        && !visited.contains(&rel.to_table)
                    {
                        visited.insert(rel.to_table.clone());
                        queue.push((rel.to_table.clone(), false));
                    }
                }
            }

            // Backward relations (to_table=table -> from_table)
            if can_go_backward {
                if let Some(rels) = by_target.get(table.as_str()) {
                    for rel in rels {
                        if all_tables.contains(&rel.from_table)
                            && !plan.full_tables.contains(&rel.from_table)
                            && !exclude.contains(&rel.from_table)
                            && !visited.contains(&rel.from_table)
                        {
                            visited.insert(rel.from_table.clone());
                            queue.push((rel.from_table.clone(), true));
                        }
                    }
                }
            }
        }
    }

    potential
}

// ============================================================================
// Aggregate BFS (runs in its own thread with its own MySQL connection)
// ============================================================================

/// Run aggregate BFS traversal, sending data through a channel.
///
/// Opens its own MySQL connection, builds relation indices, pre-caches schemas,
/// then runs the full BFS. After BFS completes, streams any "false positive"
/// tables (potential-aggregate that BFS didn't actually touch) as full tables.
///
/// Returns the set of tables that were actually touched by aggregate BFS.
fn run_aggregate_bfs(
    mysql_url: &str,
    plan: ExecutionPlan,
    all_table_names_vec: Vec<String>,
    potential_aggregate_tables: HashSet<String>,
    compression: CompressionMode,
    tx: mpsc::SyncSender<ServerMessage>,
    metrics: Arc<MetricsCollector>,
) -> Result<HashSet<String>> {
    let mut conn = MySqlConnection::connect(mysql_url)?;

    // Build relation indices
    let mut relations_by_source: HashMap<String, Vec<&Relation>> = HashMap::new();
    let mut relations_by_target: HashMap<String, Vec<&Relation>> = HashMap::new();
    for relation in &plan.relations {
        relations_by_source
            .entry(relation.from_table.clone())
            .or_default()
            .push(relation);
        relations_by_target
            .entry(relation.to_table.clone())
            .or_default()
            .push(relation);
    }

    // Pre-cache schemas for all tables
    let all_table_names: HashSet<String> = all_table_names_vec.iter().cloned().collect();
    for table_name in &all_table_names_vec {
        conn.get_column_defs(table_name)?;
    }

    // Track visited rows per table (only PKs, not full rows)
    let mut visited: HashMap<String, HashSet<Vec<u8>>> = HashMap::new();

    // Tables actually touched by aggregates
    let mut aggregate_tables: HashSet<String> = HashSet::new();

    let full_tables = &plan.full_tables;

    // Track which tables have had Schema sent and their total row counts
    let mut schemas_sent: HashSet<String> = HashSet::new();
    let mut table_row_counts: HashMap<String, u64> = HashMap::new();

    for aggregate in &plan.aggregates {
        aggregate_tables.insert(aggregate.root_table.clone());

        // Tables excluded from BFS for this aggregate (expand regex patterns)
        let mut exclude_tables: HashSet<String> =
            aggregate.exclude_tables.iter().cloned().collect();
        if !aggregate.exclude_patterns.is_empty() {
            let regexes: Vec<regex::Regex> = aggregate
                .exclude_patterns
                .iter()
                .filter_map(|p| regex::Regex::new(p).ok())
                .collect();
            for table_name in &all_table_names_vec {
                if regexes.iter().any(|re| re.is_match(table_name)) {
                    exclude_tables.insert(table_name.clone());
                }
            }
        }

        // BFS pending set: coalesces FK values by (table, column, via_backward)
        let mut pending: HashMap<(String, String, bool), Vec<MySqlValue>> = HashMap::new();

        // Stream root table
        let root_table = &aggregate.root_table;
        let root_query = build_root_query(aggregate);
        let pk_columns = conn
            .get_cached_primary_key(root_table)
            .cloned()
            .unwrap_or_default();
        let columns = conn.get_column_defs(root_table)?;
        let anonymization = plan
            .anonymization
            .get(root_table.as_str())
            .cloned()
            .unwrap_or_default();

        // Prepare FK extraction info for the root table (pre-filtered)
        let forward_relations = if aggregate.root_only {
            vec![]
        } else {
            get_forward_relations(
                root_table,
                &relations_by_source,
                &all_table_names,
                &exclude_tables,
            )
        };
        let backward_relations = if aggregate.root_only {
            vec![]
        } else {
            get_backward_relations(
                root_table,
                &relations_by_target,
                &all_table_names,
                &exclude_tables,
            )
        };

        let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let mut tsv_writer =
            TsvWriter::new(column_names, anonymization, plan.fakers.clone());
        let mut chunk_row_count: u32 = 0;

        // Accumulators for FK values
        let mut backward_fk_vecs: Vec<Vec<MySqlValue>> =
            vec![Vec::new(); backward_relations.len()];
        let mut forward_fk_vecs: Vec<Vec<MySqlValue>> =
            vec![Vec::new(); forward_relations.len()];

        let visited_set = visited.entry(root_table.clone()).or_default();

        // Stream root table rows
        let root_query_ms: u64;
        let mut root_iterate_ns: u64 = 0;
        let mut root_row_count: u64 = 0;
        {
            let query_start = Instant::now();
            let result = conn.query_iter(&root_query)?;
            let root_query_elapsed = query_start.elapsed();
            metrics.add_query_time(root_query_elapsed);
            root_query_ms = root_query_elapsed.as_millis() as u64;

            let mut result_iter = result.into_iter();
            loop {
                let iterate_start = Instant::now();
                let row: Row = match result_iter.next() {
                    Some(Ok(row)) => row,
                    Some(Err(e)) => return Err(e.into()),
                    None => break,
                };
                let iter_elapsed = iterate_start.elapsed();
                metrics.add_iterate_time(iter_elapsed);
                root_iterate_ns += iter_elapsed.as_nanos() as u64;
                root_row_count += 1;

                // Send schema on first row
                if schemas_sent.insert(root_table.clone()) {
                    tx.send(ServerMessage::Schema {
                        table: root_table.clone(),
                        columns: columns.clone(),
                    })
                    .map_err(|_| ServerError::Protocol("Channel closed".to_string()))?;
                }

                // Dedup check + FK extraction
                let dedup_start = Instant::now();
                let pk = extract_pk_from_row(&row, &pk_columns);
                let pk_bytes = serialize_pk(&pk);
                let is_new = visited_set.insert(pk_bytes);

                // Always extract backward FK values from root table rows,
                // even for already-visited rows.
                for (i, relation) in backward_relations.iter().enumerate() {
                    if let Some(Ok(v)) =
                        row.get_opt::<MySqlValue, _>(relation.to_column.as_str())
                    {
                        if !matches!(v, MySqlValue::NULL) {
                            backward_fk_vecs[i].push(v);
                        }
                    }
                }

                if !is_new {
                    metrics.add_dedup_time(dedup_start.elapsed());
                    continue;
                }

                // Extract FK values for forward relations
                for (i, relation) in forward_relations.iter().enumerate() {
                    if let Some(Ok(v)) =
                        row.get_opt::<MySqlValue, _>(relation.from_column.as_str())
                    {
                        if !matches!(v, MySqlValue::NULL) {
                            forward_fk_vecs[i].push(v);
                        }
                    }
                }
                metrics.add_dedup_time(dedup_start.elapsed());

                // Write TSV
                let serialize_start = Instant::now();
                tsv_writer.write_row(&row)?;
                metrics.add_serialize_time(serialize_start.elapsed());

                chunk_row_count += 1;
                *table_row_counts.entry(root_table.clone()).or_default() += 1;

                if chunk_row_count >= CHUNK_ROW_LIMIT as u32
                    || tsv_writer.buffer_size() >= CHUNK_BYTE_LIMIT
                {
                    send_chunk(
                        root_table,
                        &mut tsv_writer,
                        chunk_row_count,
                        compression,
                        &tx,
                        &metrics,
                    )?;
                    chunk_row_count = 0;
                }
            }
        }

        // Record per-query timing for root query
        metrics.record_query(jibs_protocol::QueryTiming {
            table: root_table.clone(),
            column: String::new(),
            num_values: 0,
            query_ms: root_query_ms,
            iterate_ms: root_iterate_ns / 1_000_000,
            rows: root_row_count,
        });

        // Flush remaining root table data
        if chunk_row_count > 0 {
            send_chunk(
                root_table,
                &mut tsv_writer,
                chunk_row_count,
                compression,
                &tx,
                &metrics,
            )?;
        }

        // Seed pending set from root table FK vecs
        for (i, relation) in backward_relations.iter().enumerate() {
            let values = std::mem::take(&mut backward_fk_vecs[i]);
            if !values.is_empty() {
                let dedup_start = Instant::now();
                let unique = dedupe_values(values);
                metrics.add_interlevel_dedup_time(dedup_start.elapsed());
                if !unique.is_empty() {
                    pending
                        .entry((
                            relation.from_table.clone(),
                            relation.from_column.clone(),
                            true,
                        ))
                        .or_default()
                        .extend(unique);
                }
            }
        }
        for (i, relation) in forward_relations.iter().enumerate() {
            let values = std::mem::take(&mut forward_fk_vecs[i]);
            if !values.is_empty() {
                let dedup_start = Instant::now();
                let unique = dedupe_values(values);
                metrics.add_interlevel_dedup_time(dedup_start.elapsed());
                if !unique.is_empty() {
                    pending
                        .entry((
                            relation.to_table.clone(),
                            relation.to_column.clone(),
                            false,
                        ))
                        .or_default()
                        .extend(unique);
                }
            }
        }

        // BFS traversal — level by level
        while !pending.is_empty() {
            let current_level: Vec<_> = pending.drain().collect();

            for ((table, column, reached_via_backward), fk_values) in current_level {
                // Skip full_tables — they will be imported in full
                if full_tables.contains(&table) {
                    continue;
                }

                // Skip tables excluded from this aggregate's BFS
                if exclude_tables.contains(&table) {
                    continue;
                }

                aggregate_tables.insert(table.clone());

                if plan.excluded_tables.contains(&table) {
                    continue;
                }

                // Get cached metadata
                let pk_columns = conn
                    .get_cached_primary_key(&table)
                    .cloned()
                    .unwrap_or_default();
                let columns = conn.get_column_defs(&table)?;
                let anonymization = plan
                    .anonymization
                    .get(&table)
                    .cloned()
                    .unwrap_or_default();

                let forward_relations = get_forward_relations(
                    &table,
                    &relations_by_source,
                    &all_table_names,
                    &exclude_tables,
                );
                let backward_relations = if reached_via_backward {
                    get_backward_relations(
                        &table,
                        &relations_by_target,
                        &all_table_names,
                        &exclude_tables,
                    )
                } else {
                    vec![]
                };

                let column_names: Vec<String> =
                    columns.iter().map(|c| c.name.clone()).collect();
                let mut tsv_writer = TsvWriter::new(
                    column_names,
                    anonymization,
                    plan.fakers.clone(),
                );
                let mut chunk_row_count: u32 = 0;

                let mut fwd_fk_vecs: Vec<Vec<MySqlValue>> =
                    vec![Vec::new(); forward_relations.len()];
                let mut bwd_fk_vecs: Vec<Vec<MySqlValue>> =
                    vec![Vec::new(); backward_relations.len()];

                let visited_set = visited.entry(table.clone()).or_default();
                let row_count_entry = table_row_counts.entry(table.clone()).or_default();

                let dedup_start = Instant::now();
                let unique_values = dedupe_values(fk_values);
                metrics.add_interlevel_dedup_time(dedup_start.elapsed());

                for batch in unique_values.chunks(MAX_IN_VALUES) {
                    let batch_len = batch.len() as u32;
                    let batch_query_ms: u64;
                    let mut batch_iterate_ns: u64 = 0;
                    let mut batch_row_count: u64 = 0;
                    {
                        let placeholders: Vec<&str> =
                            (0..batch.len()).map(|_| "?").collect();
                        let query = format!(
                            "SELECT * FROM `{}` WHERE `{}` IN ({})",
                            table,
                            column,
                            placeholders.join(", ")
                        );
                        let params: Vec<MySqlValue> = batch.to_vec();

                        let query_start = Instant::now();
                        let result = conn.exec_iter(
                            &query,
                            mysql::Params::Positional(params),
                        )?;
                        let batch_query_elapsed = query_start.elapsed();
                        metrics.add_query_time(batch_query_elapsed);
                        batch_query_ms = batch_query_elapsed.as_millis() as u64;

                        let mut result_iter = result.into_iter();
                        loop {
                            let iterate_start = Instant::now();
                            let row: Row = match result_iter.next() {
                                Some(Ok(row)) => row,
                                Some(Err(e)) => return Err(e.into()),
                                None => break,
                            };
                            let iter_elapsed = iterate_start.elapsed();
                            metrics.add_iterate_time(iter_elapsed);
                            batch_iterate_ns += iter_elapsed.as_nanos() as u64;
                            batch_row_count += 1;

                            // Dedup check + FK extraction
                            let dedup_start = Instant::now();
                            let pk = extract_pk_from_row(&row, &pk_columns);
                            let pk_bytes = serialize_pk(&pk);
                            if !visited_set.insert(pk_bytes) {
                                metrics.add_dedup_time(dedup_start.elapsed());
                                continue;
                            }

                            for (i, relation) in forward_relations.iter().enumerate()
                            {
                                if let Some(Ok(v)) = row.get_opt::<MySqlValue, _>(
                                    relation.from_column.as_str(),
                                ) {
                                    if !matches!(v, MySqlValue::NULL) {
                                        fwd_fk_vecs[i].push(v);
                                    }
                                }
                            }

                            for (i, relation) in
                                backward_relations.iter().enumerate()
                            {
                                if let Some(Ok(v)) = row.get_opt::<MySqlValue, _>(
                                    relation.to_column.as_str(),
                                ) {
                                    if !matches!(v, MySqlValue::NULL) {
                                        bwd_fk_vecs[i].push(v);
                                    }
                                }
                            }
                            metrics.add_dedup_time(dedup_start.elapsed());

                            // Send schema on first new row for this table
                            if schemas_sent.insert(table.clone()) {
                                tx.send(ServerMessage::Schema {
                                    table: table.clone(),
                                    columns: columns.clone(),
                                })
                                .map_err(|_| {
                                    ServerError::Protocol("Channel closed".to_string())
                                })?;
                            }

                            let serialize_start = Instant::now();
                            tsv_writer.write_row(&row)?;
                            metrics.add_serialize_time(serialize_start.elapsed());

                            chunk_row_count += 1;
                            *row_count_entry += 1;

                            if chunk_row_count >= CHUNK_ROW_LIMIT as u32
                                || tsv_writer.buffer_size() >= CHUNK_BYTE_LIMIT
                            {
                                send_chunk(
                                    &table,
                                    &mut tsv_writer,
                                    chunk_row_count,
                                    compression,
                                    &tx,
                                    &metrics,
                                )?;
                                chunk_row_count = 0;
                            }
                        }
                    }

                    metrics.record_query(jibs_protocol::QueryTiming {
                        table: table.clone(),
                        column: column.clone(),
                        num_values: batch_len,
                        query_ms: batch_query_ms,
                        iterate_ms: batch_iterate_ns / 1_000_000,
                        rows: batch_row_count,
                    });
                }

                if chunk_row_count > 0 {
                    send_chunk(
                        &table,
                        &mut tsv_writer,
                        chunk_row_count,
                        compression,
                        &tx,
                        &metrics,
                    )?;
                }

                // Coalesce FK values into next level's pending set
                for (i, relation) in forward_relations.iter().enumerate() {
                    let values = std::mem::take(&mut fwd_fk_vecs[i]);
                    if !values.is_empty() {
                        let dedup_start = Instant::now();
                        let unique = dedupe_values(values);
                        metrics.add_interlevel_dedup_time(dedup_start.elapsed());
                        if !unique.is_empty() {
                            pending
                                .entry((
                                    relation.to_table.clone(),
                                    relation.to_column.clone(),
                                    false,
                                ))
                                .or_default()
                                .extend(unique);
                        }
                    }
                }
                for (i, relation) in backward_relations.iter().enumerate() {
                    let values = std::mem::take(&mut bwd_fk_vecs[i]);
                    if !values.is_empty() {
                        let dedup_start = Instant::now();
                        let unique = dedupe_values(values);
                        metrics.add_interlevel_dedup_time(dedup_start.elapsed());
                        if !unique.is_empty() {
                            pending
                                .entry((
                                    relation.from_table.clone(),
                                    relation.from_column.clone(),
                                    true,
                                ))
                                .or_default()
                                .extend(unique);
                        }
                    }
                }
            }
        }
    }

    // Send TableDone for all aggregate tables that had rows
    for (table, row_count) in &table_row_counts {
        tx.send(ServerMessage::TableDone {
            table: table.clone(),
            row_count: *row_count,
            metrics: None,
        })
        .map_err(|_| ServerError::Protocol("Channel closed".to_string()))?;
    }

    // Snapshot aggregate phase metrics
    metrics.snapshot_aggregate_phase();

    // Stream false-positive tables (potential aggregate that BFS didn't touch)
    for table in &all_table_names_vec {
        if potential_aggregate_tables.contains(table)
            && !aggregate_tables.contains(table)
            && !plan.excluded_tables.contains(table)
            && !plan.ignored_tables.contains(table)
        {
            let columns = conn.get_column_defs(table)?;
            let anonymization = plan
                .anonymization
                .get(table)
                .cloned()
                .unwrap_or_default();

            stream_full_table_to_channel(
                &mut conn,
                table,
                &columns,
                anonymization,
                compression,
                &plan.fakers,
                &tx,
                &metrics,
            )?;
        }
    }

    Ok(aggregate_tables)
}

// ============================================================================
// Dependency Traverser
// ============================================================================

/// Dependency graph traverser
pub struct DependencyTraverser<'a> {
    conn: &'a mut MySqlConnection,
    plan: &'a ExecutionPlan,
    /// Metrics collector for timing
    metrics: Arc<MetricsCollector>,
}

impl<'a> DependencyTraverser<'a> {
    /// Create a new traverser
    pub fn new(
        conn: &'a mut MySqlConnection,
        plan: &'a ExecutionPlan,
        collect_metrics: bool,
    ) -> Result<Self> {
        let metrics = if collect_metrics {
            Arc::new(MetricsCollector::enabled())
        } else {
            Arc::new(MetricsCollector::disabled())
        };

        Ok(Self {
            conn,
            plan,
            metrics,
        })
    }

    /// Get the collected metrics (if enabled)
    pub fn get_metrics(&self) -> Option<ServerMetrics> {
        self.metrics.to_server_metrics()
    }

    /// Stream all tables concurrently: aggregate BFS and full-table workers run in parallel,
    /// sending through a shared channel. The main thread drains the channel and writes to stdout.
    pub fn stream_all_tables<W: Write>(
        &mut self,
        compression: CompressionMode,
        writer: &mut W,
        parallel: u32,
        mysql_url: &str,
        interrupt: &std::sync::atomic::AtomicBool,
    ) -> Result<Vec<(String, TableDisposition)>> {
        let overall_start = Instant::now();

        // 1. Pre-cache schemas on self.conn, build table list
        let schema_start = Instant::now();
        let all_table_names_vec = self.conn.get_all_table_names()?;
        let all_table_names: HashSet<String> = all_table_names_vec.iter().cloned().collect();
        for table_name in &all_table_names_vec {
            self.conn.get_column_defs(table_name)?;
        }
        self.metrics.set_schema_cache_time(schema_start.elapsed());

        // 2. Compute potential aggregate tables (static graph walk, no MySQL queries)
        let potential_aggregate =
            compute_potential_aggregate_tables(self.plan, &all_table_names);

        // 3. Create shared channel
        let num_workers = (parallel as usize).max(1);
        let (tx, rx) = mpsc::sync_channel::<ServerMessage>(num_workers * 4);

        // Classify tables into excluded, definite-full, and potential-aggregate.
        // Write excluded tables directly to the writer (not through channel)
        // to avoid deadlocking on the bounded channel before the writer loop starts.
        let mut excluded_set: HashSet<String> = HashSet::new();
        let mut full_table_tasks: Vec<TableTask> = Vec::new();

        for table in &all_table_names_vec {
            if self.plan.ignored_tables.contains(table) {
                continue;
            }
            if self.plan.excluded_tables.contains(table) {
                let columns = self.conn.get_column_defs(table)?;
                let write_start = Instant::now();
                write_message_noflush(
                    writer,
                    &ServerMessage::Schema {
                        table: table.clone(),
                        columns,
                    },
                )?;
                write_message_noflush(
                    writer,
                    &ServerMessage::TableDone {
                        table: table.clone(),
                        row_count: 0,
                        metrics: None,
                    },
                )?;
                self.metrics.add_write_time(write_start.elapsed());
                excluded_set.insert(table.clone());
                continue;
            }
            if potential_aggregate.contains(table) {
                // Handled by BFS thread (either as aggregate or false-positive)
                continue;
            }

            // Definite full table
            let columns = self.conn.get_column_defs(table)?;
            let anonymization = self
                .plan
                .anonymization
                .get(table)
                .cloned()
                .unwrap_or_default();
            full_table_tasks.push(TableTask {
                table: table.clone(),
                columns,
                anonymization,
            });
        }

        // 5. Spawn full-table workers
        let num_ft_workers = num_workers.min(full_table_tasks.len().max(1));

        let mut worker_tasks: Vec<Vec<TableTask>> =
            (0..num_ft_workers).map(|_| Vec::new()).collect();
        for (i, task) in full_table_tasks.into_iter().enumerate() {
            worker_tasks[i % num_ft_workers].push(task);
        }

        let mut handles: Vec<std::thread::JoinHandle<Result<()>>> = Vec::new();

        for tasks in worker_tasks {
            if tasks.is_empty() {
                continue;
            }
            let tx = tx.clone();
            let fakers = self.plan.fakers.clone();
            let mysql_url = mysql_url.to_string();
            let metrics = Arc::clone(&self.metrics);

            let handle = std::thread::spawn(move || -> Result<()> {
                let mut conn = MySqlConnection::connect(&mysql_url)?;
                for task in tasks {
                    let result = stream_full_table_to_channel(
                        &mut conn,
                        &task.table,
                        &task.columns,
                        task.anonymization,
                        compression,
                        &fakers,
                        &tx,
                        &metrics,
                    );
                    if let Err(e) = result {
                        let _ = tx.send(ServerMessage::Error {
                            message: e.to_string(),
                            recoverable: e.is_recoverable(),
                        });
                        return Err(e);
                    }
                }
                Ok(())
            });
            handles.push(handle);
        }

        // 6. Spawn BFS thread (if there are aggregates)
        let has_bfs_work = !self.plan.aggregates.is_empty();
        let bfs_handle = if has_bfs_work {
            let bfs_tx = tx.clone();
            let plan = self.plan.clone();
            let all_names = all_table_names_vec.clone();
            let pot_agg = potential_aggregate.clone();
            let mysql_url = mysql_url.to_string();
            let metrics = Arc::clone(&self.metrics);

            Some(std::thread::spawn(move || -> Result<HashSet<String>> {
                run_aggregate_bfs(
                    &mysql_url,
                    plan,
                    all_names,
                    pot_agg,
                    compression,
                    bfs_tx,
                    metrics,
                )
            }))
        } else {
            // No aggregates: potential-aggregate tables that aren't excluded/ignored
            // need to be streamed as full tables. But with no aggregates,
            // potential_aggregate is empty, so nothing to do.
            None
        };

        // 7. Drop main thread's tx clone so channel closes when all producers finish
        drop(tx);

        // 8. Main thread becomes writer loop
        let mut interrupted = false;
        let mut streamed_tables: HashSet<String> = HashSet::new();
        loop {
            // Check for client interrupt between messages
            if interrupt.load(std::sync::atomic::Ordering::Relaxed) {
                interrupted = true;
                break;
            }
            // Use recv_timeout so we can periodically re-check the interrupt flag
            // even when worker threads are busy (e.g. running long MySQL queries)
            let msg = match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(msg) => msg,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            };
            // For TableDone messages, attach a metrics snapshot so the client
            // has server metrics even if the import is interrupted
            let msg = if let ServerMessage::TableDone { table, row_count, .. } = msg {
                if row_count > 0 {
                    streamed_tables.insert(table.clone());
                }
                ServerMessage::TableDone {
                    table,
                    row_count,
                    metrics: self.metrics.snapshot(),
                }
            } else {
                msg
            };
            let write_start = Instant::now();
            write_message_noflush(writer, &msg)?;
            self.metrics.add_write_time(write_start.elapsed());
        }
        writer.flush()?;

        if interrupted {
            return Err(ServerError::Protocol("Interrupted by client".to_string()));
        }

        // 9. Join all threads and propagate first error
        let mut first_error: Option<ServerError> = None;

        // Join BFS thread and get actual aggregate tables
        let actual_aggregate_tables = if let Some(handle) = bfs_handle {
            match handle.join() {
                Ok(Ok(tables)) => tables,
                Ok(Err(e)) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                    HashSet::new()
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error =
                            Some(ServerError::Protocol("BFS thread panicked".to_string()));
                    }
                    HashSet::new()
                }
            }
        } else {
            HashSet::new()
        };

        // Join full-table workers
        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error =
                            Some(ServerError::Protocol("Worker thread panicked".to_string()));
                    }
                }
            }
        }

        if let Some(e) = first_error {
            return Err(e);
        }

        // 10. Assemble dispositions
        let mut dispositions: Vec<(String, TableDisposition)> = Vec::new();
        for table in &all_table_names_vec {
            if self.plan.ignored_tables.contains(table) {
                continue;
            }
            if excluded_set.contains(table) {
                dispositions.push((table.clone(), TableDisposition::Excluded));
            } else if actual_aggregate_tables.contains(table) {
                dispositions.push((table.clone(), TableDisposition::Aggregate));
            } else if streamed_tables.contains(table) {
                dispositions.push((table.clone(), TableDisposition::Full));
            } else {
                dispositions.push((table.clone(), TableDisposition::Empty));
            }
        }

        self.metrics
            .set_aggregate_wall_time(overall_start.elapsed());
        self.metrics
            .set_full_tables_wall_time(overall_start.elapsed());

        dispositions.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(dispositions)
    }
}

// ============================================================================
// Query building
// ============================================================================

/// Build the root table query string with WHERE, ORDER BY, and LIMIT
fn build_root_query(aggregate: &ResolvedAggregate) -> String {
    let mut query = format!("SELECT * FROM `{}`", aggregate.root_table);

    if let Some(where_clause) = &aggregate.where_clause {
        query.push_str(" WHERE ");
        query.push_str(where_clause);
    }

    if let Some(order_by) = &aggregate.order_by {
        query.push_str(" ORDER BY `");
        query.push_str(order_by);
        query.push('`');
        if let Some(dir) = &aggregate.order_direction {
            match dir {
                SortDirection::Asc => query.push_str(" ASC"),
                SortDirection::Desc => query.push_str(" DESC"),
            }
        }
    }

    if let Some(limit) = aggregate.limit {
        query.push_str(&format!(" LIMIT {}", limit));
    }

    query
}

// ============================================================================
// Utility functions
// ============================================================================

/// Deduplicate MySQL values
fn dedupe_values(values: Vec<MySqlValue>) -> Vec<MySqlValue> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|v| {
            let bytes = serialize_pk(&[v.clone()]);
            seen.insert(bytes)
        })
        .collect()
}

/// Normalize a MySQL value to a canonical form for comparison
fn normalize_value(value: &MySqlValue) -> MySqlValue {
    match value {
        MySqlValue::Bytes(b) => {
            if let Ok(s) = std::str::from_utf8(b) {
                if let Ok(i) = s.parse::<i64>() {
                    return MySqlValue::Int(i);
                }
                if let Ok(u) = s.parse::<u64>() {
                    return MySqlValue::UInt(u);
                }
            }
            value.clone()
        }
        MySqlValue::UInt(u) if *u <= i64::MAX as u64 => MySqlValue::Int(*u as i64),
        _ => value.clone(),
    }
}

/// Serialize a primary key to bytes for deduplication
fn serialize_pk(pk: &[MySqlValue]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in pk {
        let normalized = normalize_value(value);
        match normalized {
            MySqlValue::NULL => bytes.push(0),
            MySqlValue::Int(i) => {
                bytes.push(1);
                bytes.extend_from_slice(&i.to_le_bytes());
            }
            MySqlValue::UInt(u) => {
                bytes.push(2);
                bytes.extend_from_slice(&u.to_le_bytes());
            }
            MySqlValue::Bytes(ref b) => {
                bytes.push(3);
                bytes.extend_from_slice(&(b.len() as u32).to_le_bytes());
                bytes.extend_from_slice(b);
            }
            MySqlValue::Float(f) => {
                bytes.push(4);
                bytes.extend_from_slice(&f.to_le_bytes());
            }
            MySqlValue::Double(d) => {
                bytes.push(5);
                bytes.extend_from_slice(&d.to_le_bytes());
            }
            MySqlValue::Date(y, m, d, h, mi, s, us) => {
                bytes.push(6);
                bytes.extend_from_slice(&y.to_le_bytes());
                bytes.push(m);
                bytes.push(d);
                bytes.push(h);
                bytes.push(mi);
                bytes.push(s);
                bytes.extend_from_slice(&us.to_le_bytes());
            }
            MySqlValue::Time(neg, d, h, m, s, us) => {
                bytes.push(7);
                bytes.push(if neg { 1 } else { 0 });
                bytes.extend_from_slice(&d.to_le_bytes());
                bytes.push(h);
                bytes.push(m);
                bytes.push(s);
                bytes.extend_from_slice(&us.to_le_bytes());
            }
        }
    }
    bytes
}

/// Optionally compress data based on compression mode
fn maybe_compress(data: Vec<u8>, mode: CompressionMode) -> Vec<u8> {
    match mode {
        CompressionMode::None | CompressionMode::Auto => data,
        CompressionMode::Zstd => {
            match zstd::encode_all(data.as_slice(), 3) {
                Ok(compressed) => {
                    let mut result = Vec::with_capacity(4 + compressed.len());
                    result.extend_from_slice(&(data.len() as u32).to_le_bytes());
                    result.extend(compressed);
                    result
                }
                Err(_) => data,
            }
        }
    }
}

/// Stream a full table using its own MySQL connection, sending messages through a channel.
/// Used by worker threads in parallel mode.
fn stream_full_table_to_channel(
    conn: &mut MySqlConnection,
    table: &str,
    columns: &[ColumnDef],
    anonymization: Vec<AnonymizeRule>,
    compression: CompressionMode,
    fakers: &HashMap<String, Vec<String>>,
    tx: &mpsc::SyncSender<ServerMessage>,
    metrics: &Arc<MetricsCollector>,
) -> Result<()> {
    let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut tsv_writer = TsvWriter::new(column_names, anonymization, fakers.clone());
    let mut total_rows: u64 = 0;
    let mut chunk_row_count: u32 = 0;

    // Always send schema so the table is created locally even if empty
    tx.send(ServerMessage::Schema {
        table: table.to_string(),
        columns: columns.to_vec(),
    })
    .map_err(|_| crate::error::ServerError::Protocol("Channel closed".to_string()))?;

    let query = format!("SELECT * FROM `{}`", table);

    // Time query execution
    let query_start = Instant::now();
    let result = conn.query_iter(&query)?;
    metrics.add_query_time(query_start.elapsed());

    let mut result_iter = result.into_iter();
    loop {
        // Time row fetch + parse (the actual MySQL read from TCP)
        let iterate_start = Instant::now();
        let row: Row = match result_iter.next() {
            Some(Ok(row)) => row,
            Some(Err(e)) => return Err(e.into()),
            None => break,
        };
        metrics.add_iterate_time(iterate_start.elapsed());

        // Time serialization
        let serialize_start = Instant::now();
        tsv_writer.write_row(&row)?;
        metrics.add_serialize_time(serialize_start.elapsed());

        chunk_row_count += 1;
        total_rows += 1;

        if chunk_row_count >= CHUNK_ROW_LIMIT as u32
            || tsv_writer.buffer_size() >= CHUNK_BYTE_LIMIT
        {
            // Flush chunk through channel
            let tsv_data = tsv_writer.take_buffer();
            let bytes = tsv_data.len() as u64;

            metrics.add_bytes_sent(bytes);
            metrics.add_rows_sent(chunk_row_count as u64);

            let compress_start = Instant::now();
            let compressed = maybe_compress(tsv_data, compression);
            metrics.add_compress_time(compress_start.elapsed());
            let compressed_len = compressed.len() as u64;
            let msg = ServerMessage::Data {
                table: table.to_string(),
                row_count: chunk_row_count,
                tsv_data: compressed,
            };
            tx.send(msg)
                .map_err(|_| crate::error::ServerError::Protocol("Channel closed".to_string()))?;
            metrics.add_message(compressed_len);
            chunk_row_count = 0;
        }
    }

    // Flush remaining data
    if chunk_row_count > 0 {
        let tsv_data = tsv_writer.take_buffer();
        let bytes = tsv_data.len() as u64;

        metrics.add_bytes_sent(bytes);
        metrics.add_rows_sent(chunk_row_count as u64);

        let compress_start = Instant::now();
        let compressed = maybe_compress(tsv_data, compression);
        metrics.add_compress_time(compress_start.elapsed());
        let compressed_len = compressed.len() as u64;
        let msg = ServerMessage::Data {
            table: table.to_string(),
            row_count: chunk_row_count,
            tsv_data: compressed,
        };
        tx.send(msg)
            .map_err(|_| crate::error::ServerError::Protocol("Channel closed".to_string()))?;
        metrics.add_message(compressed_len);
    }

    // Send TableDone (always, even for empty tables so the client marks them complete)
    tx.send(ServerMessage::TableDone {
        table: table.to_string(),
        row_count: total_rows,
        metrics: None,
    })
    .map_err(|_| crate::error::ServerError::Protocol("Channel closed".to_string()))?;

    Ok(())
}
