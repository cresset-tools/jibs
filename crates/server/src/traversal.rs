//! Dependency graph traversal for collecting related rows

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::{mpsc, Arc};
use std::time::Instant;

use mysql::{Row, Value as MySqlValue};

use jibs_protocol::{
    framing::write_message, AnonymizeRule, ColumnDef, CompressionMode, ExecutionPlan, Relation,
    ResolvedAggregate, ServerMessage, ServerMetrics, SortDirection, TableDisposition,
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

/// Flush a TSV buffer as a Data message if non-empty.
fn flush_chunk<W: Write>(
    table: &str,
    tsv_writer: &mut TsvWriter,
    chunk_row_count: u32,
    compression: CompressionMode,
    writer: &mut W,
    metrics: &MetricsCollector,
) -> Result<()> {
    if tsv_writer.is_empty() {
        return Ok(());
    }

    let tsv_data = tsv_writer.take_buffer();
    let bytes = tsv_data.len() as u64;

    // Track bytes sent for metrics
    metrics.add_bytes_sent(bytes);
    metrics.add_rows_sent(chunk_row_count as u64);

    let compress_start = Instant::now();
    let compressed = maybe_compress(tsv_data, compression);
    metrics.add_compress_time(compress_start.elapsed());

    let msg = ServerMessage::Data {
        table: table.to_string(),
        row_count: chunk_row_count,
        tsv_data: compressed,
    };

    // Time the write operation (includes flush)
    let write_start = Instant::now();
    write_message(writer, &msg)?;
    metrics.add_write_time(write_start.elapsed());

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
// Dependency Traverser
// ============================================================================

/// Dependency graph traverser
pub struct DependencyTraverser<'a> {
    conn: &'a mut MySqlConnection,
    plan: &'a ExecutionPlan,
    /// Relations indexed by source table
    relations_by_source: HashMap<String, Vec<&'a Relation>>,
    /// Relations indexed by target table
    relations_by_target: HashMap<String, Vec<&'a Relation>>,
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

        let metrics = if collect_metrics {
            Arc::new(MetricsCollector::enabled())
        } else {
            Arc::new(MetricsCollector::disabled())
        };

        Ok(Self {
            conn,
            plan,
            relations_by_source,
            relations_by_target,
            metrics,
        })
    }

    /// Get the collected metrics (if enabled)
    pub fn get_metrics(&self) -> Option<ServerMetrics> {
        self.metrics.to_server_metrics()
    }

    /// Stream all tables, processing one table at a time to avoid loading all rows into memory.
    ///
    /// Logic:
    /// 1. Tables touched by aggregates: only include rows reachable from aggregate (streamed during BFS)
    /// 2. Tables NOT touched by aggregates: include ALL rows (streamed directly from query)
    /// 3. Excluded tables: skip data (structure only)
    /// 4. Ignored tables: skip entirely
    ///
    /// If `parallel > 1`, Phase 2 uses worker threads with their own MySQL connections.
    pub fn stream_all_tables<W: Write>(
        &mut self,
        compression: CompressionMode,
        writer: &mut W,
        parallel: u32,
        mysql_url: &str,
    ) -> Result<Vec<(String, TableDisposition)>> {
        let mut dispositions: Vec<(String, TableDisposition)> = Vec::new();

        // Phase 1: Stream aggregate tables via BFS traversal.
        // This streams rows directly during traversal, only keeping PKs and FK values in memory.
        // Returns the set of tables that were touched by aggregates.
        let phase1_start = Instant::now();
        let aggregate_tables = self.stream_aggregate_tables(compression, writer)?;
        self.metrics.set_aggregate_wall_time(phase1_start.elapsed());
        self.metrics.snapshot_aggregate_phase();

        // Phase 2: Stream non-aggregate tables.
        // Collect the list of tables to stream with their metadata.
        let phase2_start = Instant::now();
        let all_tables: Vec<String> = self.conn.get_all_table_names()?;

        let mut tables_to_stream: Vec<TableTask> = Vec::new();
        for table in all_tables {
            if self.plan.ignored_tables.contains(&table) {
                // Ignored tables are not even in the Ready message, skip silently
                continue;
            }
            if self.plan.excluded_tables.contains(&table) {
                dispositions.push((table, TableDisposition::Excluded));
                continue;
            }
            if aggregate_tables.contains(&table) {
                dispositions.push((table, TableDisposition::Aggregate));
                continue;
            }

            let columns = self.conn.get_column_defs(&table)?;
            let anonymization = self
                .plan
                .anonymization
                .get(&table)
                .cloned()
                .unwrap_or_default();

            tables_to_stream.push(TableTask {
                table,
                columns,
                anonymization,
            });
        }

        if parallel <= 1 || tables_to_stream.len() <= 1 {
            // Sequential path
            for task in tables_to_stream {
                let total_rows = self.stream_full_table(
                    &task.table,
                    &task.columns,
                    task.anonymization,
                    compression,
                    writer,
                )?;

                write_message(
                    writer,
                    &ServerMessage::TableDone {
                        table: task.table.clone(),
                        row_count: total_rows,
                    },
                )?;
                if total_rows > 0 {
                    dispositions.push((task.table, TableDisposition::Full));
                } else {
                    dispositions.push((task.table, TableDisposition::Empty));
                }
            }
        } else {
            // Parallel path - collect table names before moving into parallel fn
            let table_names: Vec<String> = tables_to_stream.iter().map(|t| t.table.clone()).collect();
            let streamed = self.stream_full_tables_parallel(
                tables_to_stream,
                compression,
                writer,
                parallel as usize,
                mysql_url,
            )?;
            // streamed is the set of tables that actually had rows
            for name in table_names {
                if streamed.contains(&name) {
                    dispositions.push((name, TableDisposition::Full));
                } else {
                    dispositions.push((name, TableDisposition::Empty));
                }
            }
        }
        self.metrics.set_full_tables_wall_time(phase2_start.elapsed());

        dispositions.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(dispositions)
    }

    /// Stream all rows from a non-aggregate table directly from a MySQL query.
    /// Returns the total number of rows streamed.
    fn stream_full_table<W: Write>(
        &mut self,
        table: &str,
        columns: &[ColumnDef],
        anonymization: Vec<jibs_protocol::AnonymizeRule>,
        compression: CompressionMode,
        writer: &mut W,
    ) -> Result<u64> {
        let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let mut tsv_writer =
            TsvWriter::new(column_names, anonymization, self.plan.fakers.clone());
        let mut total_rows: u64 = 0;
        let mut chunk_row_count: u32 = 0;

        // Always send schema so the table is created locally even if empty
        let write_start = Instant::now();
        write_message(
            writer,
            &ServerMessage::Schema {
                table: table.to_string(),
                columns: columns.to_vec(),
            },
        )?;
        self.metrics.add_write_time(write_start.elapsed());

        let query = format!("SELECT * FROM `{}`", table);

        // Time the query execution
        let query_start = Instant::now();
        let result = self.conn.query_iter(&query)?;
        self.metrics.add_query_time(query_start.elapsed());

        for row_result in result {
            // Time row iteration (fetch from network/buffer)
            let iterate_start = Instant::now();
            let row: Row = row_result?;
            self.metrics.add_iterate_time(iterate_start.elapsed());

            // Time serialization
            let serialize_start = Instant::now();
            tsv_writer.write_row(&row)?;
            self.metrics.add_serialize_time(serialize_start.elapsed());

            chunk_row_count += 1;
            total_rows += 1;

            if chunk_row_count >= CHUNK_ROW_LIMIT as u32
                || tsv_writer.buffer_size() >= CHUNK_BYTE_LIMIT
            {
                flush_chunk(
                    table,
                    &mut tsv_writer,
                    chunk_row_count,
                    compression,
                    writer,
                    &self.metrics,
                )?;
                chunk_row_count = 0;
            }
        }

        // Flush remaining data
        if chunk_row_count > 0 {
            flush_chunk(
                table,
                &mut tsv_writer,
                chunk_row_count,
                compression,
                writer,
                &self.metrics,
            )?;
        }

        Ok(total_rows)
    }

    /// Stream full tables in parallel using worker threads.
    ///
    /// Each worker opens its own MySQL connection and streams assigned tables.
    /// Workers send ServerMessage values through a bounded channel.
    /// The main thread drains the channel and writes to stdout.
    fn stream_full_tables_parallel<W: Write>(
        &self,
        tables: Vec<TableTask>,
        compression: CompressionMode,
        writer: &mut W,
        num_workers: usize,
        mysql_url: &str,
    ) -> Result<HashSet<String>> {
        let num_workers = num_workers.min(tables.len());

        // Distribute tables round-robin across workers
        let mut worker_tasks: Vec<Vec<TableTask>> = (0..num_workers).map(|_| Vec::new()).collect();
        for (i, task) in tables.into_iter().enumerate() {
            worker_tasks[i % num_workers].push(task);
        }

        // Bounded channel: workers send messages, main thread writes them
        let (tx, rx) = mpsc::sync_channel::<ServerMessage>(num_workers * 4);

        // Clone data workers need
        let fakers = self.plan.fakers.clone();
        let mysql_url = mysql_url.to_string();
        let metrics = Arc::clone(&self.metrics);

        // Spawn worker threads
        let mut handles = Vec::with_capacity(num_workers);
        for tasks in worker_tasks {
            let tx = tx.clone();
            let fakers = fakers.clone();
            let mysql_url = mysql_url.clone();
            let metrics = Arc::clone(&metrics);

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
                        // Send error through channel before returning
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

        // Drop our copy of tx so the channel closes when all workers finish
        drop(tx);

        // Writer loop: drain channel and write to stdout
        let mut streamed_tables: HashSet<String> = HashSet::new();
        for msg in rx {
            if let ServerMessage::TableDone { ref table, row_count } = msg {
                if row_count > 0 {
                    streamed_tables.insert(table.clone());
                }
            }
            let write_start = Instant::now();
            write_message(writer, &msg)?;
            self.metrics.add_write_time(write_start.elapsed());
        }

        // Join all workers and propagate first error
        let mut first_error: Option<ServerError> = None;
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

        Ok(streamed_tables)
    }

    /// Stream all rows reachable from aggregates via BFS relation traversal.
    ///
    /// Instead of collecting all rows in memory, this streams rows directly during
    /// BFS traversal. Only PKs (for deduplication) and FK values (for next BFS level)
    /// are kept in memory.
    ///
    /// Traversal strategy:
    /// - From root table: follow BOTH forward and backward relations
    /// - From tables reached via BACKWARD traversal: continue bidirectional
    /// - From tables reached via FORWARD traversal: forward only
    fn stream_aggregate_tables<W: Write>(
        &mut self,
        compression: CompressionMode,
        writer: &mut W,
    ) -> Result<HashSet<String>> {
        // Track visited rows per table (only PKs, not full rows)
        let mut visited: HashMap<String, HashSet<Vec<u8>>> = HashMap::new();

        // Tables touched by aggregates
        let mut aggregate_tables: HashSet<String> = HashSet::new();

        // Full tables skip BFS and get imported fully in Phase 2
        let full_tables = &self.plan.full_tables;

        // Track which tables have had Schema sent and their total row counts
        let mut schemas_sent: HashSet<String> = HashSet::new();
        let mut table_row_counts: HashMap<String, u64> = HashMap::new();

        // Pre-cache schemas and PK columns for all tables that might be touched
        // (needed before we start streaming since we can't query metadata during iteration)
        let schema_start = Instant::now();
        let all_table_names_vec = self.conn.get_all_table_names()?;
        let all_table_names: HashSet<String> = all_table_names_vec.iter().cloned().collect();
        for table_name in &all_table_names_vec {
            self.conn.get_column_defs(table_name)?;
        }
        self.metrics.set_schema_cache_time(schema_start.elapsed());

        let aggregates = self.plan.aggregates.clone();
        for aggregate in &aggregates {
            aggregate_tables.insert(aggregate.root_table.clone());

            // Tables excluded from BFS for this aggregate (expand regex patterns)
            let mut exclude_tables: HashSet<String> = aggregate.exclude_tables.iter().cloned().collect();
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
            // Processed level-by-level to allow merging entries targeting the same table+column
            let mut pending: HashMap<(String, String, bool), Vec<MySqlValue>> = HashMap::new();

            // Stream root table
            let root_table = &aggregate.root_table;
            let root_query = Self::build_root_query(aggregate);
            let pk_columns = self
                .conn
                .get_cached_primary_key(root_table)
                .cloned()
                .unwrap_or_default();
            let columns = self
                .conn
                .get_column_defs(root_table)?;
            let anonymization = self
                .plan
                .anonymization
                .get(root_table.as_str())
                .cloned()
                .unwrap_or_default();

            // Prepare FK extraction info for the root table (pre-filtered)
            // When root_only, skip FK extraction entirely — no BFS traversal
            let forward_relations: Vec<_> = if aggregate.root_only {
                vec![]
            } else {
                self.relations_by_source
                    .get(root_table.as_str())
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|r| {
                        all_table_names.contains(&r.from_table)
                            && all_table_names.contains(&r.to_table)
                            && !exclude_tables.contains(&r.to_table)
                    })
                    .collect()
            };
            let backward_relations: Vec<_> = if aggregate.root_only {
                vec![]
            } else {
                self.relations_by_target
                    .get(root_table.as_str())
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|r| {
                        all_table_names.contains(&r.from_table)
                            && all_table_names.contains(&r.to_table)
                            && !exclude_tables.contains(&r.from_table)
                    })
                    .collect()
            };

            let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
            let mut tsv_writer = TsvWriter::new(
                column_names,
                anonymization,
                self.plan.fakers.clone(),
            );
            let mut chunk_row_count: u32 = 0;

            // Accumulators for FK values — indexed by relation to avoid String cloning per row
            let mut backward_fk_vecs: Vec<Vec<MySqlValue>> = vec![Vec::new(); backward_relations.len()];
            let mut forward_fk_vecs: Vec<Vec<MySqlValue>> = vec![Vec::new(); forward_relations.len()];

            // Pre-insert visited set for root table to avoid cloning table name per row
            let visited_set = visited.entry(root_table.clone()).or_default();

            // Stream root table rows
            let root_query_ms: u64;
            let mut root_iterate_ns: u64 = 0;
            let mut root_row_count: u64 = 0;
            {
                // Time query execution
                let query_start = Instant::now();
                let result = self.conn.query_iter(&root_query)?;
                let root_query_elapsed = query_start.elapsed();
                self.metrics.add_query_time(root_query_elapsed);
                root_query_ms = root_query_elapsed.as_millis() as u64;

                for row_result in result {
                    // Time row iteration
                    let iterate_start = Instant::now();
                    let row: Row = row_result?;
                    let iter_elapsed = iterate_start.elapsed();
                    self.metrics.add_iterate_time(iter_elapsed);
                    root_iterate_ns += iter_elapsed.as_nanos() as u64;
                    root_row_count += 1;

                    // Send schema on first row
                    if schemas_sent.insert(root_table.clone()) {
                        let write_start = Instant::now();
                        write_message(
                            writer,
                            &ServerMessage::Schema {
                                table: root_table.clone(),
                                columns: columns.clone(),
                            },
                        )?;
                        self.metrics.add_write_time(write_start.elapsed());
                    }

                    // Dedup check + FK extraction
                    let dedup_start = Instant::now();
                    let pk = extract_pk_from_row(&row, &pk_columns);
                    let pk_bytes = serialize_pk(&pk);
                    let is_new = visited_set.insert(pk_bytes);

                    // Always extract backward FK values from root table rows,
                    // even for already-visited rows. A previous aggregate may have
                    // reached this table via forward traversal (which skips backward
                    // relations), so we need to ensure child tables are discovered.
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
                        self.metrics.add_dedup_time(dedup_start.elapsed());
                        continue; // Already visited — skip TSV and forward FK extraction
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
                    self.metrics.add_dedup_time(dedup_start.elapsed());

                    // Write TSV - time serialization
                    let serialize_start = Instant::now();
                    tsv_writer.write_row(&row)?;
                    self.metrics.add_serialize_time(serialize_start.elapsed());

                    chunk_row_count += 1;
                    *table_row_counts.entry(root_table.clone()).or_default() += 1;

                    if chunk_row_count >= CHUNK_ROW_LIMIT as u32
                        || tsv_writer.buffer_size() >= CHUNK_BYTE_LIMIT
                    {
                        flush_chunk(
                            root_table,
                            &mut tsv_writer,
                            chunk_row_count,
                            compression,
                            writer,
                            &self.metrics,
                        )?;
                        chunk_row_count = 0;
                    }
                }
            }
            // QueryResult dropped here, conn is free

            // Record per-query timing for root query
            self.metrics.record_query(jibs_protocol::QueryTiming {
                table: root_table.clone(),
                column: String::new(),
                num_values: 0,
                query_ms: root_query_ms,
                iterate_ms: root_iterate_ns / 1_000_000,
                rows: root_row_count,
            });

            // Flush remaining root table data
            if chunk_row_count > 0 {
                flush_chunk(
                    root_table,
                    &mut tsv_writer,
                    chunk_row_count,
                    compression,
                    writer,
                    &self.metrics,
                )?;
            }

            // Seed pending set from root table FK vecs
            for (i, relation) in backward_relations.iter().enumerate() {
                let values = std::mem::take(&mut backward_fk_vecs[i]);
                if !values.is_empty() {
                    let dedup_start = Instant::now();
                    let unique = dedupe_values(values);
                    self.metrics.add_dedup_time(dedup_start.elapsed());
                    if !unique.is_empty() {
                        pending
                            .entry((relation.from_table.clone(), relation.from_column.clone(), true))
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
                    self.metrics.add_dedup_time(dedup_start.elapsed());
                    if !unique.is_empty() {
                        pending
                            .entry((relation.to_table.clone(), relation.to_column.clone(), false))
                            .or_default()
                            .extend(unique);
                    }
                }
            }

            // BFS traversal — level by level
            // Each level drains all pending entries, coalesces by (table, column, via_backward),
            // and splits large value sets into batches of MAX_IN_VALUES.
            while !pending.is_empty() {
                let current_level: Vec<_> = pending.drain().collect();

                for ((table, column, reached_via_backward), fk_values) in current_level {
                    // Skip full_tables — they will be imported in full by Phase 2
                    if full_tables.contains(&table) {
                        continue;
                    }

                    // Skip tables excluded from this aggregate's BFS
                    if exclude_tables.contains(&table) {
                        continue;
                    }

                    aggregate_tables.insert(table.clone());

                    if self.plan.excluded_tables.contains(&table) {
                        continue;
                    }

                    // Get cached metadata (pre-cached above)
                    let pk_columns = self
                        .conn
                        .get_cached_primary_key(&table)
                        .cloned()
                        .unwrap_or_default();
                    let columns = self.conn.get_column_defs(&table)?;
                    let anonymization = self
                        .plan
                        .anonymization
                        .get(&table)
                        .cloned()
                        .unwrap_or_default();

                    // Prepare FK extraction info (pre-filtered)
                    let forward_relations: Vec<_> = self
                        .relations_by_source
                        .get(table.as_str())
                        .cloned()
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|r| {
                            all_table_names.contains(&r.from_table)
                                && all_table_names.contains(&r.to_table)
                                && !exclude_tables.contains(&r.to_table)
                        })
                        .collect();
                    let backward_relations: Vec<_> = if reached_via_backward {
                        self.relations_by_target
                            .get(table.as_str())
                            .cloned()
                            .unwrap_or_default()
                            .into_iter()
                            .filter(|r| {
                                all_table_names.contains(&r.from_table)
                                    && all_table_names.contains(&r.to_table)
                                    && !exclude_tables.contains(&r.from_table)
                            })
                            .collect()
                    } else {
                        vec![]
                    };

                    let column_names: Vec<String> =
                        columns.iter().map(|c| c.name.clone()).collect();
                    let mut tsv_writer = TsvWriter::new(
                        column_names,
                        anonymization,
                        self.plan.fakers.clone(),
                    );
                    let mut chunk_row_count: u32 = 0;

                    // FK accumulators indexed by relation (avoids String cloning per row)
                    let mut fwd_fk_vecs: Vec<Vec<MySqlValue>> = vec![Vec::new(); forward_relations.len()];
                    let mut bwd_fk_vecs: Vec<Vec<MySqlValue>> = vec![Vec::new(); backward_relations.len()];

                    // Pre-get visited set for this table
                    let visited_set = visited.entry(table.clone()).or_default();

                    // Pre-get row count entry
                    let row_count_entry = table_row_counts.entry(table.clone()).or_default();

                    // Dedupe and batch the FK values
                    let dedup_start = Instant::now();
                    let unique_values = dedupe_values(fk_values);
                    self.metrics.add_dedup_time(dedup_start.elapsed());

                    for batch in unique_values.chunks(MAX_IN_VALUES) {
                        let batch_len = batch.len() as u32;
                        let batch_query_ms: u64;
                        let mut batch_iterate_ns: u64 = 0;
                        let mut batch_row_count: u64 = 0;
                        // Build and execute the IN query with streaming
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

                            // Time query execution
                            let query_start = Instant::now();
                            let result = self.conn.exec_iter(
                                &query,
                                mysql::Params::Positional(params),
                            )?;
                            let batch_query_elapsed = query_start.elapsed();
                            self.metrics.add_query_time(batch_query_elapsed);
                            batch_query_ms = batch_query_elapsed.as_millis() as u64;

                            for row_result in result {
                                // Time row iteration
                                let iterate_start = Instant::now();
                                let row: Row = row_result?;
                                let iter_elapsed = iterate_start.elapsed();
                                self.metrics.add_iterate_time(iter_elapsed);
                                batch_iterate_ns += iter_elapsed.as_nanos() as u64;
                                batch_row_count += 1;

                                // Dedup check + FK extraction
                                let dedup_start = Instant::now();
                                let pk = extract_pk_from_row(&row, &pk_columns);
                                let pk_bytes = serialize_pk(&pk);
                                if !visited_set.insert(pk_bytes) {
                                    self.metrics.add_dedup_time(dedup_start.elapsed());
                                    continue; // Already visited
                                }

                                // Extract FK values for forward relations
                                for (i, relation) in forward_relations.iter().enumerate() {
                                    if let Some(Ok(v)) = row
                                        .get_opt::<MySqlValue, _>(
                                            relation.from_column.as_str(),
                                        )
                                    {
                                        if !matches!(v, MySqlValue::NULL) {
                                            fwd_fk_vecs[i].push(v);
                                        }
                                    }
                                }

                                // Extract FK values for backward relations
                                for (i, relation) in backward_relations.iter().enumerate() {
                                    if let Some(Ok(v)) = row
                                        .get_opt::<MySqlValue, _>(
                                            relation.to_column.as_str(),
                                        )
                                    {
                                        if !matches!(v, MySqlValue::NULL) {
                                            bwd_fk_vecs[i].push(v);
                                        }
                                    }
                                }
                                self.metrics.add_dedup_time(dedup_start.elapsed());

                                // Send schema on first new row for this table
                                if schemas_sent.insert(table.clone()) {
                                    let write_start = Instant::now();
                                    write_message(
                                        writer,
                                        &ServerMessage::Schema {
                                            table: table.clone(),
                                            columns: columns.clone(),
                                        },
                                    )?;
                                    self.metrics.add_write_time(write_start.elapsed());
                                }

                                // Write TSV - time serialization
                                let serialize_start = Instant::now();
                                tsv_writer.write_row(&row)?;
                                self.metrics.add_serialize_time(serialize_start.elapsed());

                                chunk_row_count += 1;
                                *row_count_entry += 1;

                                if chunk_row_count >= CHUNK_ROW_LIMIT as u32
                                    || tsv_writer.buffer_size() >= CHUNK_BYTE_LIMIT
                                {
                                    flush_chunk(
                                        &table,
                                        &mut tsv_writer,
                                        chunk_row_count,
                                        compression,
                                        writer,
                                        &self.metrics,
                                    )?;
                                    chunk_row_count = 0;
                                }
                            }
                        }
                        // QueryResult dropped, conn is free

                        // Record per-query timing for this batch
                        self.metrics.record_query(jibs_protocol::QueryTiming {
                            table: table.clone(),
                            column: column.clone(),
                            num_values: batch_len,
                            query_ms: batch_query_ms,
                            iterate_ms: batch_iterate_ns / 1_000_000,
                            rows: batch_row_count,
                        });
                    }

                    // Flush remaining data for this table+column group
                    if chunk_row_count > 0 {
                        flush_chunk(
                            &table,
                            &mut tsv_writer,
                            chunk_row_count,
                            compression,
                            writer,
                            &self.metrics,
                        )?;
                    }

                    // Coalesce FK values into next level's pending set
                    for (i, relation) in forward_relations.iter().enumerate() {
                        let values = std::mem::take(&mut fwd_fk_vecs[i]);
                        if !values.is_empty() {
                            let dedup_start = Instant::now();
                            let unique = dedupe_values(values);
                            self.metrics.add_dedup_time(dedup_start.elapsed());
                            if !unique.is_empty() {
                                pending
                                    .entry((relation.to_table.clone(), relation.to_column.clone(), false))
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
                            self.metrics.add_dedup_time(dedup_start.elapsed());
                            if !unique.is_empty() {
                                pending
                                    .entry((relation.from_table.clone(), relation.from_column.clone(), true))
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
            let write_start = Instant::now();
            write_message(
                writer,
                &ServerMessage::TableDone {
                    table: table.clone(),
                    row_count: *row_count,
                },
            )?;
            self.metrics.add_write_time(write_start.elapsed());
        }

        Ok(aggregate_tables)
    }

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

}

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
/// This handles the case where MySQL returns the same value as different types
/// (e.g., INT(1) vs Bytes("1") depending on the query)
fn normalize_value(value: &MySqlValue) -> MySqlValue {
    match value {
        // Try to parse bytes as an integer if possible
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
        // UInt can be normalized to Int if it fits
        MySqlValue::UInt(u) if *u <= i64::MAX as u64 => MySqlValue::Int(*u as i64),
        _ => value.clone(),
    }
}

/// Serialize a primary key to bytes for deduplication
fn serialize_pk(pk: &[MySqlValue]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in pk {
        // Normalize value before serialization to handle type mismatches
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
            // Compress with zstd
            match zstd::encode_all(data.as_slice(), 3) {
                Ok(compressed) => {
                    // Prepend uncompressed length
                    let mut result = Vec::with_capacity(4 + compressed.len());
                    result.extend_from_slice(&(data.len() as u32).to_le_bytes());
                    result.extend(compressed);
                    result
                }
                Err(_) => data, // Fall back to uncompressed on error
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

    for row_result in result {
        // Time row iteration
        let iterate_start = Instant::now();
        let row: Row = row_result?;
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

            // Track metrics
            metrics.add_bytes_sent(bytes);
            metrics.add_rows_sent(chunk_row_count as u64);

            let compress_start = Instant::now();
            let compressed = maybe_compress(tsv_data, compression);
            metrics.add_compress_time(compress_start.elapsed());
            let msg = ServerMessage::Data {
                table: table.to_string(),
                row_count: chunk_row_count,
                tsv_data: compressed,
            };
            tx.send(msg)
                .map_err(|_| crate::error::ServerError::Protocol("Channel closed".to_string()))?;
            chunk_row_count = 0;
        }
    }

    // Flush remaining data
    if chunk_row_count > 0 {
        let tsv_data = tsv_writer.take_buffer();
        let bytes = tsv_data.len() as u64;

        // Track metrics
        metrics.add_bytes_sent(bytes);
        metrics.add_rows_sent(chunk_row_count as u64);

        let compress_start = Instant::now();
        let compressed = maybe_compress(tsv_data, compression);
        metrics.add_compress_time(compress_start.elapsed());
        let msg = ServerMessage::Data {
            table: table.to_string(),
            row_count: chunk_row_count,
            tsv_data: compressed,
        };
        tx.send(msg)
            .map_err(|_| crate::error::ServerError::Protocol("Channel closed".to_string()))?;
    }

    // Send TableDone (always, even for empty tables so the client marks them complete)
    tx.send(ServerMessage::TableDone {
        table: table.to_string(),
        row_count: total_rows,
    })
    .map_err(|_| crate::error::ServerError::Protocol("Channel closed".to_string()))?;

    Ok(())
}
