//! Dependency graph traversal for collecting related rows

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;

use mysql::{Row, Value as MySqlValue};

use jibs_protocol::{
    framing::write_message, Checkpoint, ColumnDef, CompressionMode, ExecutionPlan, Relation,
    ResolvedAggregate, ServerMessage, SortDirection,
};

use crate::error::Result;
use crate::mysql::MySqlConnection;
use crate::tsv::TsvWriter;

/// Maximum rows per chunk
const CHUNK_ROW_LIMIT: usize = 10_000;
/// Maximum bytes per chunk
const CHUNK_BYTE_LIMIT: usize = 10 * 1024 * 1024; // 10MB

// ============================================================================
// Streaming helpers
// ============================================================================

/// Flush a TSV buffer as a Data message if non-empty.
/// Returns the number of bytes flushed.
fn flush_chunk<W: Write>(
    table: &str,
    tsv_writer: &mut TsvWriter,
    chunk_row_count: u32,
    compression: CompressionMode,
    checkpoint: &mut Checkpoint,
    writer: &mut W,
) -> Result<()> {
    if tsv_writer.is_empty() {
        return Ok(());
    }

    let tsv_data = tsv_writer.take_buffer();
    let bytes = tsv_data.len() as u64;
    checkpoint.bytes_transferred += bytes;
    checkpoint.rows_transferred += chunk_row_count as u64;

    let msg = ServerMessage::Data {
        table: table.to_string(),
        row_count: chunk_row_count,
        tsv_data: maybe_compress(tsv_data, compression),
        checkpoint: checkpoint.clone(),
    };
    write_message(writer, &msg)?;
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
}

impl<'a> DependencyTraverser<'a> {
    /// Create a new traverser
    pub fn new(conn: &'a mut MySqlConnection, plan: &'a ExecutionPlan) -> Result<Self> {
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

        Ok(Self {
            conn,
            plan,
            relations_by_source,
            relations_by_target,
        })
    }

    /// Stream all tables, processing one table at a time to avoid loading all rows into memory.
    ///
    /// Logic:
    /// 1. Tables touched by aggregates: only include rows reachable from aggregate (streamed during BFS)
    /// 2. Tables NOT touched by aggregates: include ALL rows (streamed directly from query)
    /// 3. Excluded tables: skip data (structure only)
    /// 4. Ignored tables: skip entirely
    ///
    /// If `resume_from` is provided, skip tables already completed.
    pub fn stream_all_tables<W: Write>(
        &mut self,
        resume_from: Option<Checkpoint>,
        compression: CompressionMode,
        writer: &mut W,
    ) -> Result<()> {
        let mut checkpoint = resume_from.unwrap_or_default();

        // Phase 1: Stream aggregate tables via BFS traversal.
        // This streams rows directly during traversal, only keeping PKs and FK values in memory.
        // Returns the set of tables that were touched by aggregates.
        let aggregate_tables = self.stream_aggregate_tables(compression, &mut checkpoint, writer)?;

        // Phase 2: Stream non-aggregate tables one at a time.
        let all_tables: Vec<String> = self.conn.get_all_table_names()?;

        for table in all_tables {
            if self.plan.ignored_tables.contains(&table) {
                continue;
            }
            if self.plan.excluded_tables.contains(&table) {
                continue;
            }
            if aggregate_tables.contains(&table) {
                continue;
            }
            if checkpoint.should_skip_table(&table) {
                continue;
            }

            let columns = self.conn.get_column_defs(&table)?;
            let anonymization = self
                .plan
                .anonymization
                .get(&table)
                .cloned()
                .unwrap_or_default();

            let total_rows = self.stream_full_table(
                &table,
                &columns,
                anonymization,
                compression,
                &mut checkpoint,
                writer,
            )?;

            if total_rows > 0 {
                write_message(
                    writer,
                    &ServerMessage::TableDone {
                        table: table.clone(),
                        row_count: total_rows,
                    },
                )?;
            }
        }

        Ok(())
    }

    /// Stream all rows from a non-aggregate table directly from a MySQL query.
    /// Returns the total number of rows streamed.
    fn stream_full_table<W: Write>(
        &mut self,
        table: &str,
        columns: &[ColumnDef],
        anonymization: Vec<jibs_protocol::AnonymizeRule>,
        compression: CompressionMode,
        checkpoint: &mut Checkpoint,
        writer: &mut W,
    ) -> Result<u64> {
        let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let mut tsv_writer =
            TsvWriter::new(column_names, anonymization, self.plan.fakers.clone());
        let mut total_rows: u64 = 0;
        let mut chunk_row_count: u32 = 0;
        let mut schema_sent = false;

        let query = format!("SELECT * FROM `{}`", table);
        let result = self.conn.query_iter(&query)?;

        for row_result in result {
            let row: Row = row_result?;

            // Send schema before first row
            if !schema_sent {
                write_message(
                    writer,
                    &ServerMessage::Schema {
                        table: table.to_string(),
                        columns: columns.to_vec(),
                    },
                )?;
                schema_sent = true;
            }

            tsv_writer.write_row(&row)?;
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
                    checkpoint,
                    writer,
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
                checkpoint,
                writer,
            )?;
        }

        Ok(total_rows)
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
        checkpoint: &mut Checkpoint,
        writer: &mut W,
    ) -> Result<HashSet<String>> {
        // Track visited rows per table (only PKs, not full rows)
        let mut visited: HashMap<String, HashSet<Vec<u8>>> = HashMap::new();

        // Tables touched by aggregates
        let mut aggregate_tables: HashSet<String> = HashSet::new();

        // Track which tables have had Schema sent and their total row counts
        let mut schemas_sent: HashSet<String> = HashSet::new();
        let mut table_row_counts: HashMap<String, u64> = HashMap::new();

        // Pre-cache schemas and PK columns for all tables that might be touched
        // (needed before we start streaming since we can't query metadata during iteration)
        let all_table_names = self.conn.get_all_table_names()?;
        for table_name in &all_table_names {
            self.conn.get_column_defs(table_name)?;
        }

        let aggregates = self.plan.aggregates.clone();
        for aggregate in &aggregates {
            aggregate_tables.insert(aggregate.root_table.clone());

            // BFS queue: (table_name, fk_column, fk_values, reached_via_backward)
            let mut queue: VecDeque<(String, String, Vec<MySqlValue>, bool)> = VecDeque::new();

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

            // Prepare FK extraction info for the root table
            let forward_relations: Vec<_> = self
                .relations_by_source
                .get(root_table.as_str())
                .cloned()
                .unwrap_or_default();
            let backward_relations: Vec<_> = self
                .relations_by_target
                .get(root_table.as_str())
                .cloned()
                .unwrap_or_default();

            let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
            let mut tsv_writer = TsvWriter::new(
                column_names,
                anonymization,
                self.plan.fakers.clone(),
            );
            let mut chunk_row_count: u32 = 0;

            // Accumulators for FK values to queue after streaming
            let mut fk_accumulator: HashMap<(String, String, bool), Vec<MySqlValue>> =
                HashMap::new();

            // Stream root table rows
            {
                let result = self.conn.query_iter(&root_query)?;

                for row_result in result {
                    let row: Row = row_result?;

                    // Send schema on first row
                    if schemas_sent.insert(root_table.clone()) {
                        write_message(
                            writer,
                            &ServerMessage::Schema {
                                table: root_table.clone(),
                                columns: columns.clone(),
                            },
                        )?;
                    }

                    // Dedup check
                    let pk = extract_pk_from_row(&row, &pk_columns);
                    let pk_bytes = serialize_pk(&pk);
                    if !visited
                        .entry(root_table.clone())
                        .or_default()
                        .insert(pk_bytes)
                    {
                        continue; // Already visited
                    }

                    // Extract FK values for forward relations
                    for relation in &forward_relations {
                        if !all_table_names.contains(&relation.from_table) || !all_table_names.contains(&relation.to_table) {
                            continue;
                        }
                        if let Some(Ok(v)) =
                            row.get_opt::<MySqlValue, _>(relation.from_column.as_str())
                        {
                            if !matches!(v, MySqlValue::NULL) {
                                fk_accumulator
                                    .entry((
                                        relation.to_table.clone(),
                                        relation.to_column.clone(),
                                        false,
                                    ))
                                    .or_default()
                                    .push(v);
                            }
                        }
                    }

                    // Extract FK values for backward relations (bidirectional from root)
                    for relation in &backward_relations {
                        if !all_table_names.contains(&relation.from_table) || !all_table_names.contains(&relation.to_table) {
                            continue;
                        }
                        if let Some(Ok(v)) =
                            row.get_opt::<MySqlValue, _>(relation.to_column.as_str())
                        {
                            if !matches!(v, MySqlValue::NULL) {
                                fk_accumulator
                                    .entry((
                                        relation.from_table.clone(),
                                        relation.from_column.clone(),
                                        true,
                                    ))
                                    .or_default()
                                    .push(v);
                            }
                        }
                    }

                    // Write TSV
                    tsv_writer.write_row(&row)?;
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
                            checkpoint,
                            writer,
                        )?;
                        chunk_row_count = 0;
                    }
                }
            }
            // QueryResult dropped here, conn is free

            // Flush remaining root table data
            if chunk_row_count > 0 {
                flush_chunk(
                    root_table,
                    &mut tsv_writer,
                    chunk_row_count,
                    compression,
                    checkpoint,
                    writer,
                )?;
            }

            // Queue accumulated FK values
            for ((to_table, to_column, via_backward), values) in fk_accumulator.drain() {
                let unique = dedupe_values(values);
                if !unique.is_empty() {
                    queue.push_back((to_table, to_column, unique, via_backward));
                }
            }

            // BFS traversal
            while let Some((table, column, fk_values, reached_via_backward)) = queue.pop_front() {
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

                // Prepare FK extraction info
                let forward_relations: Vec<_> = self
                    .relations_by_source
                    .get(table.as_str())
                    .cloned()
                    .unwrap_or_default();
                let backward_relations: Vec<_> = if reached_via_backward {
                    self.relations_by_target
                        .get(table.as_str())
                        .cloned()
                        .unwrap_or_default()
                } else {
                    vec![]
                };

                let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                let mut tsv_writer = TsvWriter::new(
                    column_names,
                    anonymization,
                    self.plan.fakers.clone(),
                );
                let mut chunk_row_count: u32 = 0;
                let mut fk_accumulator: HashMap<(String, String, bool), Vec<MySqlValue>> =
                    HashMap::new();

                // Build and execute the IN query with streaming
                {
                    let placeholders: Vec<&str> = (0..fk_values.len()).map(|_| "?").collect();
                    let query = format!(
                        "SELECT * FROM `{}` WHERE `{}` IN ({})",
                        table,
                        column,
                        placeholders.join(", ")
                    );
                    let params: Vec<MySqlValue> = fk_values;

                    let result =
                        self.conn
                            .exec_iter(&query, mysql::Params::Positional(params))?;

                    for row_result in result {
                        let row: Row = row_result?;

                        // Dedup check
                        let pk = extract_pk_from_row(&row, &pk_columns);
                        let pk_bytes = serialize_pk(&pk);
                        if !visited
                            .entry(table.clone())
                            .or_default()
                            .insert(pk_bytes)
                        {
                            continue; // Already visited
                        }

                        // Send schema on first new row for this table
                        if schemas_sent.insert(table.clone()) {
                            write_message(
                                writer,
                                &ServerMessage::Schema {
                                    table: table.clone(),
                                    columns: columns.clone(),
                                },
                            )?;
                        }

                        // Extract FK values for forward relations
                        for relation in &forward_relations {
                            if let Some(Ok(v)) =
                                row.get_opt::<MySqlValue, _>(relation.from_column.as_str())
                            {
                                if !matches!(v, MySqlValue::NULL) {
                                    fk_accumulator
                                        .entry((
                                            relation.to_table.clone(),
                                            relation.to_column.clone(),
                                            false,
                                        ))
                                        .or_default()
                                        .push(v);
                                }
                            }
                        }

                        // Extract FK values for backward relations
                        for relation in &backward_relations {
                            if let Some(Ok(v)) =
                                row.get_opt::<MySqlValue, _>(relation.to_column.as_str())
                            {
                                if !matches!(v, MySqlValue::NULL) {
                                    fk_accumulator
                                        .entry((
                                            relation.from_table.clone(),
                                            relation.from_column.clone(),
                                            true,
                                        ))
                                        .or_default()
                                        .push(v);
                                }
                            }
                        }

                        // Write TSV
                        tsv_writer.write_row(&row)?;
                        chunk_row_count += 1;
                        *table_row_counts.entry(table.clone()).or_default() += 1;

                        if chunk_row_count >= CHUNK_ROW_LIMIT as u32
                            || tsv_writer.buffer_size() >= CHUNK_BYTE_LIMIT
                        {
                            flush_chunk(
                                &table,
                                &mut tsv_writer,
                                chunk_row_count,
                                compression,
                                checkpoint,
                                writer,
                            )?;
                            chunk_row_count = 0;
                        }
                    }
                }
                // QueryResult dropped, conn is free

                // Flush remaining data for this BFS step
                if chunk_row_count > 0 {
                    flush_chunk(
                        &table,
                        &mut tsv_writer,
                        chunk_row_count,
                        compression,
                        checkpoint,
                        writer,
                    )?;
                }

                // Queue accumulated FK values
                for ((to_table, to_column, via_backward), values) in fk_accumulator.drain() {
                    let unique = dedupe_values(values);
                    if !unique.is_empty() {
                        queue.push_back((to_table, to_column, unique, via_backward));
                    }
                }
            }
        }

        // Send TableDone for all aggregate tables that had rows
        for (table, row_count) in &table_row_counts {
            write_message(
                writer,
                &ServerMessage::TableDone {
                    table: table.clone(),
                    row_count: *row_count,
                },
            )?;
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
