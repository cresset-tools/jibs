//! Dependency graph traversal for collecting related rows

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;

use mysql::{Row, Value as MySqlValue};

use jibs_protocol::{
    framing::write_message, Checkpoint, ColumnDef, CompressionMode, ExecutionPlan, Relation,
    ResolvedAggregate, ServerMessage, SortDirection,
};

use crate::error::{Result, ServerError};
use crate::mysql::MySqlConnection;
use crate::tsv::TsvWriter;

/// Maximum rows per chunk
const CHUNK_ROW_LIMIT: usize = 10_000;
/// Maximum bytes per chunk
const CHUNK_BYTE_LIMIT: usize = 10 * 1024 * 1024; // 10MB

// ============================================================================
// Table Streamer - generates chunks for a single table
// ============================================================================

/// Generates data chunks for a single table
struct TableStreamer {
    table: String,
    columns: Vec<ColumnDef>,
    rows: std::vec::IntoIter<Row>,
    tsv_writer: TsvWriter,
    total_rows: u64,
    done: bool,
}

impl TableStreamer {
    /// Create a new table streamer
    fn new(
        table: String,
        columns: Vec<ColumnDef>,
        rows: Vec<Row>,
        anonymization: Vec<jibs_protocol::AnonymizeRule>,
        fakers: HashMap<String, Vec<String>>,
    ) -> Self {
        let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        let tsv_writer = TsvWriter::new(column_names, anonymization, fakers);

        Self {
            table,
            columns,
            rows: rows.into_iter(),
            tsv_writer,
            total_rows: 0,
            done: false,
        }
    }

    /// Get the schema message for this table
    fn schema_message(&self) -> ServerMessage {
        ServerMessage::Schema {
            table: self.table.clone(),
            columns: self.columns.clone(),
        }
    }

    /// Generate the next chunk, returns None when done
    fn next_chunk(&mut self, compression: CompressionMode) -> Result<Option<(ServerMessage, u64)>> {
        if self.done {
            return Ok(None);
        }

        let mut chunk_row_count = 0u32;

        loop {
            // Check if we should flush current buffer
            if chunk_row_count >= CHUNK_ROW_LIMIT as u32
                || self.tsv_writer.buffer_size() >= CHUNK_BYTE_LIMIT
            {
                let tsv_data = self.tsv_writer.take_buffer();
                let bytes = tsv_data.len() as u64;
                let msg = ServerMessage::Data {
                    table: self.table.clone(),
                    row_count: chunk_row_count,
                    tsv_data: maybe_compress(tsv_data, compression),
                    checkpoint: Checkpoint::default(), // Will be updated by caller
                };
                return Ok(Some((msg, bytes)));
            }

            // Try to get next row
            match self.rows.next() {
                Some(row) => {
                    self.tsv_writer.write_row(&row)?;
                    chunk_row_count += 1;
                    self.total_rows += 1;
                }
                None => {
                    // No more rows
                    self.done = true;

                    // Flush any remaining data
                    if !self.tsv_writer.is_empty() {
                        let tsv_data = self.tsv_writer.take_buffer();
                        let bytes = tsv_data.len() as u64;
                        let msg = ServerMessage::Data {
                            table: self.table.clone(),
                            row_count: chunk_row_count,
                            tsv_data: maybe_compress(tsv_data, compression),
                            checkpoint: Checkpoint::default(),
                        };
                        return Ok(Some((msg, bytes)));
                    }

                    return Ok(None);
                }
            }
        }
    }

    /// Get the TableDone message
    fn done_message(&self) -> ServerMessage {
        ServerMessage::TableDone {
            table: self.table.clone(),
            row_count: self.total_rows,
        }
    }

    /// Check if this streamer is done
    fn is_done(&self) -> bool {
        self.done
    }
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

    /// Stream all tables with interleaved chunks for parallel client loading
    ///
    /// Logic:
    /// 1. Tables touched by aggregates: only include rows reachable from aggregate
    /// 2. Tables NOT touched by aggregates: include ALL rows
    /// 3. Excluded tables: skip data (structure only)
    /// 4. Ignored tables: skip entirely
    ///
    /// Streaming strategy:
    /// 1. Prepare all table data and create streamers
    /// 2. Send all Schema messages upfront
    /// 3. Round-robin data chunks across tables
    /// 4. Send TableDone as each table completes
    ///
    /// If `resume_from` is provided, skip tables already completed.
    pub fn stream_all_tables<W: Write>(
        &mut self,
        resume_from: Option<Checkpoint>,
        compression: CompressionMode,
        writer: &mut W,
    ) -> Result<()> {
        // Initialize checkpoint (either from resume or fresh)
        let mut checkpoint = resume_from.unwrap_or_default();

        // First, collect rows from all aggregates
        let (mut aggregate_rows, aggregate_tables) = self.collect_aggregate_rows()?;

        // Get all tables in the database
        let all_tables: Vec<String> = self.conn.get_all_table_names()?;

        // Build list of tables to stream with their rows
        let mut streamers: Vec<TableStreamer> = Vec::new();

        for table in all_tables {
            // Skip ignored tables
            if self.plan.ignored_tables.contains(&table) {
                continue;
            }

            // Skip excluded tables (structure only, handled elsewhere)
            if self.plan.excluded_tables.contains(&table) {
                continue;
            }

            // Skip already completed tables (on resume)
            if checkpoint.should_skip_table(&table) {
                continue;
            }

            // Get rows for this table
            let rows = if aggregate_tables.contains(&table) {
                // This table is filtered by an aggregate - use collected rows
                aggregate_rows.remove(&table).unwrap_or_default()
            } else {
                // This table is not filtered - query ALL rows
                let query = format!("SELECT * FROM `{}`", table);
                self.conn.query_rows(&query)?
            };

            // Skip empty tables
            if rows.is_empty() {
                continue;
            }

            // Get schema and anonymization rules
            let columns = self.conn.get_column_defs(&table)?;
            let anonymization = self
                .plan
                .anonymization
                .get(&table)
                .cloned()
                .unwrap_or_default();

            // Create streamer
            let streamer = TableStreamer::new(
                table,
                columns,
                rows,
                anonymization,
                self.plan.fakers.clone(),
            );
            streamers.push(streamer);
        }

        // Phase 1: Send all Schema messages upfront
        for streamer in &streamers {
            write_message(writer, &streamer.schema_message())?;
        }

        // Phase 2: Interleaved data streaming (round-robin)
        loop {
            let mut any_progress = false;

            for streamer in &mut streamers {
                if streamer.is_done() {
                    continue;
                }

                // Get next chunk from this table
                if let Some((mut msg, bytes)) = streamer.next_chunk(compression)? {
                    // Update checkpoint in the message
                    checkpoint.bytes_transferred += bytes;
                    checkpoint.rows_transferred += match &msg {
                        ServerMessage::Data { row_count, .. } => *row_count as u64,
                        _ => 0,
                    };

                    if let ServerMessage::Data { checkpoint: ref mut cp, .. } = msg {
                        *cp = checkpoint.clone();
                    }

                    write_message(writer, &msg)?;
                    any_progress = true;
                }

                // Check if this table just finished
                if streamer.is_done() {
                    write_message(writer, &streamer.done_message())?;
                }
            }

            // If no streamer made progress, we're done
            if !any_progress {
                break;
            }
        }

        Ok(())
    }

    /// Collect all rows reachable from aggregates via relations
    ///
    /// Traversal strategy:
    /// - From root table: follow BOTH forward (FKs we reference) and backward (tables that reference us)
    /// - From tables reached via BACKWARD traversal: continue bidirectional (follow ownership chain)
    /// - From tables reached via FORWARD traversal: forward only (don't reverse direction)
    ///
    /// This prevents cycles where e.g. products -> order_items -> orders pulls in unrelated orders,
    /// while still allowing proper traversal when rooted at parent tables like users.
    fn collect_aggregate_rows(&mut self) -> Result<(HashMap<String, Vec<Row>>, HashSet<String>)> {
        // Track visited rows per table to avoid duplicates
        let mut visited: HashMap<String, HashSet<Vec<u8>>> = HashMap::new();

        // Rows collected, grouped by table
        let mut collected_rows: HashMap<String, Vec<Row>> = HashMap::new();

        // Tables that are touched by aggregates (will be filtered)
        let mut aggregate_tables: HashSet<String> = HashSet::new();

        // Process each aggregate
        for aggregate in &self.plan.aggregates.clone() {
            aggregate_tables.insert(aggregate.root_table.clone());

            // BFS queue: (table_name, fk_column, fk_values, reached_via_backward)
            // reached_via_backward=true means we can continue bidirectional traversal
            let mut queue: VecDeque<(String, String, Vec<MySqlValue>, bool)> = VecDeque::new();

            // Start with root table query
            let root_rows = self.query_root_table(aggregate)?;

            if !root_rows.is_empty() {
                // Mark root rows as visited
                let root_pks = self.extract_primary_keys(&aggregate.root_table, &root_rows)?;
                for pk in root_pks.iter() {
                    let pk_bytes = self.serialize_pk(pk);
                    visited
                        .entry(aggregate.root_table.clone())
                        .or_default()
                        .insert(pk_bytes);
                }

                // Queue related tables - from root, do BIDIRECTIONAL traversal
                self.queue_related_tables_directional(
                    &aggregate.root_table,
                    &root_rows,
                    &mut queue,
                    true, // bidirectional for root
                )?;

                // Store root rows
                collected_rows
                    .entry(aggregate.root_table.clone())
                    .or_default()
                    .extend(root_rows);
            }

            // BFS traversal for this aggregate
            while let Some((table, column, fk_values, reached_via_backward)) = queue.pop_front() {
                // Mark this table as aggregate-touched
                aggregate_tables.insert(table.clone());

                // Skip excluded tables
                if self.plan.excluded_tables.contains(&table) {
                    continue;
                }

                // Fetch rows matching the FK values
                let rows = self.fetch_by_column(&table, &column, &fk_values)?;

                if rows.is_empty() {
                    continue;
                }

                // Filter out already visited rows
                let pks = self.extract_primary_keys(&table, &rows)?;
                let visited_set = visited.entry(table.clone()).or_default();

                let new_rows: Vec<Row> = rows
                    .into_iter()
                    .zip(pks.iter())
                    .filter(|(_, pk)| {
                        let pk_bytes = self.serialize_pk(pk);
                        if visited_set.contains(&pk_bytes) {
                            false
                        } else {
                            visited_set.insert(pk_bytes);
                            true
                        }
                    })
                    .map(|(row, _)| row)
                    .collect();

                if new_rows.is_empty() {
                    continue;
                }

                // Queue related tables based on how we reached this table:
                // - If reached via backward traversal: continue bidirectional (following ownership)
                // - If reached via forward traversal: forward only (don't reverse direction)
                self.queue_related_tables_directional(
                    &table,
                    &new_rows,
                    &mut queue,
                    reached_via_backward,
                )?;

                // Collect rows
                collected_rows
                    .entry(table)
                    .or_default()
                    .extend(new_rows);
            }
        }

        Ok((collected_rows, aggregate_tables))
    }

    /// Queue related tables based on FK values in the given rows
    ///
    /// If `bidirectional` is true:
    /// 1. Forward: Follow FKs from current table to referenced tables
    ///    e.g., orders.user_id -> users.id: from orders, find users
    /// 2. Backward: Find tables that reference current table
    ///    e.g., order_items.order_id -> orders.id: from orders, find order_items
    ///
    /// If `bidirectional` is false, only forward traversal is done.
    /// This prevents cycles where following backward from a "leaf" table
    /// pulls in unrelated rows through a common parent.
    ///
    /// The `reached_via_backward` flag in queued items indicates whether the
    /// target table should continue with bidirectional traversal (true) or
    /// forward-only (false).
    fn queue_related_tables_directional(
        &mut self,
        source_table: &str,
        rows: &[Row],
        queue: &mut VecDeque<(String, String, Vec<MySqlValue>, bool)>,
        bidirectional: bool,
    ) -> Result<()> {
        // Forward traversal: follow FKs FROM this table TO other tables
        // e.g., orders.user_id -> users.id: extract user_id values, find users by id
        // Tables reached via forward should NOT continue bidirectional (forward-only)
        if let Some(relations) = self.relations_by_source.get(source_table).cloned() {
            for relation in relations {
                // Extract FK column values from source rows
                let fk_values: Vec<MySqlValue> = rows
                    .iter()
                    .filter_map(|row| {
                        row.get_opt::<MySqlValue, _>(relation.from_column.as_str())
                            .and_then(|r| r.ok())
                            .filter(|v| !matches!(v, MySqlValue::NULL))
                    })
                    .collect();

                if !fk_values.is_empty() {
                    // Deduplicate FK values
                    let unique_fks = self.dedupe_values(fk_values);
                    queue.push_back((
                        relation.to_table.clone(),
                        relation.to_column.clone(),
                        unique_fks,
                        false, // reached via forward -> forward-only from here
                    ));
                }
            }
        }

        // Backward traversal: find tables that reference THIS table
        // Only done when bidirectional=true (root or tables reached via backward)
        // Tables reached via backward should continue bidirectional (follow ownership chain)
        if bidirectional {
            if let Some(relations) = self.relations_by_target.get(source_table).cloned() {
                for relation in relations {
                    // Extract PK/referenced column values from source rows
                    let pk_values: Vec<MySqlValue> = rows
                        .iter()
                        .filter_map(|row| {
                            row.get_opt::<MySqlValue, _>(relation.to_column.as_str())
                                .and_then(|r| r.ok())
                                .filter(|v| !matches!(v, MySqlValue::NULL))
                        })
                        .collect();

                    if !pk_values.is_empty() {
                        // Deduplicate values
                        let unique_pks = self.dedupe_values(pk_values);
                        // Look up the referencing table by its FK column
                        queue.push_back((
                            relation.from_table.clone(),
                            relation.from_column.clone(),
                            unique_pks,
                            true, // reached via backward -> continue bidirectional
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Deduplicate MySQL values
    fn dedupe_values(&self, values: Vec<MySqlValue>) -> Vec<MySqlValue> {
        let mut seen = HashSet::new();
        values
            .into_iter()
            .filter(|v| {
                let bytes = self.serialize_pk(&[v.clone()]);
                seen.insert(bytes)
            })
            .collect()
    }

    /// Query the root table with WHERE, ORDER BY, and LIMIT
    fn query_root_table(&mut self, aggregate: &ResolvedAggregate) -> Result<Vec<Row>> {
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

        self.conn.query_rows(&query)
    }

    /// Get primary key column names for a table
    fn get_pk_columns(&mut self, table: &str) -> Result<Vec<String>> {
        self.conn
            .get_cached_primary_key(table)
            .cloned()
            .ok_or_else(|| ServerError::NotFound(format!("Primary key for table '{}'", table)))
    }

    /// Extract primary key values from rows
    fn extract_primary_keys(&mut self, table: &str, rows: &[Row]) -> Result<Vec<Vec<MySqlValue>>> {
        let pk_columns = self.get_pk_columns(table)?;
        let mut pks = Vec::with_capacity(rows.len());

        for row in rows {
            let mut pk_values = Vec::with_capacity(pk_columns.len());
            for col in &pk_columns {
                let value: MySqlValue = row
                    .get_opt(col as &str)
                    .and_then(|r| r.ok())
                    .unwrap_or(MySqlValue::NULL);
                pk_values.push(value);
            }
            pks.push(pk_values);
        }

        Ok(pks)
    }

    /// Normalize a MySQL value to a canonical form for comparison
    /// This handles the case where MySQL returns the same value as different types
    /// (e.g., INT(1) vs Bytes("1") depending on the query)
    fn normalize_value(&self, value: &MySqlValue) -> MySqlValue {
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
    fn serialize_pk(&self, pk: &[MySqlValue]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in pk {
            // Normalize value before serialization to handle type mismatches
            let normalized = self.normalize_value(value);
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

    /// Fetch rows by column values (used for FK lookups)
    fn fetch_by_column(
        &mut self,
        table: &str,
        column: &str,
        values: &[MySqlValue],
    ) -> Result<Vec<Row>> {
        if values.is_empty() {
            return Ok(Vec::new());
        }

        // Build IN clause
        let placeholders: Vec<&str> = (0..values.len()).map(|_| "?").collect();
        let query = format!(
            "SELECT * FROM `{}` WHERE `{}` IN ({})",
            table,
            column,
            placeholders.join(", ")
        );

        // Convert values to params
        let params: Vec<MySqlValue> = values.to_vec();
        self.conn.query_rows_with_params(&query, mysql::Params::Positional(params))
    }
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
