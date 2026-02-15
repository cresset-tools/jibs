//! Dependency graph traversal for collecting related rows

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;

use mysql::{Row, Value as MySqlValue};

use jibs_protocol::{
    framing::write_message, Checkpoint, CompressionMode, ExecutionPlan, Relation,
    ResolvedAggregate, ServerMessage, SortDirection,
};

use crate::error::{Result, ServerError};
use crate::mysql::MySqlConnection;
use crate::tsv::TsvWriter;

/// Maximum rows per chunk
const CHUNK_ROW_LIMIT: usize = 10_000;
/// Maximum bytes per chunk
const CHUNK_BYTE_LIMIT: usize = 10 * 1024 * 1024; // 10MB

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

    /// Stream all tables, with aggregates providing filtered subsets
    ///
    /// Logic:
    /// 1. Tables touched by aggregates: only include rows reachable from aggregate
    /// 2. Tables NOT touched by aggregates: include ALL rows
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
        // Initialize checkpoint (either from resume or fresh)
        let mut checkpoint = resume_from.unwrap_or_default();

        // First, collect rows from all aggregates
        let (aggregate_rows, aggregate_tables) = self.collect_aggregate_rows()?;

        // Get all tables in the database
        let all_tables: Vec<String> = self.conn.get_all_table_names()?;

        // Stream tables
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

            checkpoint.start_table(&table);

            if aggregate_tables.contains(&table) {
                // This table is filtered by an aggregate - use collected rows
                if let Some(rows) = aggregate_rows.get(&table) {
                    if !rows.is_empty() {
                        self.stream_table_data(&table, rows.clone(), &mut checkpoint, compression, writer)?;
                    }
                }
            } else {
                // This table is not filtered - stream ALL rows
                self.stream_full_table(&table, &mut checkpoint, compression, writer)?;
            }

            checkpoint.complete_table(&table);
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

    /// Stream an entire table (no filtering)
    fn stream_full_table<W: Write>(
        &mut self,
        table: &str,
        checkpoint: &mut Checkpoint,
        compression: CompressionMode,
        writer: &mut W,
    ) -> Result<()> {
        // Query all rows from the table
        let query = format!("SELECT * FROM `{}`", table);
        let rows = self.conn.query_rows(&query)?;

        if !rows.is_empty() {
            self.stream_table_data(table, rows, checkpoint, compression, writer)?;
        }

        Ok(())
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

    /// Stream table data to the writer
    fn stream_table_data<W: Write>(
        &mut self,
        table: &str,
        rows: Vec<Row>,
        checkpoint: &mut Checkpoint,
        compression: CompressionMode,
        writer: &mut W,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        // Get schema and send it
        let columns = self.conn.get_column_defs(table)?;
        let column_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

        let schema_msg = ServerMessage::Schema {
            table: table.to_string(),
            columns: columns.clone(),
        };
        write_message(writer, &schema_msg)?;

        // Get anonymization rules for this table
        let anonymization = self
            .plan
            .anonymization
            .get(table)
            .cloned()
            .unwrap_or_default();

        // Create TSV writer
        let mut tsv_writer = TsvWriter::new(
            column_names,
            anonymization,
            self.plan.fakers.clone(),
        );

        // Write rows in chunks
        let mut chunk_row_count = 0u32;
        let mut table_row_count = 0u64;

        for row in rows {
            tsv_writer.write_row(&row)?;
            chunk_row_count += 1;
            table_row_count += 1;
            checkpoint.rows_transferred += 1;

            // Check if we should flush a chunk
            if chunk_row_count >= CHUNK_ROW_LIMIT as u32 || tsv_writer.buffer_size() >= CHUNK_BYTE_LIMIT {
                let tsv_data = tsv_writer.take_buffer();
                checkpoint.bytes_transferred += tsv_data.len() as u64;

                let data_msg = ServerMessage::Data {
                    table: table.to_string(),
                    row_count: chunk_row_count,
                    tsv_data: maybe_compress(tsv_data, compression),
                    checkpoint: checkpoint.clone(),
                };
                write_message(writer, &data_msg)?;
                chunk_row_count = 0;
            }
        }

        // Flush remaining data
        if !tsv_writer.is_empty() {
            let tsv_data = tsv_writer.take_buffer();
            checkpoint.bytes_transferred += tsv_data.len() as u64;

            let data_msg = ServerMessage::Data {
                table: table.to_string(),
                row_count: chunk_row_count,
                tsv_data: maybe_compress(tsv_data, compression),
                checkpoint: checkpoint.clone(),
            };
            write_message(writer, &data_msg)?;
        }

        // Send TableDone
        let done_msg = ServerMessage::TableDone {
            table: table.to_string(),
            row_count: table_row_count,
        };
        write_message(writer, &done_msg)?;

        Ok(())
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
