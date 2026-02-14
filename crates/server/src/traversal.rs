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

    /// Traverse from an aggregate root and stream data
    pub fn traverse_and_stream<W: Write>(
        &mut self,
        aggregate: &ResolvedAggregate,
        _resume_from: Option<Checkpoint>,
        compression: CompressionMode,
        writer: &mut W,
    ) -> Result<()> {
        // Track visited rows per table to avoid duplicates
        // Key: table name, Value: set of serialized PK values
        let mut visited: HashMap<String, HashSet<Vec<u8>>> = HashMap::new();

        // Rows to process, grouped by table
        // Key: table name, Value: list of rows
        let mut collected_rows: HashMap<String, Vec<Row>> = HashMap::new();

        // BFS queue: (table_name, pk_values to fetch)
        let mut queue: VecDeque<(String, Vec<Vec<MySqlValue>>)> = VecDeque::new();

        // Start with root table query
        let root_rows = self.query_root_table(aggregate)?;

        if !root_rows.is_empty() {
            // Get PKs from root rows for relation traversal
            let root_pks = self.extract_primary_keys(&aggregate.root_table, &root_rows)?;

            // Mark as visited and collect
            for pk in root_pks.iter() {
                let pk_bytes = self.serialize_pk(pk);
                visited
                    .entry(aggregate.root_table.clone())
                    .or_default()
                    .insert(pk_bytes);
            }
            collected_rows.insert(aggregate.root_table.clone(), root_rows);

            // Add related tables to queue
            if let Some(relations) = self.relations_by_source.get(&aggregate.root_table) {
                for relation in relations {
                    let fk_values = self.collect_fk_values(&aggregate.root_table, relation, &root_pks)?;
                    if !fk_values.is_empty() {
                        queue.push_back((relation.to_table.clone(), fk_values));
                    }
                }
            }
        }

        // BFS traversal
        while let Some((table, pk_values)) = queue.pop_front() {
            // Skip excluded tables
            if self.plan.excluded_tables.contains(&table) {
                continue;
            }

            // Filter out already visited PKs
            let visited_set = visited.entry(table.clone()).or_default();
            let new_pks: Vec<Vec<MySqlValue>> = pk_values
                .into_iter()
                .filter(|pk| {
                    let pk_bytes = self.serialize_pk(pk);
                    !visited_set.contains(&pk_bytes)
                })
                .collect();

            if new_pks.is_empty() {
                continue;
            }

            // Fetch rows for these PKs
            let rows = self.fetch_by_primary_keys(&table, &new_pks)?;

            // Mark as visited
            for pk in &new_pks {
                let pk_bytes = self.serialize_pk(pk);
                visited_set.insert(pk_bytes);
            }

            if !rows.is_empty() {
                // Extract PKs from fetched rows
                let fetched_pks = self.extract_primary_keys(&table, &rows)?;

                // Find related tables and add to queue
                if let Some(relations) = self.relations_by_source.get(&table) {
                    for relation in relations {
                        let fk_values = self.collect_fk_values(&table, relation, &fetched_pks)?;
                        if !fk_values.is_empty() {
                            queue.push_back((relation.to_table.clone(), fk_values));
                        }
                    }
                }

                // Collect rows
                collected_rows
                    .entry(table)
                    .or_default()
                    .extend(rows);
            }
        }

        // Now stream the collected data table by table
        for (table, rows) in collected_rows {
            self.stream_table_data(&table, rows, compression, writer)?;
        }

        Ok(())
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

    /// Serialize a primary key to bytes for deduplication
    fn serialize_pk(&self, pk: &[MySqlValue]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in pk {
            match value {
                MySqlValue::NULL => bytes.push(0),
                MySqlValue::Int(i) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&i.to_le_bytes());
                }
                MySqlValue::UInt(u) => {
                    bytes.push(2);
                    bytes.extend_from_slice(&u.to_le_bytes());
                }
                MySqlValue::Bytes(b) => {
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
                    bytes.push(*m);
                    bytes.push(*d);
                    bytes.push(*h);
                    bytes.push(*mi);
                    bytes.push(*s);
                    bytes.extend_from_slice(&us.to_le_bytes());
                }
                MySqlValue::Time(neg, d, h, m, s, us) => {
                    bytes.push(7);
                    bytes.push(if *neg { 1 } else { 0 });
                    bytes.extend_from_slice(&d.to_le_bytes());
                    bytes.push(*h);
                    bytes.push(*m);
                    bytes.push(*s);
                    bytes.extend_from_slice(&us.to_le_bytes());
                }
            }
        }
        bytes
    }

    /// Collect foreign key values from rows to fetch related records
    fn collect_fk_values(
        &self,
        _source_table: &str,
        _relation: &Relation,
        _source_pks: &[Vec<MySqlValue>],
    ) -> Result<Vec<Vec<MySqlValue>>> {
        // For now, we assume single-column FKs
        // The FK column in the source table points to the PK in the target table
        // We need to collect unique FK values from the source rows

        // Note: This is simplified - we'd need the actual row data to extract FK values
        // In a full implementation, we'd query for the FK column values
        Ok(Vec::new()) // Placeholder - full implementation below
    }

    /// Fetch rows by primary key values
    fn fetch_by_primary_keys(
        &mut self,
        table: &str,
        pks: &[Vec<MySqlValue>],
    ) -> Result<Vec<Row>> {
        if pks.is_empty() {
            return Ok(Vec::new());
        }

        let pk_columns = self.get_pk_columns(table)?;

        if pk_columns.len() == 1 {
            // Simple case: single-column PK
            let col = &pk_columns[0];
            let values: Vec<&MySqlValue> = pks.iter().map(|pk| &pk[0]).collect();

            // Build IN clause
            let placeholders: Vec<&str> = (0..values.len()).map(|_| "?").collect();
            let query = format!(
                "SELECT * FROM `{}` WHERE `{}` IN ({})",
                table,
                col,
                placeholders.join(", ")
            );

            // Convert values to params
            let params: Vec<MySqlValue> = values.into_iter().cloned().collect();
            self.conn.query_rows_with_params(&query, mysql::Params::Positional(params))
        } else {
            // Composite PK - use OR conditions
            let mut conditions = Vec::new();
            let mut all_params = Vec::new();

            for pk in pks {
                let mut condition_parts = Vec::new();
                for (col, val) in pk_columns.iter().zip(pk.iter()) {
                    condition_parts.push(format!("`{}` = ?", col));
                    all_params.push(val.clone());
                }
                conditions.push(format!("({})", condition_parts.join(" AND ")));
            }

            let query = format!(
                "SELECT * FROM `{}` WHERE {}",
                table,
                conditions.join(" OR ")
            );

            self.conn.query_rows_with_params(&query, mysql::Params::Positional(all_params))
        }
    }

    /// Stream table data to the writer
    fn stream_table_data<W: Write>(
        &mut self,
        table: &str,
        rows: Vec<Row>,
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
        let mut row_count = 0u32;
        let mut total_rows = 0u64;
        let mut checkpoint = Checkpoint::new(&self.plan.aggregates.first().map(|a| a.name.clone()).unwrap_or_default());

        for row in rows {
            tsv_writer.write_row(&row)?;
            row_count += 1;
            total_rows += 1;

            // Check if we should flush a chunk
            if row_count >= CHUNK_ROW_LIMIT as u32 || tsv_writer.buffer_size() >= CHUNK_BYTE_LIMIT {
                let tsv_data = tsv_writer.take_buffer();
                checkpoint.rows_transferred = total_rows;
                checkpoint.bytes_transferred += tsv_data.len() as u64;

                let data_msg = ServerMessage::Data {
                    table: table.to_string(),
                    row_count,
                    tsv_data: maybe_compress(tsv_data, compression),
                    checkpoint: checkpoint.clone(),
                };
                write_message(writer, &data_msg)?;
                row_count = 0;
            }
        }

        // Flush remaining data
        if !tsv_writer.is_empty() {
            let tsv_data = tsv_writer.take_buffer();
            checkpoint.rows_transferred = total_rows;
            checkpoint.bytes_transferred += tsv_data.len() as u64;

            let data_msg = ServerMessage::Data {
                table: table.to_string(),
                row_count,
                tsv_data: maybe_compress(tsv_data, compression),
                checkpoint: checkpoint.clone(),
            };
            write_message(writer, &data_msg)?;
        }

        // Send TableDone
        let done_msg = ServerMessage::TableDone {
            table: table.to_string(),
            row_count: total_rows,
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
