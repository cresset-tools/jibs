//! Execution plan types - resolved DSL for server execution

use bincode::{Decode, Encode};
use std::collections::{HashMap, HashSet};

/// Compression mode for data transfer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CompressionMode {
    /// No compression
    None,
    /// Zstd compression
    Zstd,
    /// Auto-detect (benchmark both, pick faster)
    Auto,
}

impl Default for CompressionMode {
    fn default() -> Self {
        Self::Auto
    }
}

/// A fully resolved execution plan sent to the server
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ExecutionPlan {
    /// All resolved variables
    pub variables: HashMap<String, Value>,
    /// Relations defining the dependency graph
    pub relations: Vec<Relation>,
    /// Aggregates to fetch (with resolved where clauses)
    pub aggregates: Vec<ResolvedAggregate>,
    /// Tables to exclude entirely (no data, but structure preserved)
    pub excluded_tables: HashSet<String>,
    /// Tables to ignore (no structure either)
    pub ignored_tables: HashSet<String>,
    /// Anonymization rules per table
    pub anonymization: HashMap<String, Vec<AnonymizeRule>>,
    /// Faker pools (resolved values)
    pub fakers: HashMap<String, Vec<String>>,
    /// Preserve rules
    pub preserves: Vec<PreserveRule>,
    /// Set blocks (post-processing)
    pub sets: Vec<SetRule>,
    /// After blocks (raw SQL to run post-import)
    ///
    /// Statements are executed in order. When imports are used, the order is
    /// depth-first based on import position:
    /// 1. First import's after statements (including its nested imports, depth-first)
    /// 2. Second import's after statements (including its nested imports)
    /// 3. ... and so on for each import in the file
    /// 4. Current file's after statements (top to bottom)
    ///
    /// This means imports are "hoisted" - their after statements run before
    /// the importing file's after statements.
    pub after_statements: Vec<String>,
}

impl ExecutionPlan {
    /// Create a new empty execution plan
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
            relations: Vec::new(),
            aggregates: Vec::new(),
            excluded_tables: HashSet::new(),
            ignored_tables: HashSet::new(),
            anonymization: HashMap::new(),
            fakers: HashMap::new(),
            preserves: Vec::new(),
            sets: Vec::new(),
            after_statements: Vec::new(),
        }
    }
}

impl Default for ExecutionPlan {
    fn default() -> Self {
        Self::new()
    }
}

/// A resolved value (after variable interpolation)
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
}

impl Value {
    /// Convert the value to a string representation
    pub fn as_string(&self) -> String {
        match self {
            Value::String(s) => s.clone(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "NULL".to_string(),
        }
    }

    /// Try to get the value as an i64
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to get the value as a bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

/// A relation between two tables
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Relation {
    /// Source table.column
    pub from_table: String,
    pub from_column: String,
    /// Target table.column (typically the PK)
    pub to_table: String,
    pub to_column: String,
}

/// A resolved aggregate definition
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ResolvedAggregate {
    /// Name of the aggregate
    pub name: String,
    /// Root table to start traversal from
    pub root_table: String,
    /// Resolved WHERE clause (SQL fragment)
    pub where_clause: Option<String>,
    /// ORDER BY column
    pub order_by: Option<String>,
    /// Sort direction
    pub order_direction: Option<SortDirection>,
    /// LIMIT value
    pub limit: Option<i64>,
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Rule for preserving data in a table
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PreserveRule {
    /// Table to preserve rows in
    pub table: String,
    /// Resolved WHERE clause
    pub where_clause: String,
}

/// Anonymization rule for a column
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct AnonymizeRule {
    /// Column to anonymize
    pub column: String,
    /// Anonymization target
    pub target: AnonymizeTarget,
}

/// What to replace the column value with
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AnonymizeTarget {
    /// Use values from a faker pool
    Faker(String),
    /// Set to NULL
    Null,
}

/// Post-import SET rule
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SetRule {
    /// Table to update
    pub table: String,
    /// Match conditions (WHERE clause assignments)
    pub match_clause: Vec<Assignment>,
    /// Assignments to apply
    pub assignments: Vec<Assignment>,
}

/// Column assignment
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Assignment {
    /// Column name
    pub column: String,
    /// Value to set
    pub value: Value,
}

/// Information about a table discovered on the server
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TableInfo {
    /// Table name
    pub name: String,
    /// Estimated row count
    pub estimated_rows: u64,
    /// Primary key columns
    pub primary_key: Vec<String>,
}

/// MySQL column definition
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// MySQL type name (e.g., "VARCHAR", "INT", "DATETIME")
    pub type_name: String,
    /// Full column type (e.g., "varchar(255)", "enum('a','b')", "int unsigned")
    pub full_type: String,
    /// Maximum length (for string types)
    pub max_length: Option<u64>,
    /// Whether the column is nullable
    pub nullable: bool,
    /// Whether this is part of the primary key
    pub is_primary_key: bool,
    /// Character set (for string columns)
    pub charset: Option<String>,
    /// Collation (for string columns)
    pub collation: Option<String>,
    /// Column flags (unsigned, zerofill, etc.)
    pub flags: ColumnFlags,
}

/// Column flags from MySQL
#[derive(Debug, Clone, Default, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ColumnFlags {
    pub unsigned: bool,
    pub zerofill: bool,
    pub binary: bool,
    pub auto_increment: bool,
}
