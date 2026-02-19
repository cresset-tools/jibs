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
    /// Regex patterns for tables to exclude (expanded server-side)
    pub excluded_patterns: Vec<String>,
    /// Tables to ignore (no structure either)
    pub ignored_tables: HashSet<String>,
    /// Regex patterns for tables to ignore (expanded server-side)
    pub ignored_patterns: Vec<String>,
    /// Relations to ignore (filter out from auto-discovered FKs)
    pub ignored_relations: Vec<Relation>,
    /// Anonymization rules per table
    pub anonymization: HashMap<String, Vec<AnonymizeRule>>,
    /// Faker pools (resolved values)
    pub fakers: HashMap<String, Vec<String>>,
    /// Preserve rules
    pub preserves: Vec<PreserveRule>,
    /// Set blocks (post-processing)
    pub sets: Vec<SetRule>,
    /// Tables to import in full (skip BFS filtering)
    pub full_tables: HashSet<String>,
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
            excluded_patterns: Vec::new(),
            ignored_tables: HashSet::new(),
            ignored_patterns: Vec::new(),
            ignored_relations: Vec::new(),
            anonymization: HashMap::new(),
            fakers: HashMap::new(),
            full_tables: HashSet::new(),
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
    StringArray(Vec<String>),
    Int(i64),
    IntArray(Vec<i64>),
    Float(f64),
    FloatArray(Vec<f64>),
    Bool(bool),
    BoolArray(Vec<bool>),
    Null,
}

/// Macro to generate array accessor methods for Value
macro_rules! impl_value_array_accessor {
    ($method:ident, $variant:ident, $inner:ty) => {
        pub fn $method(&self) -> Option<&[$inner]> {
            match self {
                Value::$variant(arr) => Some(arr),
                _ => None,
            }
        }
    };
}

/// Macro to generate scalar accessor methods for Value
macro_rules! impl_value_scalar_accessor {
    ($method:ident, $variant:ident, $inner:ty) => {
        pub fn $method(&self) -> Option<$inner> {
            match self {
                Value::$variant(v) => Some(*v),
                _ => None,
            }
        }
    };
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
            // Arrays formatted as [elem1, elem2, ...]
            Value::StringArray(arr) => format!("[{}]", arr.join(", ")),
            Value::IntArray(arr) => Self::format_array(arr),
            Value::FloatArray(arr) => Self::format_array(arr),
            Value::BoolArray(arr) => Self::format_array(arr),
        }
    }

    /// Format an array of displayable items
    fn format_array<T: std::fmt::Display>(arr: &[T]) -> String {
        format!(
            "[{}]",
            arr.iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    // Scalar accessors
    impl_value_scalar_accessor!(as_int, Int, i64);
    impl_value_scalar_accessor!(as_bool, Bool, bool);

    // Array accessors
    impl_value_array_accessor!(as_string_array, StringArray, String);
    impl_value_array_accessor!(as_int_array, IntArray, i64);
    impl_value_array_accessor!(as_float_array, FloatArray, f64);
    impl_value_array_accessor!(as_bool_array, BoolArray, bool);
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
    /// Tables to skip during BFS traversal
    pub exclude_tables: Vec<String>,
    /// Regex patterns for tables to skip during BFS traversal (expanded server-side)
    pub exclude_patterns: Vec<String>,
    /// If true, only import the root table (no BFS traversal)
    pub root_only: bool,
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
