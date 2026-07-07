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
    /// Regex patterns for tables to import in full (expanded server-side)
    pub full_patterns: Vec<String>,
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
    /// When true, only import aggregate-related tables (skip all full-table imports).
    /// Used by the `get` command to fetch only specific aggregates.
    pub aggregates_only: bool,
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
            full_patterns: Vec::new(),
            preserves: Vec::new(),
            sets: Vec::new(),
            after_statements: Vec::new(),
            aggregates_only: false,
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
    /// Interned table ID (u16) — used in all subsequent protocol messages
    pub table_id: u16,
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

impl ColumnDef {
    /// True for column types whose values are raw bytes rather than text.
    ///
    /// Values of these types are hex-encoded in the TSV stream by the server
    /// and decoded with UNHEX() in the client's LOAD DATA statement, so
    /// arbitrary binary data survives the transfer. Both sides must agree on
    /// this classification, which is why it lives in the shared protocol crate.
    pub fn is_binary_type(&self) -> bool {
        matches!(
            self.type_name.as_str(),
            "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BIT"
        )
    }
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

/// A foreign key constraint captured from the *source* database so a load or
/// import into a **fresh** database can reconstruct it. jibs otherwise only
/// preserves the *target's* pre-existing FKs across a reload, so a load into an
/// empty schema would end up with none.
///
/// The referenced table is resolved in the current (target) schema — no schema
/// qualifier is carried, so the source's database name never leaks into the
/// recreated constraint.
#[derive(Debug, Clone, Default, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ForeignKeyDef {
    /// Table the constraint lives on.
    pub table: String,
    /// Constraint name.
    pub constraint: String,
    /// Referencing columns, ordered by `ORDINAL_POSITION` (composite FKs keep
    /// their column order).
    pub columns: Vec<String>,
    /// Referenced table (resolved in the target schema).
    pub ref_table: String,
    /// Referenced columns, aligned with `columns`.
    pub ref_columns: Vec<String>,
    /// `ON UPDATE` action (`RESTRICT` / `CASCADE` / `SET NULL` / `NO ACTION`).
    pub update_rule: String,
    /// `ON DELETE` action.
    pub delete_rule: String,
}

/// A secondary index on a table. The PRIMARY KEY is never represented here — it
/// is emitted from [`ColumnDef::is_primary_key`]; this covers every other index
/// (unique and non-unique), which the loader would otherwise drop.
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IndexDef {
    /// Index name (`Key_name` from `SHOW INDEX`).
    pub name: String,
    /// Key parts, ordered by `Seq_in_index`.
    pub columns: Vec<IndexColumn>,
    /// True when `Non_unique = 0`.
    pub unique: bool,
    /// Index method / kind.
    pub kind: IndexKind,
}

/// One key part of an [`IndexDef`].
#[derive(Debug, Clone, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct IndexColumn {
    /// Column name (ignored when `expression` is set).
    pub name: String,
    /// Prefix length (`Sub_part`), e.g. `col`(255) — `None` for a full-column key.
    pub prefix_len: Option<u32>,
    /// Descending key part (`Collation = 'D'`).
    pub descending: bool,
    /// Functional/expression key part; when `Some`, emitted as `((expr))` and
    /// `name` is ignored.
    pub expression: Option<String>,
}

/// Index method. Non-BTree kinds need an explicit keyword in the DDL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum IndexKind {
    #[default]
    BTree,
    Fulltext,
    Spatial,
    Hash,
}

/// Table-level options needed to reproduce a `CREATE TABLE` faithfully. Without
/// these the recreated table silently inherits the server/database defaults
/// (notably a different collation).
#[derive(Debug, Clone, Default, Encode, Decode)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TableOptions {
    /// Storage engine (e.g. `InnoDB`).
    pub engine: Option<String>,
    /// Default character set (e.g. `utf8mb4`).
    pub charset: Option<String>,
    /// Default collation (e.g. `utf8mb4_general_ci`).
    pub collation: Option<String>,
    /// Row format, when the table pins one (e.g. `DYNAMIC`).
    pub row_format: Option<String>,
}
