//! Abstract Syntax Tree definitions for the MySQL Import DSL

use crate::Span;

/// A spanned value - wraps a value with its source location
pub type Spanned<T> = (T, Span);

/// The root of the AST - a program is a list of statements
#[derive(Debug, Clone)]
pub struct Program<'src> {
    pub statements: Vec<Spanned<Statement<'src>>>,
}

/// A statement, optionally prefixed with a #[when()] attribute
#[derive(Debug, Clone)]
pub struct Statement<'src> {
    pub attribute: Option<Spanned<Expr<'src>>>,
    pub kind: StatementKind<'src>,
}

/// The different kinds of statements
#[derive(Debug, Clone)]
pub enum StatementKind<'src> {
    /// import "path"
    Import(Spanned<&'src str>),

    /// var name: type = default
    Var(VarDecl<'src>),

    /// faker name ["value1", "value2"]
    Faker(FakerDecl<'src>),

    /// relation table.column -> table.column
    Relation(RelationDecl<'src>),

    /// ignore_relation table.column -> table.column
    IgnoreRelation(RelationDecl<'src>),

    /// anonymize table { ... }
    Anonymize(AnonymizeBlock<'src>),

    /// exclude table or /pattern/
    Exclude(TablePattern<'src>),

    /// ignore table or /pattern/
    Ignore(TablePattern<'src>),

    /// full table1, table2, /pattern/, ...
    Full(Vec<TablePattern<'src>>),

    /// aggregate name { ... }
    Aggregate(AggregateBlock<'src>),

    /// get function definition
    Get(GetFunctionDef<'src>),

    /// preserve table where "condition"
    Preserve(PreserveStmt<'src>),

    /// set table { ... }
    Set(SetBlock<'src>),

    /// after { ... }
    After(AfterBlock<'src>),
}

/// Variable declaration
#[derive(Debug, Clone)]
pub struct VarDecl<'src> {
    pub name: Spanned<&'src str>,
    pub var_type: Spanned<VarType>,
    pub default: Option<Spanned<Literal<'src>>>,
}

/// Variable types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VarType {
    String,
    StringArray,
    Int,
    IntArray,
    Float,
    FloatArray,
    Bool,
    BoolArray,
}

/// Literal values
#[derive(Debug, Clone, PartialEq)]
pub enum Literal<'src> {
    String(StringLiteral<'src>),
    StringArray(Vec<Spanned<StringLiteral<'src>>>),
    Int(i64),
    IntArray(Vec<i64>),
    Float(f64),
    FloatArray(Vec<f64>),
    Bool(bool),
    BoolArray(Vec<bool>),
    Null,
}

/// A string literal, possibly with interpolations
#[derive(Debug, Clone, PartialEq)]
pub struct StringLiteral<'src> {
    pub parts: Vec<StringPart<'src>>,
}

/// Part of an interpolated string
#[derive(Debug, Clone, PartialEq)]
pub enum StringPart<'src> {
    /// Plain text (owned when escape sequences were decoded)
    Text(std::borrow::Cow<'src, str>),
    /// Interpolated expression {$var} or {expr}
    Interpolation(Spanned<Expr<'src>>),
}

/// Faker declaration
#[derive(Debug, Clone)]
pub struct FakerDecl<'src> {
    pub name: Spanned<&'src str>,
    pub source: FakerSource<'src>,
}

/// A value in a faker list - either a string literal or a spread variable
#[derive(Debug, Clone, PartialEq)]
pub enum FakerValue<'src> {
    /// A regular string literal (possibly with interpolation)
    Literal(StringLiteral<'src>),
    /// A spread of a string array variable: ...$variable
    Spread(&'src str),
}

/// The source of faker values - either an inline array or a variable reference
#[derive(Debug, Clone)]
pub enum FakerSource<'src> {
    /// Inline array of values: ["a", "b", ...$var]
    Array(Vec<Spanned<FakerValue<'src>>>),
    /// Direct variable reference: $emails
    Variable(&'src str),
}

/// Relation declaration
#[derive(Debug, Clone)]
pub struct RelationDecl<'src> {
    pub from: Spanned<ColumnRef<'src>>,
    pub to: Spanned<ColumnRef<'src>>,
}

/// Column reference (table.column)
#[derive(Debug, Clone)]
pub struct ColumnRef<'src> {
    pub table: &'src str,
    pub column: &'src str,
}

/// Anonymize block
#[derive(Debug, Clone)]
pub struct AnonymizeBlock<'src> {
    pub table: Spanned<&'src str>,
    pub rules: Vec<Spanned<AnonymizeRule<'src>>>,
}

/// Anonymize rule
#[derive(Debug, Clone)]
pub struct AnonymizeRule<'src> {
    pub column: Spanned<&'src str>,
    pub target: AnonymizeTarget<'src>,
}

/// Target for anonymization
#[derive(Debug, Clone)]
pub enum AnonymizeTarget<'src> {
    /// Use a faker
    Faker(Spanned<&'src str>),
    /// Set to NULL
    Null,
}

/// Aggregate block
#[derive(Debug, Clone)]
pub struct AggregateBlock<'src> {
    pub name: Spanned<&'src str>,
    pub root: Spanned<&'src str>,
    pub where_clause: Option<Spanned<StringLiteral<'src>>>,
    pub order_by: Option<OrderByClause<'src>>,
    pub limit: Option<Spanned<LimitValue<'src>>>,
    pub exclude_tables: Vec<TablePattern<'src>>,
    pub root_only: bool,
}

/// Order by clause
#[derive(Debug, Clone)]
pub struct OrderByClause<'src> {
    pub column: Spanned<&'src str>,
    pub direction: Option<SortDirection>,
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Limit value - either a literal or a variable reference
#[derive(Debug, Clone)]
pub enum LimitValue<'src> {
    Literal(i64),
    Variable(&'src str),
}

/// Get function definition
#[derive(Debug, Clone)]
pub struct GetFunctionDef<'src> {
    pub name: Spanned<&'src str>,
    pub params: Vec<Spanned<VarDecl<'src>>>,
    pub aggregate: Spanned<&'src str>,
    pub where_clause: Option<Spanned<StringLiteral<'src>>>,
    pub order_by: Option<OrderByClause<'src>>,
    pub limit: Option<Spanned<LimitValue<'src>>>,
    pub exclude_tables: Vec<TablePattern<'src>>,
    pub root_only: bool,
}

/// Preserve statement
#[derive(Debug, Clone)]
pub struct PreserveStmt<'src> {
    pub table: Spanned<&'src str>,
    pub where_clause: Spanned<StringLiteral<'src>>,
}

/// Set block
#[derive(Debug, Clone)]
pub struct SetBlock<'src> {
    pub table: Spanned<&'src str>,
    pub match_clause: Vec<Spanned<Assignment<'src>>>,
    pub assignments: Vec<Spanned<Assignment<'src>>>,
}

/// Assignment (column = value)
#[derive(Debug, Clone)]
pub struct Assignment<'src> {
    pub column: Spanned<&'src str>,
    pub value: Spanned<Value<'src>>,
}

/// A value in an assignment
#[derive(Debug, Clone)]
pub enum Value<'src> {
    Literal(Literal<'src>),
    Variable(&'src str),
    Expr(Expr<'src>),
}

/// After block
#[derive(Debug, Clone)]
pub struct AfterBlock<'src> {
    pub statements: Vec<Spanned<&'src str>>,
}

/// A table name pattern - either an exact name or a regex
#[derive(Debug, Clone)]
pub enum TablePattern<'src> {
    /// Exact table name match
    Exact(Spanned<&'src str>),
    /// Regex pattern match
    Regex(Spanned<&'src str>),
}

/// Expression (used in conditionals and interpolation)
#[derive(Debug, Clone, PartialEq)]
pub enum Expr<'src> {
    /// Literal value
    Literal(Literal<'src>),
    /// Variable reference ($name)
    Variable(&'src str),
    /// Binary operation
    Binary(Box<Spanned<Expr<'src>>>, BinaryOp, Box<Spanned<Expr<'src>>>),
    /// Unary operation
    Unary(UnaryOp, Box<Spanned<Expr<'src>>>),
    /// unique() — generates a unique counter value (used in faker string interpolation)
    Unique,
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // Comparison
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    // Logical
    And,
    Or,
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}
