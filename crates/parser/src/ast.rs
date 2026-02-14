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

    /// anonymize table { ... }
    Anonymize(AnonymizeBlock<'src>),

    /// exclude table
    Exclude(Spanned<&'src str>),

    /// ignore table
    Ignore(Spanned<&'src str>),

    /// aggregate name { ... }
    Aggregate(AggregateBlock<'src>),

    /// include aggregate where "condition"
    Include(IncludeStmt<'src>),

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
    Int,
    Float,
    Bool,
}

/// Literal values
#[derive(Debug, Clone, PartialEq)]
pub enum Literal<'src> {
    String(StringLiteral<'src>),
    Int(i64),
    Float(f64),
    Bool(bool),
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
    /// Plain text
    Text(&'src str),
    /// Interpolated expression {$var} or {expr}
    Interpolation(Spanned<Expr<'src>>),
}

/// Faker declaration
#[derive(Debug, Clone)]
pub struct FakerDecl<'src> {
    pub name: Spanned<&'src str>,
    pub values: Vec<Spanned<StringLiteral<'src>>>,
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

/// Include statement
#[derive(Debug, Clone)]
pub struct IncludeStmt<'src> {
    pub aggregate: Spanned<&'src str>,
    pub where_clause: Spanned<StringLiteral<'src>>,
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
