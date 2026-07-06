//! Parser for the MySQL Import DSL (hand-written recursive descent)
//!
//! Statements are dispatched on their leading keyword; on a statement-level
//! error the parser records it and re-synchronizes at the next statement
//! keyword, so one bad statement doesn't hide errors in the rest of the file.

use crate::ast::*;
use crate::interpolation::parse_interpolated_string;
use crate::lexer::{token_as_ident, Token};
use crate::{ParseError, Span};

/// Parse a token stream into a Program. Always returns whatever statements
/// parsed successfully, plus the list of errors encountered.
pub fn parse_tokens<'src>(
    tokens: &[Spanned<Token<'src>>],
    eoi: Span,
) -> (Program<'src>, Vec<ParseError>) {
    let mut parser = Parser::new(tokens, eoi);
    let program = parser.program();
    (program, parser.errors)
}

/// Parse a single expression from a token stream, requiring all tokens to be
/// consumed. Used for interpolation expressions ({...} inside strings).
pub(crate) fn parse_expr_tokens<'src>(
    tokens: &[Spanned<Token<'src>>],
    eoi: Span,
) -> Result<Spanned<Expr<'src>>, ParseError> {
    let mut parser = Parser::new(tokens, eoi);
    parser.in_interpolation = true;
    let expr = parser.expr()?;
    if !parser.at_end() {
        return Err(parser.err_here("expected end of expression"));
    }
    Ok(expr)
}

struct Parser<'tokens, 'src> {
    tokens: &'tokens [Spanned<Token<'src>>],
    pos: usize,
    eoi: Span,
    errors: Vec<ParseError>,
    /// Inside a string interpolation: keywords act as identifiers after `$`,
    /// and `unique()` is a valid expression.
    in_interpolation: bool,
}

/// Tokens that can start a statement — used for error recovery
fn starts_statement(token: &Token) -> bool {
    matches!(
        token,
        Token::Hash
            | Token::Import
            | Token::Var
            | Token::Faker
            | Token::Relation
            | Token::IgnoreRelation
            | Token::Anonymize
            | Token::ExcludeData
            | Token::IgnoreTable
            | Token::Full
            | Token::Aggregate
            | Token::Get
            | Token::Preserve
            | Token::Set
            | Token::After
    )
}

impl<'tokens, 'src> Parser<'tokens, 'src> {
    fn new(tokens: &'tokens [Spanned<Token<'src>>], eoi: Span) -> Self {
        Self { tokens, pos: 0, eoi, errors: Vec::new(), in_interpolation: false }
    }

    // ------------------------------------------------------------------
    // Cursor primitives
    // ------------------------------------------------------------------

    fn at_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn peek(&self) -> Option<&Token<'src>> {
        self.tokens.get(self.pos).map(|(t, _)| t)
    }

    fn peek2(&self) -> Option<&Token<'src>> {
        self.tokens.get(self.pos + 1).map(|(t, _)| t)
    }

    /// Span of the current token, or the end-of-input span
    fn cur_span(&self) -> Span {
        self.tokens.get(self.pos).map(|(_, s)| *s).unwrap_or(self.eoi)
    }

    /// End offset of the last consumed token
    fn prev_end(&self) -> usize {
        if self.pos == 0 {
            self.cur_span().start
        } else {
            self.tokens[self.pos - 1].1.end
        }
    }

    fn bump(&mut self) -> Option<Spanned<Token<'src>>> {
        let tok = self.tokens.get(self.pos).cloned();
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    /// Consume the next token if it equals `token`
    fn eat(&mut self, token: &Token<'src>) -> bool {
        if self.peek() == Some(token) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn found(&self) -> String {
        match self.peek() {
            Some(t) => format!("'{}'", t),
            None => "end of input".to_string(),
        }
    }

    fn err_here(&self, expected: &str) -> ParseError {
        ParseError {
            span: self.cur_span().into_range(),
            message: format!("found {}, expected {}", self.found(), expected),
        }
    }

    fn expect(&mut self, token: Token<'src>, expected: &str) -> Result<Span, ParseError> {
        if self.peek() == Some(&token) {
            let span = self.cur_span();
            self.pos += 1;
            Ok(span)
        } else {
            Err(self.err_here(expected))
        }
    }

    fn expect_ident(&mut self, what: &str) -> Result<Spanned<&'src str>, ParseError> {
        match self.peek() {
            Some(Token::Ident(s)) => {
                let s = *s;
                let span = self.cur_span();
                self.pos += 1;
                Ok((s, span))
            }
            _ => Err(self.err_here(what)),
        }
    }

    /// Skip tokens until something that can start a statement (for recovery).
    /// Always makes progress.
    fn synchronize(&mut self) {
        self.bump();
        while let Some(token) = self.peek() {
            if starts_statement(token) {
                break;
            }
            self.pos += 1;
        }
    }

    // ------------------------------------------------------------------
    // Program / statements
    // ------------------------------------------------------------------

    fn program(&mut self) -> Program<'src> {
        let mut statements = Vec::new();
        while !self.at_end() {
            let start = self.cur_span().start;
            match self.statement() {
                Ok(stmt) => {
                    statements.push((stmt, Span::new(start, self.prev_end())));
                }
                Err(e) => {
                    self.errors.push(e);
                    self.synchronize();
                }
            }
        }
        Program { statements }
    }

    fn statement(&mut self) -> Result<Statement<'src>, ParseError> {
        let attribute = if self.peek() == Some(&Token::Hash) {
            Some(self.attribute()?)
        } else {
            None
        };
        let kind = self.statement_kind()?;
        Ok(Statement { attribute, kind })
    }

    /// #[when(expr)]
    fn attribute(&mut self) -> Result<Spanned<Expr<'src>>, ParseError> {
        self.expect(Token::Hash, "'#'")?;
        self.expect(Token::LBracket, "'[' after '#'")?;
        self.expect(Token::When, "'when'")?;
        self.expect(Token::LParen, "'(' after 'when'")?;
        let expr = self.expr()?;
        self.expect(Token::RParen, "')' to close the when condition")?;
        self.expect(Token::RBracket, "']' to close the attribute")?;
        Ok(expr)
    }

    fn statement_kind(&mut self) -> Result<StatementKind<'src>, ParseError> {
        match self.peek() {
            Some(Token::Import) => self.import_stmt(),
            Some(Token::Var) => self.var_stmt(),
            Some(Token::Faker) => self.faker_stmt(),
            Some(Token::Relation) => self.relation_stmt(false),
            Some(Token::IgnoreRelation) => self.relation_stmt(true),
            Some(Token::Anonymize) => self.anonymize_stmt(),
            Some(Token::ExcludeData) => {
                self.pos += 1;
                Ok(StatementKind::Exclude(self.table_pattern()?))
            }
            Some(Token::IgnoreTable) => {
                self.pos += 1;
                Ok(StatementKind::Ignore(self.table_pattern()?))
            }
            Some(Token::Full) => {
                self.pos += 1;
                Ok(StatementKind::Full(self.table_pattern_list()?))
            }
            Some(Token::Aggregate) => self.aggregate_stmt(),
            Some(Token::Get) => self.get_stmt(),
            Some(Token::Preserve) => self.preserve_stmt(),
            Some(Token::Set) => self.set_stmt(),
            Some(Token::After) => self.after_stmt(),
            _ => Err(self.err_here(
                "a statement (import, var, faker, relation, ignore_relation, anonymize, \
                 exclude_data, ignore_table, full, aggregate, get, preserve, set, or after)",
            )),
        }
    }

    /// import "path" — the path is a raw string (no interpolation)
    fn import_stmt(&mut self) -> Result<StatementKind<'src>, ParseError> {
        self.pos += 1;
        let path = self.string_raw("a file path string after 'import'")?;
        Ok(StatementKind::Import(path))
    }

    /// var name: type [= default]
    fn var_stmt(&mut self) -> Result<StatementKind<'src>, ParseError> {
        self.pos += 1;
        let decl = self.var_decl()?;
        Ok(StatementKind::Var(decl))
    }

    /// name: type [= default] — shared by `var` and get-function parameters
    fn var_decl(&mut self) -> Result<VarDecl<'src>, ParseError> {
        let name = self.expect_ident("a variable name")?;
        self.expect(Token::Colon, "':' after the variable name")?;
        let var_type = self.var_type()?;
        let default = if self.eat(&Token::Assign) {
            Some(self.literal()?)
        } else {
            None
        };
        Ok(VarDecl { name, var_type, default })
    }

    fn var_type(&mut self) -> Result<Spanned<VarType>, ParseError> {
        let start = self.cur_span();
        let scalar = match self.peek() {
            Some(Token::TypeString) => VarType::String,
            Some(Token::TypeInt) => VarType::Int,
            Some(Token::TypeFloat) => VarType::Float,
            Some(Token::TypeBool) => VarType::Bool,
            _ => return Err(self.err_here("a type (string, int, float, or bool)")),
        };
        self.pos += 1;
        let var_type = if self.eat(&Token::LBracket) {
            self.expect(Token::RBracket, "']' to close the array type")?;
            match scalar {
                VarType::String => VarType::StringArray,
                VarType::Int => VarType::IntArray,
                VarType::Float => VarType::FloatArray,
                VarType::Bool => VarType::BoolArray,
                _ => unreachable!(),
            }
        } else {
            scalar
        };
        Ok((var_type, Span::new(start.start, self.prev_end())))
    }

    /// faker name ["a", "b", ...$var] or faker name $variable
    fn faker_stmt(&mut self) -> Result<StatementKind<'src>, ParseError> {
        self.pos += 1;
        let name = self.expect_ident("a faker pool name")?;

        let source = if self.eat(&Token::Dollar) {
            let var = self.expect_ident("a variable name after '$'")?;
            FakerSource::Variable(var.0)
        } else if self.peek() == Some(&Token::LBracket) {
            self.pos += 1;
            let mut values = Vec::new();
            if self.peek() != Some(&Token::RBracket) {
                loop {
                    let start = self.cur_span();
                    let value = if self.eat(&Token::Spread) {
                        self.expect(Token::Dollar, "'$' after '...'")?;
                        let var = self.expect_ident("a variable name after '...$'")?;
                        FakerValue::Spread(var.0)
                    } else {
                        let (lit, _) = self.string_literal("a string value or ...$variable")?;
                        FakerValue::Literal(lit)
                    };
                    values.push((value, Span::new(start.start, self.prev_end())));
                    if !self.eat(&Token::Comma) {
                        break;
                    }
                    // Trailing comma allowed
                    if self.peek() == Some(&Token::RBracket) {
                        break;
                    }
                }
            }
            self.expect(Token::RBracket, "']' to close the faker value list")?;
            FakerSource::Array(values)
        } else {
            return Err(self.err_here("'[' or '$' after the faker name"));
        };

        Ok(StatementKind::Faker(FakerDecl { name, source }))
    }

    /// relation table.column -> table.column (also ignore_relation)
    fn relation_stmt(&mut self, ignore: bool) -> Result<StatementKind<'src>, ParseError> {
        self.pos += 1;
        let from = self.column_ref()?;
        self.expect(Token::Arrow, "'->' between the relation columns")?;
        let to = self.column_ref()?;
        let decl = RelationDecl { from, to };
        Ok(if ignore {
            StatementKind::IgnoreRelation(decl)
        } else {
            StatementKind::Relation(decl)
        })
    }

    fn column_ref(&mut self) -> Result<Spanned<ColumnRef<'src>>, ParseError> {
        let start = self.cur_span();
        let table = self.expect_ident("a table name")?;
        self.expect(Token::Dot, "'.' between table and column")?;
        let column = self.expect_ident("a column name")?;
        Ok((
            ColumnRef { table: table.0, column: column.0 },
            Span::new(start.start, self.prev_end()),
        ))
    }

    /// anonymize table { column -> null|faker ... }
    fn anonymize_stmt(&mut self) -> Result<StatementKind<'src>, ParseError> {
        self.pos += 1;
        let table = self.expect_ident("a table name after 'anonymize'")?;
        self.expect(Token::LBrace, "'{' to open the anonymize block")?;

        let mut rules = Vec::new();
        while let Some(Token::Ident(_)) = self.peek() {
            let start = self.cur_span();
            let column = self.expect_ident("a column name")?;
            self.expect(Token::Arrow, "'->' after the column name")?;
            let target = match self.peek() {
                Some(Token::Null) => {
                    self.pos += 1;
                    AnonymizeTarget::Null
                }
                Some(Token::Ident(_)) => AnonymizeTarget::Faker(self.expect_ident("a faker name")?),
                _ => return Err(self.err_here("'null' or a faker name after '->'")),
            };
            rules.push((
                AnonymizeRule { column, target },
                Span::new(start.start, self.prev_end()),
            ));
        }

        self.expect(Token::RBrace, "'}' to close the anonymize block")?;
        Ok(StatementKind::Anonymize(AnonymizeBlock { table, rules }))
    }

    /// A table name or /regex/ pattern
    fn table_pattern(&mut self) -> Result<TablePattern<'src>, ParseError> {
        match self.peek() {
            Some(Token::Regex(s)) => {
                let pattern = TablePattern::Regex((s, self.cur_span()));
                self.pos += 1;
                Ok(pattern)
            }
            Some(Token::Ident(_)) => Ok(TablePattern::Exact(self.expect_ident("a table name")?)),
            _ => Err(self.err_here("a table name or /regex/ pattern")),
        }
    }

    /// pattern, pattern, ... (at least one, no trailing comma)
    fn table_pattern_list(&mut self) -> Result<Vec<TablePattern<'src>>, ParseError> {
        let mut patterns = vec![self.table_pattern()?];
        while self.eat(&Token::Comma) {
            patterns.push(self.table_pattern()?);
        }
        Ok(patterns)
    }

    /// aggregate name { root table [where] [order by] [limit] [exclude] [root_only] }
    fn aggregate_stmt(&mut self) -> Result<StatementKind<'src>, ParseError> {
        self.pos += 1;
        let name = self.expect_ident("an aggregate name")?;
        self.expect(Token::LBrace, "'{' to open the aggregate block")?;
        self.expect(Token::Root, "'root' as the first clause of an aggregate")?;
        let root = self.expect_ident("the root table name")?;

        let (where_clause, order_by, limit, exclude_tables, root_only) = self.query_clauses()?;

        self.expect(Token::RBrace, "'}' to close the aggregate block")?;
        Ok(StatementKind::Aggregate(AggregateBlock {
            name,
            root,
            where_clause,
            order_by,
            limit,
            exclude_tables,
            root_only,
        }))
    }

    /// get name(params) { aggregate [where] [order by] [limit] [exclude] [root_only] }
    fn get_stmt(&mut self) -> Result<StatementKind<'src>, ParseError> {
        self.pos += 1;
        let name = self.expect_ident("a get function name")?;

        self.expect(Token::LParen, "'(' after the get function name")?;
        let mut params = Vec::new();
        if self.peek() != Some(&Token::RParen) {
            loop {
                let start = self.cur_span();
                let decl = self.var_decl()?;
                params.push((decl, Span::new(start.start, self.prev_end())));
                if !self.eat(&Token::Comma) {
                    break;
                }
                // Trailing comma allowed
                if self.peek() == Some(&Token::RParen) {
                    break;
                }
            }
        }
        self.expect(Token::RParen, "')' to close the parameter list")?;

        self.expect(Token::LBrace, "'{' to open the get function body")?;
        let aggregate = self.expect_ident("the aggregate name to fetch")?;

        let (where_clause, order_by, limit, exclude_tables, root_only) = self.query_clauses()?;

        self.expect(Token::RBrace, "'}' to close the get function body")?;
        Ok(StatementKind::Get(GetFunctionDef {
            name,
            params,
            aggregate,
            where_clause,
            order_by,
            limit,
            exclude_tables,
            root_only,
        }))
    }

    /// The shared optional clause sequence of aggregate and get bodies:
    /// [where "..."] [order by col [asc|desc]] [limit N|$var] [exclude ...] [root_only]
    #[allow(clippy::type_complexity)]
    fn query_clauses(
        &mut self,
    ) -> Result<
        (
            Option<Spanned<StringLiteral<'src>>>,
            Option<OrderByClause<'src>>,
            Option<Spanned<LimitValue<'src>>>,
            Vec<TablePattern<'src>>,
            bool,
        ),
        ParseError,
    > {
        let where_clause = if self.eat(&Token::Where) {
            Some(self.string_literal("a condition string after 'where'")?)
        } else {
            None
        };

        let order_by = if self.eat(&Token::Order) {
            self.expect(Token::By, "'by' after 'order'")?;
            let column = self.expect_ident("a column name after 'order by'")?;
            let direction = match self.peek() {
                Some(Token::Asc) => {
                    self.pos += 1;
                    Some(SortDirection::Asc)
                }
                Some(Token::Desc) => {
                    self.pos += 1;
                    Some(SortDirection::Desc)
                }
                _ => None,
            };
            Some(OrderByClause { column, direction })
        } else {
            None
        };

        let limit = if self.eat(&Token::Limit) {
            let start = self.cur_span();
            let value = match self.peek() {
                Some(Token::Int(n)) => {
                    let n = *n;
                    self.pos += 1;
                    LimitValue::Literal(n)
                }
                Some(Token::Dollar) => {
                    self.pos += 1;
                    let var = self.expect_ident("a variable name after '$'")?;
                    LimitValue::Variable(var.0)
                }
                _ => return Err(self.err_here("a number or $variable after 'limit'")),
            };
            Some((value, Span::new(start.start, self.prev_end())))
        } else {
            None
        };

        let exclude_tables = if self.eat(&Token::Exclude) {
            self.table_pattern_list()?
        } else {
            Vec::new()
        };

        let root_only = self.eat(&Token::RootOnly);

        Ok((where_clause, order_by, limit, exclude_tables, root_only))
    }

    /// preserve table where "condition"
    fn preserve_stmt(&mut self) -> Result<StatementKind<'src>, ParseError> {
        self.pos += 1;
        let table = self.expect_ident("a table name after 'preserve'")?;
        self.expect(Token::Where, "'where' after the table name")?;
        let where_clause = self.string_literal("a condition string after 'where'")?;
        Ok(StatementKind::Preserve(PreserveStmt { table, where_clause }))
    }

    /// set table { match col = val, ...  col = val ... }
    fn set_stmt(&mut self) -> Result<StatementKind<'src>, ParseError> {
        self.pos += 1;
        let table = self.expect_ident("a table name after 'set'")?;
        self.expect(Token::LBrace, "'{' to open the set block")?;

        self.expect(Token::Match, "'match' as the first clause of a set block")?;
        let mut match_clause = vec![self.assignment()?];
        while self.eat(&Token::Comma) {
            match_clause.push(self.assignment()?);
        }

        let mut assignments = Vec::new();
        while matches!(self.peek(), Some(Token::Ident(_))) {
            assignments.push(self.assignment()?);
        }

        self.expect(Token::RBrace, "'}' to close the set block")?;
        Ok(StatementKind::Set(SetBlock { table, match_clause, assignments }))
    }

    /// column = value
    fn assignment(&mut self) -> Result<Spanned<Assignment<'src>>, ParseError> {
        let start = self.cur_span();
        let column = self.expect_ident("a column name")?;
        self.expect(Token::Assign, "'=' after the column name")?;
        let value = self.value()?;
        Ok((
            Assignment { column, value },
            Span::new(start.start, self.prev_end()),
        ))
    }

    /// after { """sql""" "sql" ... } — SQL strings are kept raw
    fn after_stmt(&mut self) -> Result<StatementKind<'src>, ParseError> {
        self.pos += 1;
        self.expect(Token::LBrace, "'{' to open the after block")?;
        let mut statements = Vec::new();
        loop {
            match self.peek() {
                Some(Token::MultilineString(s)) | Some(Token::String(s)) => {
                    statements.push((*s, self.cur_span()));
                    self.pos += 1;
                }
                _ => break,
            }
        }
        self.expect(Token::RBrace, "'}' to close the after block")?;
        Ok(StatementKind::After(AfterBlock { statements }))
    }

    // ------------------------------------------------------------------
    // Values and literals
    // ------------------------------------------------------------------

    /// A value in an assignment: $var, string, number, or bool
    fn value(&mut self) -> Result<Spanned<Value<'src>>, ParseError> {
        let start = self.cur_span();
        let value = match self.peek() {
            Some(Token::Dollar) => {
                self.pos += 1;
                let var = self.expect_ident("a variable name after '$'")?;
                Value::Variable(var.0)
            }
            Some(Token::String(_)) => {
                let (lit, _) = self.string_literal("a string")?;
                Value::Literal(Literal::String(lit))
            }
            Some(Token::Bool(b)) => {
                let b = *b;
                self.pos += 1;
                Value::Literal(Literal::Bool(b))
            }
            Some(Token::Int(_)) | Some(Token::Float(_)) | Some(Token::Minus) => {
                Value::Literal(self.signed_number()?)
            }
            _ => return Err(self.err_here("a value ($variable, string, number, or bool)")),
        };
        Ok((value, Span::new(start.start, self.prev_end())))
    }

    /// A literal: string (with interpolation), number, bool, null, or an array
    fn literal(&mut self) -> Result<Spanned<Literal<'src>>, ParseError> {
        let start = self.cur_span();
        let literal = match self.peek() {
            Some(Token::LBracket) => self.array_literal()?,
            Some(Token::String(_)) => {
                let (lit, _) = self.string_literal("a string")?;
                Literal::String(lit)
            }
            Some(Token::Bool(b)) => {
                let b = *b;
                self.pos += 1;
                Literal::Bool(b)
            }
            Some(Token::Null) => {
                self.pos += 1;
                Literal::Null
            }
            Some(Token::Int(_)) | Some(Token::Float(_)) | Some(Token::Minus) => {
                self.signed_number()?
            }
            _ => return Err(self.err_here("a literal value")),
        };
        Ok((literal, Span::new(start.start, self.prev_end())))
    }

    /// A number with optional leading minus (the lexer never folds the sign
    /// into the literal — that broke binary minus without spaces)
    fn signed_number(&mut self) -> Result<Literal<'src>, ParseError> {
        let negative = self.eat(&Token::Minus);
        match self.peek() {
            Some(Token::Int(n)) => {
                let n = *n;
                self.pos += 1;
                Ok(Literal::Int(if negative { -n } else { n }))
            }
            Some(Token::Float(n)) => {
                let n = *n;
                self.pos += 1;
                Ok(Literal::Float(if negative { -n } else { n }))
            }
            _ => Err(self.err_here("a number")),
        }
    }

    /// [a, b, c] — element type decided by the first element; trailing comma
    /// allowed; empty arrays are string arrays
    fn array_literal(&mut self) -> Result<Literal<'src>, ParseError> {
        self.expect(Token::LBracket, "'['")?;

        if self.eat(&Token::RBracket) {
            return Ok(Literal::StringArray(Vec::new()));
        }

        enum Kind {
            Str,
            Int,
            Float,
            Bool,
        }
        let kind = match (self.peek(), self.peek2()) {
            (Some(Token::String(_)), _) => Kind::Str,
            (Some(Token::Bool(_)), _) => Kind::Bool,
            (Some(Token::Int(_)), _) | (Some(Token::Minus), Some(Token::Int(_))) => Kind::Int,
            (Some(Token::Float(_)), _) | (Some(Token::Minus), Some(Token::Float(_))) => Kind::Float,
            _ => return Err(self.err_here("an array element (string, number, or bool)")),
        };

        macro_rules! parse_elements {
            ($parse_one:expr) => {{
                let mut items = Vec::new();
                loop {
                    items.push($parse_one(self)?);
                    if !self.eat(&Token::Comma) {
                        break;
                    }
                    if self.peek() == Some(&Token::RBracket) {
                        break;
                    }
                }
                items
            }};
        }

        let literal = match kind {
            Kind::Str => Literal::StringArray(parse_elements!(|p: &mut Self| p
                .string_literal("a string array element"))),
            Kind::Bool => Literal::BoolArray(parse_elements!(|p: &mut Self| {
                match p.peek() {
                    Some(Token::Bool(b)) => {
                        let b = *b;
                        p.pos += 1;
                        Ok(b)
                    }
                    _ => Err(p.err_here("a bool array element")),
                }
            })),
            Kind::Int => Literal::IntArray(parse_elements!(|p: &mut Self| {
                match p.signed_number()? {
                    Literal::Int(n) => Ok(n),
                    _ => Err(p.err_here("an int array element")),
                }
            })),
            Kind::Float => Literal::FloatArray(parse_elements!(|p: &mut Self| {
                match p.signed_number()? {
                    Literal::Float(n) => Ok(n),
                    _ => Err(p.err_here("a float array element")),
                }
            })),
        };

        self.expect(Token::RBracket, "']' to close the array")?;
        Ok(literal)
    }

    /// A string literal with interpolation and escape decoding. Malformed
    /// interpolations are recorded as errors but parsing continues with an
    /// empty literal, so one bad string doesn't hide later errors.
    fn string_literal(
        &mut self,
        what: &str,
    ) -> Result<Spanned<StringLiteral<'src>>, ParseError> {
        match self.peek() {
            Some(Token::String(s)) => {
                let s = *s;
                let span = self.cur_span();
                self.pos += 1;
                // Offset by 1 to account for the opening quote
                match parse_interpolated_string(s, span.start + 1) {
                    Ok(lit) => Ok((lit, span)),
                    Err(errors) => {
                        for err in errors {
                            self.errors.push(ParseError {
                                span: err.span.into_range(),
                                message: err.message,
                            });
                        }
                        Ok((StringLiteral { parts: vec![] }, span))
                    }
                }
            }
            _ => Err(self.err_here(what)),
        }
    }

    /// A raw string literal (no interpolation, no escape decoding)
    fn string_raw(&mut self, what: &str) -> Result<Spanned<&'src str>, ParseError> {
        match self.peek() {
            Some(Token::String(s)) => {
                let s = *s;
                let span = self.cur_span();
                self.pos += 1;
                Ok((s, span))
            }
            _ => Err(self.err_here(what)),
        }
    }

    // ------------------------------------------------------------------
    // Expressions (precedence climbing)
    // ------------------------------------------------------------------

    fn expr(&mut self) -> Result<Spanned<Expr<'src>>, ParseError> {
        self.or_expr()
    }

    fn binary_level(
        &mut self,
        next: fn(&mut Self) -> Result<Spanned<Expr<'src>>, ParseError>,
        op_of: fn(&Token<'src>) -> Option<BinaryOp>,
    ) -> Result<Spanned<Expr<'src>>, ParseError> {
        let mut lhs = next(self)?;
        while let Some(op) = self.peek().and_then(op_of) {
            self.pos += 1;
            let rhs = next(self)?;
            let span = Span::new(lhs.1.start, rhs.1.end);
            lhs = (Expr::Binary(Box::new(lhs), op, Box::new(rhs)), span);
        }
        Ok(lhs)
    }

    fn or_expr(&mut self) -> Result<Spanned<Expr<'src>>, ParseError> {
        self.binary_level(Self::and_expr, |t| match t {
            Token::Or => Some(BinaryOp::Or),
            _ => None,
        })
    }

    fn and_expr(&mut self) -> Result<Spanned<Expr<'src>>, ParseError> {
        self.binary_level(Self::equality_expr, |t| match t {
            Token::And => Some(BinaryOp::And),
            _ => None,
        })
    }

    fn equality_expr(&mut self) -> Result<Spanned<Expr<'src>>, ParseError> {
        self.binary_level(Self::comparison_expr, |t| match t {
            Token::Eq => Some(BinaryOp::Eq),
            Token::NotEq => Some(BinaryOp::NotEq),
            _ => None,
        })
    }

    fn comparison_expr(&mut self) -> Result<Spanned<Expr<'src>>, ParseError> {
        self.binary_level(Self::additive_expr, |t| match t {
            Token::Lt => Some(BinaryOp::Lt),
            Token::Gt => Some(BinaryOp::Gt),
            Token::LtEq => Some(BinaryOp::LtEq),
            Token::GtEq => Some(BinaryOp::GtEq),
            _ => None,
        })
    }

    fn additive_expr(&mut self) -> Result<Spanned<Expr<'src>>, ParseError> {
        self.binary_level(Self::multiplicative_expr, |t| match t {
            Token::Plus => Some(BinaryOp::Add),
            Token::Minus => Some(BinaryOp::Sub),
            _ => None,
        })
    }

    fn multiplicative_expr(&mut self) -> Result<Spanned<Expr<'src>>, ParseError> {
        self.binary_level(Self::unary_expr, |t| match t {
            Token::Star => Some(BinaryOp::Mul),
            Token::Slash => Some(BinaryOp::Div),
            Token::Percent => Some(BinaryOp::Mod),
            _ => None,
        })
    }

    fn unary_expr(&mut self) -> Result<Spanned<Expr<'src>>, ParseError> {
        let op = match self.peek() {
            Some(Token::Not) => Some(UnaryOp::Not),
            Some(Token::Minus) => Some(UnaryOp::Neg),
            _ => None,
        };
        if let Some(op) = op {
            let start = self.cur_span().start;
            self.pos += 1;
            let inner = self.unary_expr()?;
            let span = Span::new(start, inner.1.end);
            return Ok((Expr::Unary(op, Box::new(inner)), span));
        }
        self.primary_expr()
    }

    fn primary_expr(&mut self) -> Result<Spanned<Expr<'src>>, ParseError> {
        let span = self.cur_span();
        match self.peek() {
            Some(Token::Dollar) => {
                self.pos += 1;
                let name = if self.in_interpolation {
                    // Inside interpolations keywords are valid variable names
                    // ({$limit} etc.)
                    match self.peek().and_then(token_as_ident) {
                        Some(name) => {
                            self.pos += 1;
                            name
                        }
                        None => return Err(self.err_here("a variable name after '$'")),
                    }
                } else {
                    self.expect_ident("a variable name after '$'")?.0
                };
                Ok((Expr::Variable(name), Span::new(span.start, self.prev_end())))
            }
            Some(Token::Ident("unique"))
                if self.in_interpolation && self.peek2() == Some(&Token::LParen) =>
            {
                self.pos += 2;
                self.expect(Token::RParen, "')' to close unique()")?;
                Ok((Expr::Unique, Span::new(span.start, self.prev_end())))
            }
            Some(Token::Int(n)) => {
                let n = *n;
                self.pos += 1;
                Ok((Expr::Literal(Literal::Int(n)), span))
            }
            Some(Token::Float(n)) => {
                let n = *n;
                self.pos += 1;
                Ok((Expr::Literal(Literal::Float(n)), span))
            }
            Some(Token::Bool(b)) => {
                let b = *b;
                self.pos += 1;
                Ok((Expr::Literal(Literal::Bool(b)), span))
            }
            Some(Token::String(_)) => {
                if self.in_interpolation {
                    // Nested strings inside interpolations are plain text
                    let (s, span) = self.string_raw("a string")?;
                    Ok((
                        Expr::Literal(Literal::String(StringLiteral {
                            parts: vec![StringPart::Text(std::borrow::Cow::Borrowed(s))],
                        })),
                        span,
                    ))
                } else {
                    let (lit, span) = self.string_literal("a string")?;
                    Ok((Expr::Literal(Literal::String(lit)), span))
                }
            }
            Some(Token::LParen) => {
                self.pos += 1;
                let (inner, _) = self.expr()?;
                self.expect(Token::RParen, "')' to close the expression")?;
                Ok((inner, Span::new(span.start, self.prev_end())))
            }
            _ => Err(self.err_here("an expression ($variable, literal, or parentheses)")),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> Program<'_> {
        crate::parse(input).unwrap_or_else(|errors| panic!("Parse errors: {:?}", errors))
    }

    #[test]
    fn test_import() {
        let program = parse(r#"import "base.dsl""#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Import((path, _)) => assert_eq!(*path, "base.dsl"),
            _ => panic!("Expected Import"),
        }
    }

    #[test]
    fn test_var_decl() {
        let program = parse(r#"var base_url: string = "http://localhost""#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(decl.name.0, "base_url");
                assert_eq!(decl.var_type.0, VarType::String);
                assert!(decl.default.is_some());
            }
            _ => panic!("Expected Var"),
        }
    }

    #[test]
    fn test_negative_number_defaults() {
        // SPEC-promised and previously unparseable: negative floats, and
        // negative ints now lex as Minus + Int
        let program = parse("var tax: float = -0.5\nvar offset: int = -3");
        match &program.statements[0].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(decl.default.as_ref().unwrap().0, Literal::Float(-0.5));
            }
            _ => panic!("Expected Var"),
        }
        match &program.statements[1].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(decl.default.as_ref().unwrap().0, Literal::Int(-3));
            }
            _ => panic!("Expected Var"),
        }
    }

    #[test]
    fn test_negative_array_elements() {
        let program = parse("var a: int[] = [1, -2, 3]\nvar b: float[] = [-1.5, 2.5]");
        match &program.statements[0].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(
                    decl.default.as_ref().unwrap().0,
                    Literal::IntArray(vec![1, -2, 3])
                );
            }
            _ => panic!("Expected Var"),
        }
        match &program.statements[1].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(
                    decl.default.as_ref().unwrap().0,
                    Literal::FloatArray(vec![-1.5, 2.5])
                );
            }
            _ => panic!("Expected Var"),
        }
    }

    #[test]
    fn test_bad_interpolation_is_parse_error() {
        let input = r#"
            aggregate orders {
                root orders
                where "user_id = ${user_id}"
            }
        "#;
        let result = crate::parse(input);
        let errors = result.expect_err("${var} typo must be a parse error");
        assert!(
            errors[0].message.contains("did you mean {$user_id}?"),
            "expected hint, got: {}",
            errors[0].message
        );
    }

    #[test]
    fn test_division_in_when_condition() {
        // Previously the regex lexer rule swallowed everything after '/'
        let program = parse("#[when($order_limit / 2 > 10)]\nexclude_data audit_log");
        assert_eq!(program.statements.len(), 1);
        assert!(program.statements[0].0.attribute.is_some());
    }

    #[test]
    fn test_var_no_default() {
        let program = parse("var order_limit: int");
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(decl.name.0, "order_limit");
                assert_eq!(decl.var_type.0, VarType::Int);
                assert!(decl.default.is_none());
            }
            _ => panic!("Expected Var"),
        }
    }

    #[test]
    fn test_faker() {
        let program = parse(r#"faker names ["John", "Jane", "Bob"]"#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Faker(decl) => {
                assert_eq!(decl.name.0, "names");
                match &decl.source {
                    FakerSource::Array(values) => assert_eq!(values.len(), 3),
                    _ => panic!("Expected Array source"),
                }
            }
            _ => panic!("Expected Faker"),
        }
    }

    #[test]
    fn test_relation() {
        let program = parse("relation customer.group_id -> customer_group.id");
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Relation(decl) => {
                assert_eq!(decl.from.0.table, "customer");
                assert_eq!(decl.from.0.column, "group_id");
                assert_eq!(decl.to.0.table, "customer_group");
                assert_eq!(decl.to.0.column, "id");
            }
            _ => panic!("Expected Relation"),
        }
    }

    #[test]
    fn test_anonymize() {
        let program = parse(r#"
            anonymize customer {
                email -> emails
                password -> null
            }
        "#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Anonymize(block) => {
                assert_eq!(block.table.0, "customer");
                assert_eq!(block.rules.len(), 2);
            }
            _ => panic!("Expected Anonymize"),
        }
    }

    #[test]
    fn test_exclude_ignore() {
        let program = parse("exclude_data payments\nignore_table logs");
        assert_eq!(program.statements.len(), 2);
        match &program.statements[0].0.kind {
            StatementKind::Exclude(TablePattern::Exact((table, _))) => assert_eq!(*table, "payments"),
            _ => panic!("Expected Exclude with exact pattern"),
        }
        match &program.statements[1].0.kind {
            StatementKind::Ignore(TablePattern::Exact((table, _))) => assert_eq!(*table, "logs"),
            _ => panic!("Expected Ignore with exact pattern"),
        }
    }

    #[test]
    fn test_exclude_ignore_regex() {
        let program = parse("exclude_data /^cache_/\nignore_table /^tmp_/");
        assert_eq!(program.statements.len(), 2);
        match &program.statements[0].0.kind {
            StatementKind::Exclude(TablePattern::Regex((pattern, _))) => assert_eq!(*pattern, "^cache_"),
            _ => panic!("Expected Exclude with regex pattern"),
        }
        match &program.statements[1].0.kind {
            StatementKind::Ignore(TablePattern::Regex((pattern, _))) => assert_eq!(*pattern, "^tmp_"),
            _ => panic!("Expected Ignore with regex pattern"),
        }
    }

    #[test]
    fn test_aggregate() {
        let program = parse(r#"
            aggregate orders {
                root sales_order
                where "created_at > NOW() - INTERVAL 90 DAY"
                order by created_at desc
                limit 100
            }
        "#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Aggregate(block) => {
                assert_eq!(block.name.0, "orders");
                assert_eq!(block.root.0, "sales_order");
                assert!(block.where_clause.is_some());
                assert!(block.order_by.is_some());
                assert!(block.limit.is_some());
            }
            _ => panic!("Expected Aggregate"),
        }
    }

    #[test]
    fn test_full() {
        let program = parse("full store, catalog_category_entity");
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Full(patterns) => {
                assert_eq!(patterns.len(), 2);
                match &patterns[0] {
                    TablePattern::Exact((name, _)) => assert_eq!(*name, "store"),
                    _ => panic!("Expected Exact pattern"),
                }
                match &patterns[1] {
                    TablePattern::Exact((name, _)) => assert_eq!(*name, "catalog_category_entity"),
                    _ => panic!("Expected Exact pattern"),
                }
            }
            _ => panic!("Expected Full"),
        }
    }

    #[test]
    fn test_full_with_regex() {
        let program = parse("full store, /^eav_/, catalog_category_entity");
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Full(patterns) => {
                assert_eq!(patterns.len(), 3);
                match &patterns[0] {
                    TablePattern::Exact((name, _)) => assert_eq!(*name, "store"),
                    _ => panic!("Expected Exact pattern"),
                }
                match &patterns[1] {
                    TablePattern::Regex((pattern, _)) => assert_eq!(*pattern, "^eav_"),
                    _ => panic!("Expected Regex pattern"),
                }
                match &patterns[2] {
                    TablePattern::Exact((name, _)) => assert_eq!(*name, "catalog_category_entity"),
                    _ => panic!("Expected Exact pattern"),
                }
            }
            _ => panic!("Expected Full"),
        }
    }

    #[test]
    fn test_get_function() {
        let program = parse(r#"
            get orders_for_user(user_id: int) {
                orders where "user_id = {$user_id}"
            }
        "#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Get(func) => {
                assert_eq!(func.name.0, "orders_for_user");
                assert_eq!(func.params.len(), 1);
                assert_eq!(func.params[0].0.name.0, "user_id");
                assert_eq!(func.params[0].0.var_type.0, VarType::Int);
                assert!(func.params[0].0.default.is_none());
                assert_eq!(func.aggregate.0, "orders");
                assert!(func.where_clause.is_some());
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_get_function_with_defaults() {
        let program = parse(r#"
            get products(cat_id: int, max_rows: int = 50) {
                product_aggregate where "category_id = {$cat_id}"
                limit $max_rows
            }
        "#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Get(func) => {
                assert_eq!(func.name.0, "products");
                assert_eq!(func.params.len(), 2);
                assert_eq!(func.params[0].0.name.0, "cat_id");
                assert!(func.params[0].0.default.is_none());
                assert_eq!(func.params[1].0.name.0, "max_rows");
                assert!(func.params[1].0.default.is_some());
                assert_eq!(func.aggregate.0, "product_aggregate");
                assert!(func.where_clause.is_some());
                assert!(func.limit.is_some());
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_get_function_no_where() {
        let program = parse(r#"
            get all_products(max_rows: int = 100) {
                products
                limit $max_rows
            }
        "#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Get(func) => {
                assert_eq!(func.name.0, "all_products");
                assert_eq!(func.aggregate.0, "products");
                assert!(func.where_clause.is_none());
                assert!(func.limit.is_some());
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_get_function_full_overrides() {
        let program = parse(r#"
            get recent(cutoff: string) {
                orders where "created_at > '{$cutoff}'"
                order by created_at desc
                limit 50
                exclude /^temp_/
            }
        "#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Get(func) => {
                assert_eq!(func.name.0, "recent");
                assert_eq!(func.params.len(), 1);
                assert!(func.where_clause.is_some());
                assert!(func.order_by.is_some());
                assert!(func.limit.is_some());
                assert_eq!(func.exclude_tables.len(), 1);
            }
            _ => panic!("Expected Get"),
        }
    }

    #[test]
    fn test_preserve() {
        let program = parse(r#"preserve config where "key LIKE 'dev/%'""#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Preserve(stmt) => {
                assert_eq!(stmt.table.0, "config");
            }
            _ => panic!("Expected Preserve"),
        }
    }

    #[test]
    fn test_set() {
        let program = parse(r#"
            set config {
                match path = "web/url", scope = "default"
                value = "http://localhost"
            }
        "#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Set(block) => {
                assert_eq!(block.table.0, "config");
                assert_eq!(block.match_clause.len(), 2);
                assert_eq!(block.assignments.len(), 1);
            }
            _ => panic!("Expected Set"),
        }
    }

    #[test]
    fn test_after() {
        let program = parse(r#"
            after {
                """UPDATE orders SET date = NOW()"""
            }
        "#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::After(block) => {
                assert_eq!(block.statements.len(), 1);
            }
            _ => panic!("Expected After"),
        }
    }

    #[test]
    fn test_after_with_regular_string() {
        let program = parse(r#"
            after {
                "UPDATE orders SET status = 'active'"
            }
        "#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::After(block) => {
                assert_eq!(block.statements.len(), 1);
                assert_eq!(block.statements[0].0, "UPDATE orders SET status = 'active'");
            }
            _ => panic!("Expected After"),
        }
    }

    #[test]
    fn test_after_mixed_strings() {
        let program = parse(r#"
            after {
                """
                UPDATE orders
                SET date = NOW()
                """
                "DELETE FROM temp_table"
            }
        "#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::After(block) => {
                assert_eq!(block.statements.len(), 2);
            }
            _ => panic!("Expected After"),
        }
    }

    #[test]
    fn test_attribute() {
        let program = parse(r#"
            #[when($skip_payments)]
            exclude_data payments
        "#);
        assert_eq!(program.statements.len(), 1);
        assert!(program.statements[0].0.attribute.is_some());
    }

    #[test]
    fn test_complex_expression() {
        let program = parse(r#"
            #[when($debug && $env != "production")]
            exclude_data payments
        "#);
        assert_eq!(program.statements.len(), 1);
        let attr = program.statements[0].0.attribute.as_ref().unwrap();
        match &attr.0 {
            Expr::Binary(_, BinaryOp::And, _) => {}
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_string_interpolation_in_set() {
        let program = parse(r#"
            set config {
                match path = "web/url"
                value = "https://{$domain}:{$port}/"
            }
        "#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Set(block) => {
                assert_eq!(block.assignments.len(), 1);
                let (assignment, _) = &block.assignments[0];
                match &assignment.value.0 {
                    Value::Literal(Literal::String(s)) => {
                        // Should have: "https://" + {$domain} + ":" + {$port} + "/"
                        assert_eq!(s.parts.len(), 5);
                        assert!(matches!(&s.parts[0], StringPart::Text(t) if t == "https://"));
                        assert!(matches!(&s.parts[1], StringPart::Interpolation(_)));
                        assert!(matches!(&s.parts[2], StringPart::Text(t) if t == ":"));
                        assert!(matches!(&s.parts[3], StringPart::Interpolation(_)));
                        assert!(matches!(&s.parts[4], StringPart::Text(t) if t == "/"));
                    }
                    _ => panic!("Expected String literal with interpolation"),
                }
            }
            _ => panic!("Expected Set"),
        }
    }

    #[test]
    fn test_string_interpolation_expression() {
        let program = parse(r#"
            set config {
                match path = "port"
                value = "{$base_port + 1}"
            }
        "#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Set(block) => {
                let (assignment, _) = &block.assignments[0];
                match &assignment.value.0 {
                    Value::Literal(Literal::String(s)) => {
                        assert_eq!(s.parts.len(), 1);
                        match &s.parts[0] {
                            StringPart::Interpolation((expr, _)) => match expr {
                                Expr::Binary(_, BinaryOp::Add, _) => {}
                                _ => panic!("Expected Binary Add expression"),
                            },
                            _ => panic!("Expected Interpolation"),
                        }
                    }
                    _ => panic!("Expected String literal"),
                }
            }
            _ => panic!("Expected Set"),
        }
    }

    #[test]
    fn test_string_array_var() {
        let program = parse(r#"var emails: string[] = ["a@test.com", "b@test.com"]"#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(decl.name.0, "emails");
                assert_eq!(decl.var_type.0, VarType::StringArray);
                match &decl.default {
                    Some((Literal::StringArray(arr), _)) => {
                        assert_eq!(arr.len(), 2);
                    }
                    _ => panic!("Expected StringArray literal"),
                }
            }
            _ => panic!("Expected Var"),
        }
    }

    #[test]
    fn test_string_array_var_no_default() {
        let program = parse("var emails: string[]");
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(decl.name.0, "emails");
                assert_eq!(decl.var_type.0, VarType::StringArray);
                assert!(decl.default.is_none());
            }
            _ => panic!("Expected Var"),
        }
    }

    #[test]
    fn test_faker_with_spread() {
        let program = parse(r#"faker combined [...$base_emails, "extra@test.com"]"#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Faker(decl) => {
                assert_eq!(decl.name.0, "combined");
                match &decl.source {
                    FakerSource::Array(values) => {
                        assert_eq!(values.len(), 2);
                        match &values[0].0 {
                            FakerValue::Spread(var) => assert_eq!(*var, "base_emails"),
                            _ => panic!("Expected Spread"),
                        }
                        match &values[1].0 {
                            FakerValue::Literal(_) => {}
                            _ => panic!("Expected Literal"),
                        }
                    }
                    _ => panic!("Expected Array source"),
                }
            }
            _ => panic!("Expected Faker"),
        }
    }

    #[test]
    fn test_faker_spread_only() {
        let program = parse(r#"faker emails [...$input_emails]"#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Faker(decl) => {
                assert_eq!(decl.name.0, "emails");
                match &decl.source {
                    FakerSource::Array(values) => {
                        assert_eq!(values.len(), 1);
                        match &values[0].0 {
                            FakerValue::Spread(var) => assert_eq!(*var, "input_emails"),
                            _ => panic!("Expected Spread"),
                        }
                    }
                    _ => panic!("Expected Array source"),
                }
            }
            _ => panic!("Expected Faker"),
        }
    }

    #[test]
    fn test_faker_multiple_spreads() {
        let program = parse(r#"faker all_emails [...$emails1, ...$emails2, "extra@test.com"]"#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Faker(decl) => {
                assert_eq!(decl.name.0, "all_emails");
                match &decl.source {
                    FakerSource::Array(values) => {
                        assert_eq!(values.len(), 3);
                        match &values[0].0 {
                            FakerValue::Spread(var) => assert_eq!(*var, "emails1"),
                            _ => panic!("Expected Spread"),
                        }
                        match &values[1].0 {
                            FakerValue::Spread(var) => assert_eq!(*var, "emails2"),
                            _ => panic!("Expected Spread"),
                        }
                        match &values[2].0 {
                            FakerValue::Literal(_) => {}
                            _ => panic!("Expected Literal"),
                        }
                    }
                    _ => panic!("Expected Array source"),
                }
            }
            _ => panic!("Expected Faker"),
        }
    }

    #[test]
    fn test_faker_variable_source() {
        let program = parse("faker emails $input_emails");
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Faker(decl) => {
                assert_eq!(decl.name.0, "emails");
                match &decl.source {
                    FakerSource::Variable(var) => assert_eq!(*var, "input_emails"),
                    _ => panic!("Expected Variable source"),
                }
            }
            _ => panic!("Expected Faker"),
        }
    }

    #[test]
    fn test_int_array_var() {
        let program = parse("var ids: int[] = [1, 2, 3]");
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(decl.name.0, "ids");
                assert_eq!(decl.var_type.0, VarType::IntArray);
                match &decl.default {
                    Some((Literal::IntArray(arr), _)) => {
                        assert_eq!(*arr, vec![1, 2, 3]);
                    }
                    _ => panic!("Expected IntArray literal"),
                }
            }
            _ => panic!("Expected Var"),
        }
    }

    #[test]
    fn test_float_array_var() {
        let program = parse("var prices: float[] = [1.5, 2.75, 3.14]");
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(decl.name.0, "prices");
                assert_eq!(decl.var_type.0, VarType::FloatArray);
                match &decl.default {
                    Some((Literal::FloatArray(arr), _)) => {
                        assert_eq!(arr.len(), 3);
                        assert!((arr[0] - 1.5).abs() < 0.001);
                        assert!((arr[1] - 2.75).abs() < 0.001);
                        assert!((arr[2] - 3.14).abs() < 0.001);
                    }
                    _ => panic!("Expected FloatArray literal"),
                }
            }
            _ => panic!("Expected Var"),
        }
    }

    #[test]
    fn test_bool_array_var() {
        let program = parse("var flags: bool[] = [true, false, true]");
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(decl.name.0, "flags");
                assert_eq!(decl.var_type.0, VarType::BoolArray);
                match &decl.default {
                    Some((Literal::BoolArray(arr), _)) => {
                        assert_eq!(*arr, vec![true, false, true]);
                    }
                    _ => panic!("Expected BoolArray literal"),
                }
            }
            _ => panic!("Expected Var"),
        }
    }

    #[test]
    fn test_array_types_no_default() {
        let program = parse("var a: int[]\nvar b: float[]\nvar c: bool[]");
        assert_eq!(program.statements.len(), 3);

        match &program.statements[0].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(decl.var_type.0, VarType::IntArray);
                assert!(decl.default.is_none());
            }
            _ => panic!("Expected Var"),
        }
        match &program.statements[1].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(decl.var_type.0, VarType::FloatArray);
                assert!(decl.default.is_none());
            }
            _ => panic!("Expected Var"),
        }
        match &program.statements[2].0.kind {
            StatementKind::Var(decl) => {
                assert_eq!(decl.var_type.0, VarType::BoolArray);
                assert!(decl.default.is_none());
            }
            _ => panic!("Expected Var"),
        }
    }
}
