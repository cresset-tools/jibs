//! Parser for the MySQL Import DSL

use chumsky::{input::ValueInput, prelude::*};

use crate::ast::*;
use crate::interpolation::parse_interpolated_string;
use crate::lexer::Token;
use crate::Span;

/// Create the main parser for the DSL
pub fn parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Program<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    statement_parser()
        .repeated()
        .collect()
        .map(|statements| Program { statements })
}

/// Parse a single statement (with optional attribute)
fn statement_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Spanned<Statement<'src>>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    let attribute = attribute_parser();

    attribute
        .or_not()
        .then(statement_kind_parser())
        .map_with(|(attribute, kind), e| {
            (
                Statement { attribute, kind },
                e.span(),
            )
        })
}

/// Parse #[when(expr)]
fn attribute_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Spanned<Expr<'src>>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    just(Token::Hash)
        .ignore_then(just(Token::LBracket))
        .ignore_then(just(Token::When))
        .ignore_then(just(Token::LParen))
        .ignore_then(expr_parser())
        .then_ignore(just(Token::RParen))
        .then_ignore(just(Token::RBracket))
}

/// Parse the different statement kinds
fn statement_kind_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    choice((
        import_parser(),
        var_parser(),
        faker_parser(),
        relation_parser(),
        anonymize_parser(),
        exclude_parser(),
        ignore_parser(),
        aggregate_parser(),
        include_parser(),
        preserve_parser(),
        set_parser(),
        after_parser(),
    ))
}

/// Parse: import "path"
fn import_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    just(Token::Import)
        .ignore_then(string_literal_raw())
        .map(StatementKind::Import)
}

/// Parse: var name: type = default
fn var_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    just(Token::Var)
        .ignore_then(ident())
        .then_ignore(just(Token::Colon))
        .then(var_type())
        .then(
            just(Token::Assign)
                .ignore_then(literal())
                .or_not()
        )
        .map(|((name, var_type), default)| {
            StatementKind::Var(VarDecl {
                name,
                var_type,
                default,
            })
        })
}

/// Parse a type keyword
fn var_type<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Spanned<VarType>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    choice((
        just(Token::TypeString).to(VarType::String),
        just(Token::TypeInt).to(VarType::Int),
        just(Token::TypeFloat).to(VarType::Float),
        just(Token::TypeBool).to(VarType::Bool),
    ))
    .map_with(|t, e| (t, e.span()))
}

/// Parse: faker name ["value1", "value2"]
fn faker_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    just(Token::Faker)
        .ignore_then(ident())
        .then(
            string_literal()
                .separated_by(just(Token::Comma))
                .allow_trailing()
                .collect()
                .delimited_by(just(Token::LBracket), just(Token::RBracket))
        )
        .map(|(name, values)| {
            StatementKind::Faker(FakerDecl { name, values })
        })
}

/// Parse: relation table.column -> table.column
fn relation_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    just(Token::Relation)
        .ignore_then(column_ref())
        .then_ignore(just(Token::Arrow))
        .then(column_ref())
        .map(|(from, to)| {
            StatementKind::Relation(RelationDecl { from, to })
        })
}

/// Parse: table.column
fn column_ref<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Spanned<ColumnRef<'src>>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    ident_raw()
        .then_ignore(just(Token::Dot))
        .then(ident_raw())
        .map_with(|(table, column), e| {
            (ColumnRef { table, column }, e.span())
        })
}

/// Parse: anonymize table { ... }
fn anonymize_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    let rule = ident()
        .then_ignore(just(Token::Arrow))
        .then(
            just(Token::Null).to(AnonymizeTarget::Null)
                .or(ident().map(AnonymizeTarget::Faker))
        )
        .map_with(|(column, target), e| {
            (AnonymizeRule { column, target }, e.span())
        });

    just(Token::Anonymize)
        .ignore_then(ident())
        .then(
            rule.repeated()
                .collect()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|(table, rules)| {
            StatementKind::Anonymize(AnonymizeBlock { table, rules })
        })
}

/// Parse: exclude_data table
fn exclude_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    just(Token::ExcludeData)
        .ignore_then(ident())
        .map(StatementKind::Exclude)
}

/// Parse: ignore_table table
fn ignore_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    just(Token::IgnoreTable)
        .ignore_then(ident())
        .map(StatementKind::Ignore)
}

/// Parse: aggregate name { root, where, order by, limit }
fn aggregate_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    let root_clause = just(Token::Root)
        .ignore_then(ident());

    let where_clause = just(Token::Where)
        .ignore_then(string_literal());

    let order_by_clause = just(Token::Order)
        .ignore_then(just(Token::By))
        .ignore_then(ident())
        .then(
            just(Token::Asc).to(SortDirection::Asc)
                .or(just(Token::Desc).to(SortDirection::Desc))
                .or_not()
        )
        .map(|(column, direction)| OrderByClause { column, direction });

    let limit_clause = just(Token::Limit)
        .ignore_then(
            select! { Token::Int(n) => LimitValue::Literal(n) }
                .or(
                    just(Token::Dollar)
                        .ignore_then(ident_raw())
                        .map(LimitValue::Variable)
                )
        )
        .map_with(|v, e| (v, e.span()));

    just(Token::Aggregate)
        .ignore_then(ident())
        .then(
            root_clause
                .then(where_clause.or_not())
                .then(order_by_clause.or_not())
                .then(limit_clause.or_not())
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|(name, (((root, where_clause), order_by), limit))| {
            StatementKind::Aggregate(AggregateBlock {
                name,
                root,
                where_clause,
                order_by,
                limit,
            })
        })
}

/// Parse: include aggregate where "condition"
fn include_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    just(Token::Include)
        .ignore_then(ident())
        .then_ignore(just(Token::Where))
        .then(string_literal())
        .map(|(aggregate, where_clause)| {
            StatementKind::Include(IncludeStmt {
                aggregate,
                where_clause,
            })
        })
}

/// Parse: preserve table where "condition"
fn preserve_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    just(Token::Preserve)
        .ignore_then(ident())
        .then_ignore(just(Token::Where))
        .then(string_literal())
        .map(|(table, where_clause)| {
            StatementKind::Preserve(PreserveStmt {
                table,
                where_clause,
            })
        })
}

/// Parse: set table { match ..., col = val }
fn set_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    let assignment = ident()
        .then_ignore(just(Token::Assign))
        .then(value_parser())
        .map_with(|(column, value), e| {
            (Assignment { column, value }, e.span())
        });

    let match_clause = just(Token::Match)
        .ignore_then(
            assignment.clone()
                .separated_by(just(Token::Comma))
                .at_least(1)
                .collect()
        );

    just(Token::Set)
        .ignore_then(ident())
        .then(
            match_clause
                .then(assignment.repeated().collect())
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|(table, (match_clause, assignments))| {
            StatementKind::Set(SetBlock {
                table,
                match_clause,
                assignments,
            })
        })
}

/// Parse: after { """sql""" """sql""" }
fn after_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, StatementKind<'src>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    just(Token::After)
        .ignore_then(
            multiline_string()
                .repeated()
                .collect()
                .delimited_by(just(Token::LBrace), just(Token::RBrace))
        )
        .map(|statements| StatementKind::After(AfterBlock { statements }))
}

/// Parse a value (literal, variable, or string with interpolation)
fn value_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Spanned<Value<'src>>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    choice((
        just(Token::Dollar)
            .ignore_then(ident_raw())
            .map(Value::Variable),
        // String with interpolation support
        select! { Token::String(s) => s }
            .map_with(|s, e| {
                let span: Span = e.span();
                let str_literal = parse_interpolated_string(s, span.start);
                Value::Literal(Literal::String(str_literal))
            }),
        select! { Token::Int(n) => Value::Literal(Literal::Int(n)) },
        select! { Token::Float(n) => Value::Literal(Literal::Float(n)) },
        select! { Token::Bool(b) => Value::Literal(Literal::Bool(b)) },
    ))
    .map_with(|v, e| (v, e.span()))
}

/// Parse expression (for conditionals and interpolation)
fn expr_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Spanned<Expr<'src>>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    recursive(|expr| {
        // Primary expressions
        let primary = choice((
            // Variable reference
            just(Token::Dollar)
                .ignore_then(ident_raw())
                .map(Expr::Variable),
            // Literals
            select! {
                Token::Int(n) => Expr::Literal(Literal::Int(n)),
                Token::Float(n) => Expr::Literal(Literal::Float(n)),
                Token::Bool(b) => Expr::Literal(Literal::Bool(b)),
                Token::String(s) => Expr::Literal(Literal::String(StringLiteral {
                    parts: vec![StringPart::Text(s)],
                })),
            },
            // Parenthesized expression
            expr.clone()
                .delimited_by(just(Token::LParen), just(Token::RParen))
                .map(|(e, _)| e),
        ))
        .map_with(|e, ctx| (e, ctx.span()));

        // Unary operators
        let unary = just(Token::Not).to(UnaryOp::Not)
            .or(just(Token::Minus).to(UnaryOp::Neg))
            .repeated()
            .foldr_with(primary, |op, expr, e| {
                (Expr::Unary(op, Box::new(expr)), e.span())
            });

        // Multiplicative
        let op = just(Token::Star).to(BinaryOp::Mul)
            .or(just(Token::Slash).to(BinaryOp::Div))
            .or(just(Token::Percent).to(BinaryOp::Mod));
        let multiplicative = unary.clone().foldl_with(
            op.then(unary).repeated(),
            |a, (op, b), e| (Expr::Binary(Box::new(a), op, Box::new(b)), e.span()),
        );

        // Additive
        let op = just(Token::Plus).to(BinaryOp::Add)
            .or(just(Token::Minus).to(BinaryOp::Sub));
        let additive = multiplicative.clone().foldl_with(
            op.then(multiplicative).repeated(),
            |a, (op, b), e| (Expr::Binary(Box::new(a), op, Box::new(b)), e.span()),
        );

        // Comparison
        let op = just(Token::Lt).to(BinaryOp::Lt)
            .or(just(Token::Gt).to(BinaryOp::Gt))
            .or(just(Token::LtEq).to(BinaryOp::LtEq))
            .or(just(Token::GtEq).to(BinaryOp::GtEq));
        let comparison = additive.clone().foldl_with(
            op.then(additive).repeated(),
            |a, (op, b), e| (Expr::Binary(Box::new(a), op, Box::new(b)), e.span()),
        );

        // Equality
        let op = just(Token::Eq).to(BinaryOp::Eq)
            .or(just(Token::NotEq).to(BinaryOp::NotEq));
        let equality = comparison.clone().foldl_with(
            op.then(comparison).repeated(),
            |a, (op, b), e| (Expr::Binary(Box::new(a), op, Box::new(b)), e.span()),
        );

        // Logical AND
        let and = equality.clone().foldl_with(
            just(Token::And).ignore_then(equality).repeated(),
            |a, b, e| (Expr::Binary(Box::new(a), BinaryOp::And, Box::new(b)), e.span()),
        );

        // Logical OR
        and.clone().foldl_with(
            just(Token::Or).ignore_then(and).repeated(),
            |a, b, e| (Expr::Binary(Box::new(a), BinaryOp::Or, Box::new(b)), e.span()),
        )
    })
}

/// Parse an identifier
fn ident<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Spanned<&'src str>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    select! { Token::Ident(s) => s }
        .map_with(|s, e| (s, e.span()))
}

/// Parse an identifier (raw, without span)
fn ident_raw<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, &'src str, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    select! { Token::Ident(s) => s }
}

/// Parse a string literal with interpolation support
fn string_literal<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Spanned<StringLiteral<'src>>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    select! { Token::String(s) => s }
        .map_with(|s, e| {
            let span: Span = e.span();
            // Offset by 1 to account for opening quote
            let str_literal = parse_interpolated_string(s, span.start);
            (str_literal, span)
        })
}

/// Parse a string literal (raw, with span on the raw string)
fn string_literal_raw<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Spanned<&'src str>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    select! { Token::String(s) => s }
        .map_with(|s, e| (s, e.span()))
}

/// Parse a literal value
fn literal<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Spanned<Literal<'src>>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    choice((
        select! { Token::String(s) => Literal::String(StringLiteral { parts: vec![StringPart::Text(s)] }) },
        select! { Token::Int(n) => Literal::Int(n) },
        select! { Token::Float(n) => Literal::Float(n) },
        select! { Token::Bool(b) => Literal::Bool(b) },
        just(Token::Null).to(Literal::Null),
    ))
    .map_with(|l, e| (l, e.span()))
}

/// Parse a multiline string
fn multiline_string<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Spanned<&'src str>, extra::Err<Rich<'tokens, Token<'src>, Span>>> + Clone
where
    I: ValueInput<'tokens, Token = Token<'src>, Span = Span>,
{
    select! { Token::MultilineString(s) => s }
        .map_with(|s, e| (s, e.span()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lexer;

    fn parse(input: &str) -> Program<'_> {
        let (tokens, lex_errors) = lexer().parse(input).into_output_errors();
        if !lex_errors.is_empty() {
            panic!("Lexer errors: {:?}", lex_errors);
        }
        let tokens = tokens.unwrap();

        let len = input.len();
        let (ast, parse_errors) = parser()
            .parse(tokens.as_slice().map((len..len).into(), |(t, s)| (t, s)))
            .into_output_errors();

        if !parse_errors.is_empty() {
            panic!("Parser errors: {:?}", parse_errors);
        }
        ast.unwrap()
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
                assert_eq!(decl.values.len(), 3);
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
            StatementKind::Exclude((table, _)) => assert_eq!(*table, "payments"),
            _ => panic!("Expected Exclude"),
        }
        match &program.statements[1].0.kind {
            StatementKind::Ignore((table, _)) => assert_eq!(*table, "logs"),
            _ => panic!("Expected Ignore"),
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
    fn test_include() {
        let program = parse(r#"include orders where "id = 123""#);
        assert_eq!(program.statements.len(), 1);
        match &program.statements[0].0.kind {
            StatementKind::Include(stmt) => {
                assert_eq!(stmt.aggregate.0, "orders");
            }
            _ => panic!("Expected Include"),
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
                        assert!(matches!(&s.parts[0], StringPart::Text("https://")));
                        assert!(matches!(&s.parts[1], StringPart::Interpolation(_)));
                        assert!(matches!(&s.parts[2], StringPart::Text(":")));
                        assert!(matches!(&s.parts[3], StringPart::Interpolation(_)));
                        assert!(matches!(&s.parts[4], StringPart::Text("/")));
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
}
