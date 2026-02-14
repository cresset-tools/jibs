//! String interpolation parser
//!
//! Parses strings containing `{$variable}` or `{expression}` interpolations.

use chumsky::{input::ValueInput, prelude::*};

use crate::ast::{BinaryOp, Expr, Literal, Spanned, StringLiteral, StringPart, UnaryOp};
use crate::Span;

/// Parse a string that may contain interpolations.
///
/// Input is the raw string content (without outer quotes).
/// Returns a StringLiteral with Text and Interpolation parts.
pub fn parse_interpolated_string(input: &str, base_offset: usize) -> StringLiteral<'_> {
    let mut parts = Vec::new();
    let mut chars = input.char_indices().peekable();
    let mut text_start = 0;

    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => {
                // Escape sequence - skip next char
                if let Some((j, escaped)) = chars.next() {
                    if escaped == '{' {
                        // \{ - add text before this, then continue from the {
                        if text_start < i {
                            parts.push(StringPart::Text(&input[text_start..i]));
                        }
                        // The escaped brace - we'll include it in the next text segment
                        text_start = j; // Start from the {
                    }
                }
            }
            '{' => {
                // Start of interpolation - find matching }
                let interp_start = i;
                let mut depth = 1;
                let mut interp_end = i + 1;

                for (j, c2) in chars.by_ref() {
                    match c2 {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                interp_end = j;
                                break;
                            }
                        }
                        _ => {}
                    }
                }

                // Add text before interpolation
                if text_start < interp_start {
                    parts.push(StringPart::Text(&input[text_start..interp_start]));
                }

                // Parse the interpolation content
                let interp_content = &input[interp_start + 1..interp_end];
                let interp_offset = base_offset + interp_start + 1;

                if let Some(expr) = parse_interpolation_expr(interp_content, interp_offset) {
                    parts.push(StringPart::Interpolation(expr));
                } else {
                    // Failed to parse - treat as literal text
                    parts.push(StringPart::Text(&input[interp_start..interp_end + 1]));
                }

                text_start = interp_end + 1;
            }
            _ => {}
        }
    }

    // Add remaining text
    if text_start < input.len() {
        parts.push(StringPart::Text(&input[text_start..]));
    }

    // If no parts, add empty text
    if parts.is_empty() {
        parts.push(StringPart::Text(""));
    }

    StringLiteral { parts }
}

/// Parse an expression inside an interpolation `{...}`
fn parse_interpolation_expr(input: &str, offset: usize) -> Option<Spanned<Expr<'_>>> {
    let (tokens, lex_errors) = interp_lexer().parse(input).into_output_errors();

    if !lex_errors.is_empty() {
        return None;
    }

    let tokens = tokens?;

    if tokens.is_empty() {
        return None;
    }

    let len = input.len();
    let eoi_span: Span = (len..len).into();

    let (expr, parse_errors) = interp_expr_parser()
        .parse(tokens.as_slice().map(eoi_span, |(t, s)| (t, s)))
        .into_output_errors();

    if !parse_errors.is_empty() {
        return None;
    }

    // Adjust span to account for position in original string
    expr.map(|(e, span)| {
        let adjusted_span: Span = (offset + span.start..offset + span.end).into();
        (e, adjusted_span)
    })
}

/// Token type for interpolation expressions
#[derive(Clone, Debug, PartialEq)]
pub enum InterpToken<'src> {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(&'src str),
    Ident(&'src str),
    Dollar,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    And,
    Or,
    Not,
    LParen,
    RParen,
}

/// Lexer for interpolation expressions
fn interp_lexer<'src>(
) -> impl Parser<'src, &'src str, Vec<Spanned<InterpToken<'src>>>, extra::Err<Rich<'src, char, Span>>>
{
    let int = just('-')
        .or_not()
        .then(text::int(10))
        .to_slice()
        .from_str()
        .unwrapped()
        .map(InterpToken::Int);

    let float = text::int(10)
        .then(just('.').then(text::digits(10)))
        .to_slice()
        .from_str()
        .unwrapped()
        .map(InterpToken::Float);

    let string = just('"')
        .ignore_then(none_of('"').repeated().to_slice())
        .then_ignore(just('"'))
        .map(InterpToken::String);

    let ident = text::ascii::ident().map(|s: &str| match s {
        "true" => InterpToken::Bool(true),
        "false" => InterpToken::Bool(false),
        _ => InterpToken::Ident(s),
    });

    // Multi-char operators
    let eq = just("==").to(InterpToken::Eq);
    let not_eq = just("!=").to(InterpToken::NotEq);
    let lt_eq = just("<=").to(InterpToken::LtEq);
    let gt_eq = just(">=").to(InterpToken::GtEq);
    let and = just("&&").to(InterpToken::And);
    let or = just("||").to(InterpToken::Or);

    // Single-char tokens
    let dollar = just('$').to(InterpToken::Dollar);
    let plus = just('+').to(InterpToken::Plus);
    let minus = just('-').to(InterpToken::Minus);
    let star = just('*').to(InterpToken::Star);
    let slash = just('/').to(InterpToken::Slash);
    let percent = just('%').to(InterpToken::Percent);
    let lt = just('<').to(InterpToken::Lt);
    let gt = just('>').to(InterpToken::Gt);
    let not = just('!').to(InterpToken::Not);
    let lparen = just('(').to(InterpToken::LParen);
    let rparen = just(')').to(InterpToken::RParen);

    let operator = choice((
        eq, not_eq, lt_eq, gt_eq, and, or, dollar, plus, minus, star, slash, percent, lt, gt, not,
        lparen, rparen,
    ));

    let token = choice((float, int, string, operator, ident));

    token
        .map_with(|tok, e| (tok, e.span()))
        .padded()
        .repeated()
        .collect()
}

/// Expression parser for interpolations
fn interp_expr_parser<'tokens, 'src: 'tokens, I>(
) -> impl Parser<'tokens, I, Spanned<Expr<'src>>, extra::Err<Rich<'tokens, InterpToken<'src>, Span>>>
       + Clone
where
    I: ValueInput<'tokens, Token = InterpToken<'src>, Span = Span>,
{
    recursive(|expr| {
        // Primary expressions
        let primary = choice((
            // Variable reference $name
            just(InterpToken::Dollar)
                .ignore_then(select! { InterpToken::Ident(s) => s })
                .map(Expr::Variable),
            // Literals
            select! {
                InterpToken::Int(n) => Expr::Literal(Literal::Int(n)),
                InterpToken::Float(n) => Expr::Literal(Literal::Float(n)),
                InterpToken::Bool(b) => Expr::Literal(Literal::Bool(b)),
                InterpToken::String(s) => Expr::Literal(Literal::String(StringLiteral {
                    parts: vec![StringPart::Text(s)],
                })),
            },
            // Parenthesized
            expr.clone()
                .delimited_by(just(InterpToken::LParen), just(InterpToken::RParen))
                .map(|(e, _)| e),
        ))
        .map_with(|e, ctx| (e, ctx.span()));

        // Unary
        let unary = just(InterpToken::Not)
            .to(UnaryOp::Not)
            .or(just(InterpToken::Minus).to(UnaryOp::Neg))
            .repeated()
            .foldr_with(primary, |op, e, ctx| {
                (Expr::Unary(op, Box::new(e)), ctx.span())
            });

        // Multiplicative
        let op = just(InterpToken::Star)
            .to(BinaryOp::Mul)
            .or(just(InterpToken::Slash).to(BinaryOp::Div))
            .or(just(InterpToken::Percent).to(BinaryOp::Mod));
        let multiplicative = unary.clone().foldl_with(op.then(unary).repeated(), |a, (op, b), e| {
            (Expr::Binary(Box::new(a), op, Box::new(b)), e.span())
        });

        // Additive
        let op = just(InterpToken::Plus)
            .to(BinaryOp::Add)
            .or(just(InterpToken::Minus).to(BinaryOp::Sub));
        let additive = multiplicative
            .clone()
            .foldl_with(op.then(multiplicative).repeated(), |a, (op, b), e| {
                (Expr::Binary(Box::new(a), op, Box::new(b)), e.span())
            });

        // Comparison
        let op = just(InterpToken::Lt)
            .to(BinaryOp::Lt)
            .or(just(InterpToken::Gt).to(BinaryOp::Gt))
            .or(just(InterpToken::LtEq).to(BinaryOp::LtEq))
            .or(just(InterpToken::GtEq).to(BinaryOp::GtEq));
        let comparison = additive
            .clone()
            .foldl_with(op.then(additive).repeated(), |a, (op, b), e| {
                (Expr::Binary(Box::new(a), op, Box::new(b)), e.span())
            });

        // Equality
        let op = just(InterpToken::Eq)
            .to(BinaryOp::Eq)
            .or(just(InterpToken::NotEq).to(BinaryOp::NotEq));
        let equality = comparison
            .clone()
            .foldl_with(op.then(comparison).repeated(), |a, (op, b), e| {
                (Expr::Binary(Box::new(a), op, Box::new(b)), e.span())
            });

        // Logical AND
        let and_expr = equality.clone().foldl_with(
            just(InterpToken::And).ignore_then(equality).repeated(),
            |a, b, e| (Expr::Binary(Box::new(a), BinaryOp::And, Box::new(b)), e.span()),
        );

        // Logical OR
        and_expr.clone().foldl_with(
            just(InterpToken::Or).ignore_then(and_expr).repeated(),
            |a, b, e| (Expr::Binary(Box::new(a), BinaryOp::Or, Box::new(b)), e.span()),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plain_string() {
        let result = parse_interpolated_string("hello world", 0);
        assert_eq!(result.parts.len(), 1);
        match &result.parts[0] {
            StringPart::Text(s) => assert_eq!(*s, "hello world"),
            _ => panic!("Expected Text"),
        }
    }

    #[test]
    fn test_simple_variable() {
        let result = parse_interpolated_string("hello {$name}", 0);
        assert_eq!(result.parts.len(), 2);
        match &result.parts[0] {
            StringPart::Text(s) => assert_eq!(*s, "hello "),
            _ => panic!("Expected Text"),
        }
        match &result.parts[1] {
            StringPart::Interpolation((expr, _)) => match expr {
                Expr::Variable(name) => assert_eq!(*name, "name"),
                _ => panic!("Expected Variable"),
            },
            _ => panic!("Expected Interpolation"),
        }
    }

    #[test]
    fn test_multiple_interpolations() {
        let result = parse_interpolated_string("{$greeting} {$name}!", 0);
        assert_eq!(result.parts.len(), 4);
        // {$greeting}
        assert!(matches!(&result.parts[0], StringPart::Interpolation(_)));
        // " "
        match &result.parts[1] {
            StringPart::Text(s) => assert_eq!(*s, " "),
            _ => panic!("Expected Text"),
        }
        // {$name}
        assert!(matches!(&result.parts[2], StringPart::Interpolation(_)));
        // "!"
        match &result.parts[3] {
            StringPart::Text(s) => assert_eq!(*s, "!"),
            _ => panic!("Expected Text"),
        }
    }

    #[test]
    fn test_expression_interpolation() {
        let result = parse_interpolated_string("port: {$base_port + 1}", 0);
        assert_eq!(result.parts.len(), 2);
        match &result.parts[1] {
            StringPart::Interpolation((expr, _)) => match expr {
                Expr::Binary(_, BinaryOp::Add, _) => {}
                _ => panic!("Expected Binary Add"),
            },
            _ => panic!("Expected Interpolation"),
        }
    }

    #[test]
    fn test_escaped_brace() {
        let result = parse_interpolated_string("use \\{$var} syntax", 0);
        // Should have: "use " + "{$var} syntax" (escaped brace becomes literal)
        assert_eq!(result.parts.len(), 2);
        match &result.parts[0] {
            StringPart::Text(s) => assert_eq!(*s, "use "),
            _ => panic!("Expected Text"),
        }
        match &result.parts[1] {
            StringPart::Text(s) => assert_eq!(*s, "{$var} syntax"),
            _ => panic!("Expected Text for escaped brace"),
        }
    }

    #[test]
    fn test_complex_expression() {
        let result = parse_interpolated_string("{$a * 2 + $b}", 0);
        assert_eq!(result.parts.len(), 1);
        match &result.parts[0] {
            StringPart::Interpolation((expr, _)) => match expr {
                Expr::Binary(_, BinaryOp::Add, _) => {}
                _ => panic!("Expected Binary Add at top level"),
            },
            _ => panic!("Expected Interpolation"),
        }
    }

    #[test]
    fn test_url_interpolation() {
        let result = parse_interpolated_string("https://{$domain}:{$port}/", 0);
        assert_eq!(result.parts.len(), 5);
        // "https://"
        match &result.parts[0] {
            StringPart::Text(s) => assert_eq!(*s, "https://"),
            _ => panic!("Expected Text"),
        }
        // {$domain}
        assert!(matches!(&result.parts[1], StringPart::Interpolation(_)));
        // ":"
        match &result.parts[2] {
            StringPart::Text(s) => assert_eq!(*s, ":"),
            _ => panic!("Expected Text"),
        }
        // {$port}
        assert!(matches!(&result.parts[3], StringPart::Interpolation(_)));
        // "/"
        match &result.parts[4] {
            StringPart::Text(s) => assert_eq!(*s, "/"),
            _ => panic!("Expected Text"),
        }
    }
}
