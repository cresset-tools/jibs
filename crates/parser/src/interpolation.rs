//! String interpolation parser
//!
//! Parses strings containing `{$variable}` or `{expression}` interpolations.

use std::borrow::Cow;

use chumsky::{input::ValueInput, prelude::*};

use crate::ast::{BinaryOp, Expr, Literal, Spanned, StringLiteral, StringPart, UnaryOp};
use crate::Span;

/// An error inside a string literal (bad escape handling is lenient, so in
/// practice: unclosed or unparseable interpolations). The span is absolute
/// (already offset into the source file).
#[derive(Debug, Clone)]
pub struct InterpolationError {
    pub message: String,
    pub span: Span,
}

/// Parse a string that may contain interpolations, decoding escape sequences.
///
/// Input is the raw string content (without outer quotes); `base_offset` is
/// the source position of that content (used for error spans).
///
/// Escapes: `\\`, `\"`, `\n`, `\t` decode to their usual meaning and `\{`
/// to a literal brace. Any other `\<char>` is kept as-is (both characters),
/// so SQL escape sequences like `\%` pass through to MySQL untouched.
///
/// A malformed interpolation is an error, not literal text: these strings
/// usually become SQL, and a typo like `${var}` silently reaching the
/// database is far worse than a parse error.
pub fn parse_interpolated_string(
    input: &str,
    base_offset: usize,
) -> Result<StringLiteral<'_>, Vec<InterpolationError>> {
    let mut parts: Vec<StringPart<'_>> = Vec::new();
    let mut errors: Vec<InterpolationError> = Vec::new();
    let mut chars = input.char_indices();
    let mut text = String::new();

    fn flush<'src>(text: &mut String, parts: &mut Vec<StringPart<'src>>) {
        if !text.is_empty() {
            parts.push(StringPart::Text(Cow::Owned(std::mem::take(text))));
        }
    }

    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some((_, '\\')) => text.push('\\'),
                Some((_, '"')) => text.push('"'),
                Some((_, 'n')) => text.push('\n'),
                Some((_, 't')) => text.push('\t'),
                Some((_, '{')) => text.push('{'),
                Some((_, other)) => {
                    // Unknown escape: keep both characters
                    text.push('\\');
                    text.push(other);
                }
                None => text.push('\\'),
            },
            '{' => {
                // Start of interpolation - find matching }
                let interp_start = i;
                let mut depth = 1;
                let mut interp_end = None;

                for (j, c2) in chars.by_ref() {
                    match c2 {
                        '{' => depth += 1,
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                interp_end = Some(j);
                                break;
                            }
                        }
                        _ => {}
                    }
                }

                let Some(interp_end) = interp_end else {
                    errors.push(InterpolationError {
                        message: "unclosed interpolation: missing '}' \
                                  (write \\{ for a literal brace)"
                            .to_string(),
                        span: (base_offset + interp_start..base_offset + input.len()).into(),
                    });
                    break;
                };

                flush(&mut text, &mut parts);

                // Parse the interpolation content
                let interp_content = &input[interp_start + 1..interp_end];
                let interp_offset = base_offset + interp_start + 1;

                match parse_interpolation_expr(interp_content, interp_offset) {
                    Ok(expr) => parts.push(StringPart::Interpolation(expr)),
                    Err(err) => errors.push(err),
                }
            }
            other => text.push(other),
        }
    }

    flush(&mut text, &mut parts);

    // If no parts, add empty text
    if parts.is_empty() {
        parts.push(StringPart::Text(Cow::Borrowed("")));
    }

    if errors.is_empty() {
        Ok(StringLiteral { parts })
    } else {
        Err(errors)
    }
}

/// Parse an expression inside an interpolation `{...}`
fn parse_interpolation_expr(
    input: &str,
    offset: usize,
) -> Result<Spanned<Expr<'_>>, InterpolationError> {
    // Hint for the most common typo: `{name}` instead of `{$name}`
    let bare_ident_hint = || {
        let trimmed = input.trim();
        let is_bare_ident = !trimmed.is_empty()
            && trimmed
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && trimmed.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if is_bare_ident {
            format!(" (did you mean {{${}}}?)", trimmed)
        } else {
            String::new()
        }
    };

    let adjust = |span: Span| -> Span { (offset + span.start..offset + span.end).into() };

    let (tokens, lex_errors) = interp_lexer().parse(input).into_output_errors();

    if let Some(e) = lex_errors.first() {
        return Err(InterpolationError {
            message: format!("invalid interpolation: {}", e),
            span: adjust(*e.span()),
        });
    }

    let tokens = tokens.unwrap_or_default();

    if tokens.is_empty() {
        return Err(InterpolationError {
            message: "empty interpolation (write \\{ for a literal brace)".to_string(),
            span: (offset..offset + input.len()).into(),
        });
    }

    let len = input.len();
    let eoi_span: Span = (len..len).into();

    let (expr, parse_errors) = interp_expr_parser()
        .parse(tokens.as_slice().map(eoi_span, |(t, s)| (t, s)))
        .into_output_errors();

    if let Some(e) = parse_errors.first() {
        return Err(InterpolationError {
            message: format!("invalid interpolation: {}{}", e, bare_ident_hint()),
            span: adjust(*e.span()),
        });
    }

    let (e, span) = expr.ok_or_else(|| InterpolationError {
        message: format!("invalid interpolation{}", bare_ident_hint()),
        span: (offset..offset + input.len()).into(),
    })?;

    // Adjust span to account for position in original string
    Ok((e, adjust(span)))
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

impl std::fmt::Display for InterpToken<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            InterpToken::Int(n) => write!(f, "{n}"),
            InterpToken::Float(n) => write!(f, "{n}"),
            InterpToken::Bool(b) => write!(f, "{b}"),
            InterpToken::String(s) => write!(f, "\"{s}\""),
            InterpToken::Ident(s) => write!(f, "{s}"),
            InterpToken::Dollar => write!(f, "$"),
            InterpToken::Plus => write!(f, "+"),
            InterpToken::Minus => write!(f, "-"),
            InterpToken::Star => write!(f, "*"),
            InterpToken::Slash => write!(f, "/"),
            InterpToken::Percent => write!(f, "%"),
            InterpToken::Eq => write!(f, "=="),
            InterpToken::NotEq => write!(f, "!="),
            InterpToken::Lt => write!(f, "<"),
            InterpToken::Gt => write!(f, ">"),
            InterpToken::LtEq => write!(f, "<="),
            InterpToken::GtEq => write!(f, ">="),
            InterpToken::And => write!(f, "&&"),
            InterpToken::Or => write!(f, "||"),
            InterpToken::Not => write!(f, "!"),
            InterpToken::LParen => write!(f, "("),
            InterpToken::RParen => write!(f, ")"),
        }
    }
}

/// Lexer for interpolation expressions
fn interp_lexer<'src>(
) -> impl Parser<'src, &'src str, Vec<Spanned<InterpToken<'src>>>, extra::Err<Rich<'src, char, Span>>>
{
    // No leading sign on numbers: `-` is the Minus token and the expression
    // parser's unary Neg handles negation, so `{$a-1}` lexes as a binary
    // minus instead of the literal -1. `validate` keeps the overflow message
    // from being merged away by the surrounding `choice`.
    let int = text::int(10)
        .to_slice()
        .validate(|s: &str, e, emitter| {
            s.parse::<i64>().unwrap_or_else(|_| {
                emitter.emit(Rich::custom(
                    e.span(),
                    format!("integer literal '{}' is out of range for a 64-bit integer", s),
                ));
                i64::MAX
            })
        })
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
            // unique() function call
            select! { InterpToken::Ident("unique") => () }
                .ignore_then(just(InterpToken::LParen))
                .ignore_then(just(InterpToken::RParen))
                .to(Expr::Unique),
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
                    parts: vec![StringPart::Text(Cow::Borrowed(s))],
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

    fn parse_ok(input: &str) -> StringLiteral<'_> {
        parse_interpolated_string(input, 0).expect("expected successful parse")
    }

    #[test]
    fn test_plain_string() {
        let result = parse_ok("hello world");
        assert_eq!(result.parts.len(), 1);
        match &result.parts[0] {
            StringPart::Text(s) => assert_eq!(*s, "hello world"),
            _ => panic!("Expected Text"),
        }
    }

    #[test]
    fn test_simple_variable() {
        let result = parse_ok("hello {$name}");
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
        let result = parse_ok("{$greeting} {$name}!");
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
        let result = parse_ok("port: {$base_port + 1}");
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
        let result = parse_ok("use \\{$var} syntax");
        // Escaped brace decodes to a literal '{' within one text part
        assert_eq!(result.parts.len(), 1);
        match &result.parts[0] {
            StringPart::Text(s) => assert_eq!(*s, "use {$var} syntax"),
            _ => panic!("Expected Text for escaped brace"),
        }
    }

    #[test]
    fn test_escape_sequences_decode() {
        let result = parse_ok(r#"a\nb\tc\"d\\e"#);
        assert_eq!(result.parts.len(), 1);
        match &result.parts[0] {
            StringPart::Text(s) => assert_eq!(*s, "a\nb\tc\"d\\e"),
            _ => panic!("Expected Text"),
        }
    }

    #[test]
    fn test_unknown_escape_passes_through() {
        // \% is not a defined escape: both characters are kept so SQL
        // LIKE patterns survive intact
        let result = parse_ok(r"path LIKE 'a\%'");
        match &result.parts[0] {
            StringPart::Text(s) => assert_eq!(*s, r"path LIKE 'a\%'"),
            _ => panic!("Expected Text"),
        }
    }

    #[test]
    fn test_unclosed_interpolation_is_error_not_panic() {
        // Previously panicked with a byte-index-out-of-range
        let result = parse_interpolated_string("foo{", 0);
        let errors = result.expect_err("unclosed brace must be an error");
        assert!(errors[0].message.contains("unclosed"));

        // Multibyte char after the brace previously hit a char-boundary panic
        let result = parse_interpolated_string("a{é", 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_bad_interpolation_is_error_with_hint() {
        // `${var}` typo: the `$` lands outside, `{user_id}` is a bare ident
        let result = parse_interpolated_string("user_id = ${user_id}", 0);
        let errors = result.expect_err("bare identifier interpolation must be an error");
        assert!(
            errors[0].message.contains("did you mean {$user_id}?"),
            "expected hint in: {}",
            errors[0].message
        );
    }

    #[test]
    fn test_interpolation_int_overflow_is_error() {
        let result = parse_interpolated_string("{99999999999999999999}", 0);
        let errors = result.expect_err("overflowing literal must be an error");
        assert!(errors[0].message.contains("out of range"));
    }

    #[test]
    fn test_binary_minus_without_spaces() {
        // `-` no longer folds into the integer literal
        let result = parse_ok("{$base_port-1}");
        assert_eq!(result.parts.len(), 1);
        match &result.parts[0] {
            StringPart::Interpolation((expr, _)) => {
                assert!(matches!(expr, Expr::Binary(_, BinaryOp::Sub, _)));
            }
            _ => panic!("Expected Interpolation"),
        }
    }

    #[test]
    fn test_negative_float_in_interpolation() {
        let result = parse_ok("{-0.5}");
        match &result.parts[0] {
            StringPart::Interpolation((expr, _)) => {
                assert!(matches!(expr, Expr::Unary(UnaryOp::Neg, _)));
            }
            _ => panic!("Expected Interpolation"),
        }
    }

    #[test]
    fn test_complex_expression() {
        let result = parse_ok("{$a * 2 + $b}");
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
        let result = parse_ok("https://{$domain}:{$port}/");
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

    #[test]
    fn test_unique_function() {
        let result = parse_ok("user{unique()}@example.test");
        assert_eq!(result.parts.len(), 3);
        match &result.parts[0] {
            StringPart::Text(s) => assert_eq!(*s, "user"),
            _ => panic!("Expected Text"),
        }
        match &result.parts[1] {
            StringPart::Interpolation((expr, _)) => {
                assert!(matches!(expr, Expr::Unique));
            }
            _ => panic!("Expected Interpolation with Unique"),
        }
        match &result.parts[2] {
            StringPart::Text(s) => assert_eq!(*s, "@example.test"),
            _ => panic!("Expected Text"),
        }
    }
}
