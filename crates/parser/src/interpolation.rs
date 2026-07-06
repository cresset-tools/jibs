//! String interpolation parser
//!
//! Parses strings containing `{$variable}` or `{expression}` interpolations,
//! decoding escape sequences along the way. Interpolation expressions are
//! lexed with the main lexer (regex literals disabled — `/` is always
//! division here) and parsed with the main expression parser, so the two
//! expression dialects can't drift apart.

use std::borrow::Cow;

use crate::ast::{Expr, Spanned, StringLiteral, StringPart};
use crate::lexer::{lex_with_options, LexOptions};
use crate::parser::parse_expr_tokens;
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
                        span: Span::new(base_offset + interp_start, base_offset + input.len()),
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

    let adjust = |span: Span| -> Span { Span::new(offset + span.start, offset + span.end) };

    // Regex literals are disabled inside interpolations: `/` is division
    let (tokens, lex_errors) =
        lex_with_options(input, LexOptions { regex_literals: false });

    if let Some(e) = lex_errors.first() {
        return Err(InterpolationError {
            message: format!("invalid interpolation: {}", e.message),
            span: adjust(Span::new(e.span.start, e.span.end)),
        });
    }

    if tokens.is_empty() {
        return Err(InterpolationError {
            message: "empty interpolation (write \\{ for a literal brace)".to_string(),
            span: Span::new(offset, offset + input.len()),
        });
    }

    let eoi = Span::new(input.len(), input.len());
    match parse_expr_tokens(&tokens, eoi) {
        Ok((expr, span)) => Ok((expr, adjust(span))),
        Err(e) => Err(InterpolationError {
            message: format!("invalid interpolation: {}{}", e.message, bare_ident_hint()),
            span: adjust(Span::new(e.span.start, e.span.end)),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BinaryOp, UnaryOp};

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
    fn test_keyword_variable_names_in_interpolation() {
        // `limit` is a statement keyword, but a fine variable name inside
        // an interpolation
        let result = parse_ok("{$limit}");
        match &result.parts[0] {
            StringPart::Interpolation((expr, _)) => {
                assert_eq!(*expr, Expr::Variable("limit"));
            }
            _ => panic!("Expected Interpolation"),
        }
    }

    #[test]
    fn test_division_in_interpolation() {
        // Regex literals are disabled inside interpolations, so unspaced
        // division with multiple slashes works
        let result = parse_ok("{$a/$b/$c}");
        match &result.parts[0] {
            StringPart::Interpolation((expr, _)) => {
                assert!(matches!(expr, Expr::Binary(_, BinaryOp::Div, _)));
            }
            _ => panic!("Expected Interpolation"),
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
}
