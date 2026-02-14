//! MySQL Import DSL Parser
//!
//! A domain-specific language for directing MySQL database imports from remote
//! (production) databases to local development environments.

use chumsky::prelude::*;

pub mod ast;
pub mod interpolation;
pub mod lexer;
pub mod parser;

/// A span in the source code
pub type Span = SimpleSpan;

/// Parse a DSL source string into an AST
pub fn parse(source: &str) -> Result<ast::Program<'_>, Vec<ParseError>> {
    let (tokens, lex_errors) = lexer::lexer().parse(source).into_output_errors();

    let mut errors: Vec<ParseError> = lex_errors
        .into_iter()
        .map(|e| ParseError {
            span: e.span().into_range(),
            message: e.to_string(),
        })
        .collect();

    if let Some(tokens) = tokens {
        let len = source.len();
        let (ast, parse_errors) = parser::parser()
            .parse(
                tokens
                    .as_slice()
                    .map((len..len).into(), |(t, s)| (t, s)),
            )
            .into_output_errors();

        errors.extend(parse_errors.into_iter().map(|e| ParseError {
            span: e.span().into_range(),
            message: e.to_string(),
        }));

        if errors.is_empty() {
            return Ok(ast.unwrap());
        }
    }

    Err(errors)
}

/// A parse error with span information
#[derive(Debug, Clone)]
pub struct ParseError {
    pub span: std::ops::Range<usize>,
    pub message: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error at {}..{}: {}", self.span.start, self.span.end, self.message)
    }
}

impl std::error::Error for ParseError {}
