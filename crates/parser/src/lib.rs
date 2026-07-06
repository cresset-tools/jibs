//! Jibs Parser - Jelle's Importer with Better Speed
//!
//! A domain-specific language for directing MySQL database imports from remote
//! (production) databases to local development environments.
//!
//! The lexer and parser are hand-written (recursive descent). An earlier
//! chumsky-based implementation took ~9 minutes to compile in debug mode due
//! to combinator type blowup; this one compiles in seconds and gives us full
//! control over error messages and recovery.

pub mod ast;
pub mod interpolation;
pub mod lexer;
pub mod parser;

/// A span in the source code (byte offsets)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub fn into_range(self) -> std::ops::Range<usize> {
        self.start..self.end
    }
}

impl From<std::ops::Range<usize>> for Span {
    fn from(r: std::ops::Range<usize>) -> Self {
        Self { start: r.start, end: r.end }
    }
}

impl std::fmt::Display for Span {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}

/// Parse a DSL source string into an AST
pub fn parse(source: &str) -> Result<ast::Program<'_>, Vec<ParseError>> {
    let (tokens, mut errors) = lexer::lex(source);

    let eoi = Span::new(source.len(), source.len());
    let (program, parse_errors) = parser::parse_tokens(&tokens, eoi);
    errors.extend(parse_errors);

    if errors.is_empty() {
        Ok(program)
    } else {
        Err(errors)
    }
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
