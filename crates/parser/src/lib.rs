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

/// Render parse errors as human-readable reports with source snippets,
/// line/column numbers, and labeled spans.
///
/// `color` enables ANSI colors (pass `stderr().is_terminal()` for CLI use,
/// `false` when embedding the result in an error message or log).
pub fn render_errors(filename: &str, source: &str, errors: &[ParseError], color: bool) -> String {
    use ariadne::{Color, Config, Label, Report, ReportKind, Source};

    // ariadne 0.4 indexes by character, our spans are byte offsets
    let byte_to_char = |byte: usize| -> usize {
        let mut byte = byte.min(source.len());
        while byte > 0 && !source.is_char_boundary(byte) {
            byte -= 1;
        }
        source[..byte].chars().count()
    };

    let mut out = Vec::new();
    for error in errors {
        let range = byte_to_char(error.span.start)..byte_to_char(error.span.end);
        let result = Report::build(ReportKind::Error, filename, range.start)
            .with_config(Config::default().with_color(color))
            .with_message(&error.message)
            .with_label(
                Label::new((filename, range))
                    .with_message(&error.message)
                    .with_color(Color::Red),
            )
            .finish()
            .write((filename, Source::from(source)), &mut out);
        if result.is_err() {
            // Fall back to the plain message if rendering fails
            out.extend_from_slice(format!("{}\n", error).as_bytes());
        }
    }
    String::from_utf8_lossy(&out).into_owned()
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

#[cfg(test)]
mod tests {
    #[test]
    fn render_errors_shows_snippet_and_location() {
        let source = "var x: int = 99999999999999999999\n";
        let errors = crate::parse(source).unwrap_err();
        let rendered = crate::render_errors("test.jibs", source, &errors, false);
        assert!(rendered.contains("out of range"), "message: {}", rendered);
        assert!(rendered.contains("test.jibs:1:14"), "location: {}", rendered);
        assert!(rendered.contains("var x: int ="), "snippet: {}", rendered);
    }

    #[test]
    fn render_errors_handles_non_ascii_sources() {
        // The error span sits after multibyte characters; ariadne indexes by
        // character, so byte offsets must be converted (this used to panic
        // or misplace labels)
        let source = "// commentaire avec des caractères accentués\nvar x: int = 99999999999999999999\n";
        let errors = crate::parse(source).unwrap_err();
        let rendered = crate::render_errors("test.jibs", source, &errors, false);
        assert!(rendered.contains("out of range"));
        assert!(rendered.contains("test.jibs:2:14"), "location: {}", rendered);
    }
}
