//! Lexer for the MySQL Import DSL (hand-written scanner)

use std::fmt;

use crate::ast::Spanned;
use crate::{ParseError, Span};

/// Token types for the MySQL Import DSL
#[derive(Clone, Debug, PartialEq)]
pub enum Token<'src> {
    // Literals
    Int(i64),
    Float(f64),
    Bool(bool),
    String(&'src str),
    MultilineString(&'src str),
    Regex(&'src str),

    // Identifiers and keywords
    Ident(&'src str),

    // Keywords
    Import,
    Var,
    Faker,
    Relation,
    Anonymize,
    ExcludeData,
    IgnoreTable,
    IgnoreRelation,
    Aggregate,
    Get,
    Preserve,
    Set,
    After,
    Root,
    Where,
    Order,
    By,
    Limit,
    Full,
    Exclude,
    RootOnly,
    Match,
    When,
    Null,

    // Types
    TypeString,
    TypeInt,
    TypeFloat,
    TypeBool,

    // Sort direction
    Asc,
    Desc,

    // Operators
    Arrow,      // ->
    Spread,     // ...
    Eq,         // ==
    NotEq,      // !=
    LtEq,       // <=
    GtEq,       // >=
    And,        // &&
    Or,         // ||
    Lt,         // <
    Gt,         // >
    Not,        // !
    Plus,       // +
    Minus,      // -
    Star,       // *
    Slash,      // /
    Percent,    // %
    Assign,     // =

    // Delimiters
    LBrace,     // {
    RBrace,     // }
    LBracket,   // [
    RBracket,   // ]
    LParen,     // (
    RParen,     // )

    // Punctuation
    Colon,      // :
    Comma,      // ,
    Dot,        // .
    Hash,       // #
    Dollar,     // $
}

impl fmt::Display for Token<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::Int(n) => write!(f, "{n}"),
            Token::Float(n) => write!(f, "{n}"),
            Token::Bool(b) => write!(f, "{b}"),
            Token::String(s) => write!(f, "\"{s}\""),
            Token::MultilineString(s) => write!(f, "\"\"\"{s}\"\"\""),
            Token::Regex(s) => write!(f, "/{s}/"),
            Token::Ident(s) => write!(f, "{s}"),
            Token::Import => write!(f, "import"),
            Token::Var => write!(f, "var"),
            Token::Faker => write!(f, "faker"),
            Token::Relation => write!(f, "relation"),
            Token::Anonymize => write!(f, "anonymize"),
            Token::ExcludeData => write!(f, "exclude_data"),
            Token::IgnoreTable => write!(f, "ignore_table"),
            Token::IgnoreRelation => write!(f, "ignore_relation"),
            Token::Aggregate => write!(f, "aggregate"),
            Token::Get => write!(f, "get"),
            Token::Preserve => write!(f, "preserve"),
            Token::Set => write!(f, "set"),
            Token::After => write!(f, "after"),
            Token::Root => write!(f, "root"),
            Token::Where => write!(f, "where"),
            Token::Order => write!(f, "order"),
            Token::By => write!(f, "by"),
            Token::Limit => write!(f, "limit"),
            Token::Full => write!(f, "full"),
            Token::Exclude => write!(f, "exclude"),
            Token::RootOnly => write!(f, "root_only"),
            Token::Match => write!(f, "match"),
            Token::When => write!(f, "when"),
            Token::Null => write!(f, "null"),
            Token::TypeString => write!(f, "string"),
            Token::TypeInt => write!(f, "int"),
            Token::TypeFloat => write!(f, "float"),
            Token::TypeBool => write!(f, "bool"),
            Token::Asc => write!(f, "asc"),
            Token::Desc => write!(f, "desc"),
            Token::Arrow => write!(f, "->"),
            Token::Spread => write!(f, "..."),
            Token::Eq => write!(f, "=="),
            Token::NotEq => write!(f, "!="),
            Token::LtEq => write!(f, "<="),
            Token::GtEq => write!(f, ">="),
            Token::And => write!(f, "&&"),
            Token::Or => write!(f, "||"),
            Token::Lt => write!(f, "<"),
            Token::Gt => write!(f, ">"),
            Token::Not => write!(f, "!"),
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Star => write!(f, "*"),
            Token::Slash => write!(f, "/"),
            Token::Percent => write!(f, "%"),
            Token::Assign => write!(f, "="),
            Token::LBrace => write!(f, "{{"),
            Token::RBrace => write!(f, "}}"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::Colon => write!(f, ":"),
            Token::Comma => write!(f, ","),
            Token::Dot => write!(f, "."),
            Token::Hash => write!(f, "#"),
            Token::Dollar => write!(f, "$"),
        }
    }
}

/// Lexer options
#[derive(Clone, Copy)]
pub struct LexOptions {
    /// Whether `/pattern/` lexes as a regex literal. Disabled inside string
    /// interpolations, where `/` is always division.
    pub regex_literals: bool,
}

impl Default for LexOptions {
    fn default() -> Self {
        Self { regex_literals: true }
    }
}

/// Tokenize a source string. Returns the tokens plus any lexical errors;
/// lexing continues past errors so the parser can still run.
pub fn lex(source: &str) -> (Vec<Spanned<Token<'_>>>, Vec<ParseError>) {
    lex_with_options(source, LexOptions::default())
}

/// Tokenize with explicit options (see [`LexOptions`])
pub fn lex_with_options(source: &str, opts: LexOptions) -> (Vec<Spanned<Token<'_>>>, Vec<ParseError>) {
    let mut lexer = Lexer { source, pos: 0, opts, tokens: Vec::new(), errors: Vec::new() };
    lexer.run();
    (lexer.tokens, lexer.errors)
}

struct Lexer<'src> {
    source: &'src str,
    pos: usize,
    opts: LexOptions,
    tokens: Vec<Spanned<Token<'src>>>,
    errors: Vec<ParseError>,
}

impl<'src> Lexer<'src> {
    fn rest(&self) -> &'src str {
        &self.source[self.pos..]
    }

    fn peek_char(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn error(&mut self, span: Span, message: String) {
        self.errors.push(ParseError { span: span.into_range(), message });
    }

    fn push(&mut self, token: Token<'src>, start: usize) {
        self.tokens.push((token, Span::new(start, self.pos)));
    }

    /// Skip whitespace and // comments
    fn skip_trivia(&mut self) {
        loop {
            let mut progressed = false;
            while let Some(c) = self.peek_char() {
                if c.is_whitespace() {
                    self.pos += c.len_utf8();
                    progressed = true;
                } else {
                    break;
                }
            }
            if self.rest().starts_with("//") {
                match self.rest().find('\n') {
                    Some(n) => self.pos += n + 1,
                    None => self.pos = self.source.len(),
                }
                progressed = true;
            }
            if !progressed {
                break;
            }
        }
    }

    fn run(&mut self) {
        loop {
            self.skip_trivia();
            let Some(c) = self.peek_char() else { break };
            let start = self.pos;

            match c {
                '"' if self.rest().starts_with("\"\"\"") => self.multiline_string(start),
                '"' => self.string(start),
                '0'..='9' => self.number(start),
                '`' => self.backtick_ident(start),
                '/' => self.slash_or_regex(start),
                c if c.is_ascii_alphabetic() || c == '_' => self.ident_or_keyword(start),
                _ => self.operator_or_punct(start, c),
            }
        }
    }

    /// """raw content""" — no escape processing
    fn multiline_string(&mut self, start: usize) {
        let body_start = start + 3;
        match self.source[body_start..].find("\"\"\"") {
            Some(n) => {
                self.pos = body_start + n + 3;
                self.push(Token::MultilineString(&self.source[body_start..body_start + n]), start);
            }
            None => {
                self.pos = self.source.len();
                self.error(
                    Span::new(start, self.pos),
                    "unterminated multiline string: missing closing \"\"\"".to_string(),
                );
            }
        }
    }

    /// "content" — the token keeps the RAW inner slice; escape sequences are
    /// decoded later by parse_interpolated_string. Any `\<char>` is accepted
    /// here so unknown escapes (e.g. `\%` in SQL LIKE patterns) don't fail
    /// the whole string.
    fn string(&mut self, start: usize) {
        self.pos = start + 1;
        loop {
            match self.peek_char() {
                Some('"') => {
                    let content = &self.source[start + 1..self.pos];
                    self.pos += 1;
                    self.push(Token::String(content), start);
                    return;
                }
                Some('\\') => {
                    self.pos += 1;
                    if let Some(escaped) = self.peek_char() {
                        self.pos += escaped.len_utf8();
                    }
                }
                Some(c) => self.pos += c.len_utf8(),
                None => {
                    self.error(
                        Span::new(start, self.pos),
                        "unterminated string: missing closing '\"'".to_string(),
                    );
                    return;
                }
            }
        }
    }

    /// Integer or float. No leading sign: `-` is always the Minus token and
    /// negation is handled by the parsers (folding the sign into the literal
    /// broke binary minus without spaces and made `-0.5` unlexable).
    fn number(&mut self, start: usize) {
        while matches!(self.peek_char(), Some('0'..='9')) {
            self.pos += 1;
        }
        // Float: digits '.' digits (the dot must be followed by a digit,
        // otherwise it's an Int followed by a Dot token)
        let bytes = self.source.as_bytes();
        if self.peek_char() == Some('.')
            && bytes.get(self.pos + 1).is_some_and(|b| b.is_ascii_digit())
        {
            self.pos += 1;
            while matches!(self.peek_char(), Some('0'..='9')) {
                self.pos += 1;
            }
            let text = &self.source[start..self.pos];
            // digits '.' digits always parses as f64
            self.push(Token::Float(text.parse().unwrap()), start);
        } else {
            let text = &self.source[start..self.pos];
            let value = text.parse::<i64>().unwrap_or_else(|_| {
                self.errors.push(ParseError {
                    span: start..self.pos,
                    message: format!(
                        "integer literal '{}' is out of range for a 64-bit integer",
                        text
                    ),
                });
                i64::MAX
            });
            self.push(Token::Int(value), start);
        }
    }

    /// `name` — backtick-quoted identifier (e.g. `quote_2023-08-17`)
    fn backtick_ident(&mut self, start: usize) {
        match self.source[start + 1..].find('`') {
            Some(0) => {
                self.pos = start + 2;
                self.error(Span::new(start, self.pos), "empty backtick identifier".to_string());
            }
            Some(n) => {
                self.pos = start + 1 + n + 1;
                self.push(Token::Ident(&self.source[start + 1..start + 1 + n]), start);
            }
            None => {
                self.pos = self.source.len();
                self.error(
                    Span::new(start, self.pos),
                    "unterminated backtick identifier: missing closing '`'".to_string(),
                );
            }
        }
    }

    /// `/pattern/` regex literal, or the division operator.
    ///
    /// The regex body may not contain whitespace (use \s or [ ] for a literal
    /// space). This disambiguates regexes from division: `$a / 2` fails the
    /// regex rule at the space and falls back to the Slash operator, instead
    /// of swallowing everything up to the next '/' in the file.
    fn slash_or_regex(&mut self, start: usize) {
        if self.opts.regex_literals {
            let mut j = start + 1;
            let mut found_close = None;
            for (off, c) in self.source[start + 1..].char_indices() {
                let at = start + 1 + off;
                if c == '/' {
                    if at > start + 1 {
                        found_close = Some(at);
                    }
                    break;
                }
                if c.is_whitespace() {
                    break;
                }
                j = at + c.len_utf8();
            }
            let _ = j;
            if let Some(close) = found_close {
                self.pos = close + 1;
                self.push(Token::Regex(&self.source[start + 1..close]), start);
                return;
            }
        }
        self.pos = start + 1;
        self.push(Token::Slash, start);
    }

    fn ident_or_keyword(&mut self, start: usize) {
        while matches!(self.peek_char(), Some(c) if c.is_ascii_alphanumeric() || c == '_') {
            self.pos += 1;
        }
        let text = &self.source[start..self.pos];
        let token = match text {
            "import" => Token::Import,
            "var" => Token::Var,
            "faker" => Token::Faker,
            "relation" => Token::Relation,
            "anonymize" => Token::Anonymize,
            "exclude_data" => Token::ExcludeData,
            "ignore_table" => Token::IgnoreTable,
            "ignore_relation" => Token::IgnoreRelation,
            "aggregate" => Token::Aggregate,
            "get" => Token::Get,
            "preserve" => Token::Preserve,
            "set" => Token::Set,
            "after" => Token::After,
            "root" => Token::Root,
            "where" => Token::Where,
            "order" => Token::Order,
            "by" => Token::By,
            "limit" => Token::Limit,
            "full" => Token::Full,
            "exclude" => Token::Exclude,
            "root_only" => Token::RootOnly,
            "match" => Token::Match,
            "when" => Token::When,
            "null" => Token::Null,
            "string" => Token::TypeString,
            "int" => Token::TypeInt,
            "float" => Token::TypeFloat,
            "bool" => Token::TypeBool,
            "asc" => Token::Asc,
            "desc" => Token::Desc,
            "true" => Token::Bool(true),
            "false" => Token::Bool(false),
            _ => Token::Ident(text),
        };
        self.push(token, start);
    }

    fn operator_or_punct(&mut self, start: usize, c: char) {
        // Multi-character operators first
        let rest = self.rest();
        let two: &[(&str, Token)] = &[
            ("->", Token::Arrow),
            ("...", Token::Spread),
            ("==", Token::Eq),
            ("!=", Token::NotEq),
            ("<=", Token::LtEq),
            (">=", Token::GtEq),
            ("&&", Token::And),
            ("||", Token::Or),
        ];
        for (pat, tok) in two {
            if rest.starts_with(pat) {
                self.pos = start + pat.len();
                self.push(tok.clone(), start);
                return;
            }
        }

        let token = match c {
            '<' => Some(Token::Lt),
            '>' => Some(Token::Gt),
            '!' => Some(Token::Not),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '*' => Some(Token::Star),
            '%' => Some(Token::Percent),
            '=' => Some(Token::Assign),
            '{' => Some(Token::LBrace),
            '}' => Some(Token::RBrace),
            '[' => Some(Token::LBracket),
            ']' => Some(Token::RBracket),
            '(' => Some(Token::LParen),
            ')' => Some(Token::RParen),
            ':' => Some(Token::Colon),
            ',' => Some(Token::Comma),
            '.' => Some(Token::Dot),
            '#' => Some(Token::Hash),
            '$' => Some(Token::Dollar),
            _ => None,
        };
        self.pos = start + c.len_utf8();
        match token {
            Some(tok) => self.push(tok, start),
            None => {
                // Unknown character: report and skip it, then keep lexing
                self.error(
                    Span::new(start, self.pos),
                    format!("unexpected character '{}'", c),
                );
            }
        }
    }
}

/// Map a token back to its identifier text, treating keywords as plain
/// identifiers. Used inside interpolations so `{$limit}` works even though
/// `limit` is a keyword in the statement grammar.
pub(crate) fn token_as_ident<'src>(token: &Token<'src>) -> Option<&'src str> {
    match token {
        Token::Ident(s) => Some(s),
        Token::Import => Some("import"),
        Token::Var => Some("var"),
        Token::Faker => Some("faker"),
        Token::Relation => Some("relation"),
        Token::Anonymize => Some("anonymize"),
        Token::ExcludeData => Some("exclude_data"),
        Token::IgnoreTable => Some("ignore_table"),
        Token::IgnoreRelation => Some("ignore_relation"),
        Token::Aggregate => Some("aggregate"),
        Token::Get => Some("get"),
        Token::Preserve => Some("preserve"),
        Token::Set => Some("set"),
        Token::After => Some("after"),
        Token::Root => Some("root"),
        Token::Where => Some("where"),
        Token::Order => Some("order"),
        Token::By => Some("by"),
        Token::Limit => Some("limit"),
        Token::Full => Some("full"),
        Token::Exclude => Some("exclude"),
        Token::RootOnly => Some("root_only"),
        Token::Match => Some("match"),
        Token::When => Some("when"),
        Token::Null => Some("null"),
        Token::TypeString => Some("string"),
        Token::TypeInt => Some("int"),
        Token::TypeFloat => Some("float"),
        Token::TypeBool => Some("bool"),
        Token::Asc => Some("asc"),
        Token::Desc => Some("desc"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex_tokens(input: &str) -> Vec<Token<'_>> {
        let (tokens, errors) = lex(input);
        if !errors.is_empty() {
            panic!("Lexer errors: {:?}", errors);
        }
        tokens.into_iter().map(|(t, _)| t).collect()
    }

    #[test]
    fn test_keywords() {
        let tokens = lex_tokens("import var faker relation anonymize");
        assert_eq!(
            tokens,
            vec![
                Token::Import,
                Token::Var,
                Token::Faker,
                Token::Relation,
                Token::Anonymize,
            ]
        );
    }

    #[test]
    fn test_types() {
        let tokens = lex_tokens("string int float bool");
        assert_eq!(
            tokens,
            vec![
                Token::TypeString,
                Token::TypeInt,
                Token::TypeFloat,
                Token::TypeBool,
            ]
        );
    }

    #[test]
    fn test_numbers() {
        let tokens = lex_tokens("42 3.14 -100");
        assert_eq!(
            tokens,
            vec![
                Token::Int(42),
                Token::Float(3.14),
                Token::Minus,
                Token::Int(100),
            ]
        );
    }

    #[test]
    fn test_int_overflow_is_error_not_panic() {
        let (tokens, errors) = lex("limit 99999999999999999999");
        assert!(!errors.is_empty(), "20-digit literal must be a lex error");
        assert!(errors[0].to_string().contains("out of range"));
        // Lexing continues: the token stream is still produced
        assert_eq!(tokens.len(), 2);
    }

    #[test]
    fn test_unknown_escape_lexes_without_cascade() {
        // \% is not a defined escape but must not fail the string
        let tokens = lex_tokens(r#""path LIKE 'a\%'""#);
        assert_eq!(tokens, vec![Token::String(r"path LIKE 'a\%'")]);
    }

    #[test]
    fn test_division_with_spaces_is_slash_not_regex() {
        let tokens = lex_tokens("$a / 2 > 10");
        assert_eq!(
            tokens,
            vec![
                Token::Dollar,
                Token::Ident("a"),
                Token::Slash,
                Token::Int(2),
                Token::Gt,
                Token::Int(10),
            ]
        );
    }

    #[test]
    fn test_regex_still_lexes() {
        let tokens = lex_tokens("ignore_table /^cache_/");
        assert_eq!(tokens, vec![Token::IgnoreTable, Token::Regex("^cache_")]);
    }

    #[test]
    fn test_strings() {
        let tokens = lex_tokens(r#""hello" "world""#);
        assert_eq!(tokens, vec![Token::String("hello"), Token::String("world"),]);
    }

    #[test]
    fn test_operators() {
        let tokens = lex_tokens("-> == != <= >= && || < > + - * / %");
        assert_eq!(
            tokens,
            vec![
                Token::Arrow,
                Token::Eq,
                Token::NotEq,
                Token::LtEq,
                Token::GtEq,
                Token::And,
                Token::Or,
                Token::Lt,
                Token::Gt,
                Token::Plus,
                Token::Minus,
                Token::Star,
                Token::Slash,
                Token::Percent,
            ]
        );
    }

    #[test]
    fn test_attribute_start() {
        let tokens = lex_tokens("#[when($x)]");
        assert_eq!(
            tokens,
            vec![
                Token::Hash,
                Token::LBracket,
                Token::When,
                Token::LParen,
                Token::Dollar,
                Token::Ident("x"),
                Token::RParen,
                Token::RBracket,
            ]
        );
    }

    #[test]
    fn test_comment() {
        let tokens = lex_tokens("import // this is a comment\nvar");
        assert_eq!(tokens, vec![Token::Import, Token::Var]);
    }

    #[test]
    fn test_multiline_string() {
        let tokens = lex_tokens(r#""""SELECT * FROM table""""#);
        assert_eq!(tokens, vec![Token::MultilineString("SELECT * FROM table")]);
    }

    #[test]
    fn test_spread_operator() {
        let tokens = lex_tokens("...$emails");
        assert_eq!(
            tokens,
            vec![
                Token::Spread,
                Token::Dollar,
                Token::Ident("emails"),
            ]
        );
    }

    #[test]
    fn test_spread_vs_dot() {
        // Ensure ... is tokenized as Spread, not three Dots
        let tokens = lex_tokens("a.b ...$c");
        assert_eq!(
            tokens,
            vec![
                Token::Ident("a"),
                Token::Dot,
                Token::Ident("b"),
                Token::Spread,
                Token::Dollar,
                Token::Ident("c"),
            ]
        );
    }

    #[test]
    fn test_string_array_type() {
        let tokens = lex_tokens("var x: string[]");
        assert_eq!(
            tokens,
            vec![
                Token::Var,
                Token::Ident("x"),
                Token::Colon,
                Token::TypeString,
                Token::LBracket,
                Token::RBracket,
            ]
        );
    }

    #[test]
    fn test_backtick_quoted_ident() {
        let tokens = lex_tokens("`quote_2023-08-17`");
        assert_eq!(tokens, vec![Token::Ident("quote_2023-08-17")]);
    }

    #[test]
    fn test_backtick_quoted_in_context() {
        let tokens = lex_tokens("ignore_table `quote_2023-08-17`");
        assert_eq!(
            tokens,
            vec![Token::IgnoreTable, Token::Ident("quote_2023-08-17")]
        );
    }

    #[test]
    fn test_all_array_types() {
        // Test that all array type syntaxes lex correctly
        let tokens = lex_tokens("int[] float[] bool[]");
        assert_eq!(
            tokens,
            vec![
                Token::TypeInt,
                Token::LBracket,
                Token::RBracket,
                Token::TypeFloat,
                Token::LBracket,
                Token::RBracket,
                Token::TypeBool,
                Token::LBracket,
                Token::RBracket,
            ]
        );
    }

    #[test]
    fn test_unterminated_string_is_error() {
        let (_, errors) = lex(r#"var x: string = "oops"#);
        assert!(!errors.is_empty());
        assert!(errors[0].message.contains("unterminated string"));
    }

    #[test]
    fn test_unknown_char_recovers() {
        let (tokens, errors) = lex("var @ x");
        assert!(!errors.is_empty());
        assert!(errors[0].message.contains("unexpected character"));
        // Lexing continued past the bad character
        assert_eq!(
            tokens.into_iter().map(|(t, _)| t).collect::<Vec<_>>(),
            vec![Token::Var, Token::Ident("x")]
        );
    }

    #[test]
    fn test_string_spans_include_quotes() {
        let (tokens, _) = lex(r#"  "abc"  "#);
        assert_eq!(tokens.len(), 1);
        let (_, span) = &tokens[0];
        assert_eq!((span.start, span.end), (2, 7));
    }
}
