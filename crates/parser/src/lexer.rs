//! Lexer for the MySQL Import DSL

use chumsky::prelude::*;
use std::fmt;

use crate::Span;
use crate::ast::Spanned;

/// Token types for the MySQL Import DSL
#[derive(Clone, Debug, PartialEq)]
pub enum Token<'src> {
    // Literals
    Int(i64),
    Float(f64),
    Bool(bool),
    String(&'src str),
    MultilineString(&'src str),

    // Identifiers and keywords
    Ident(&'src str),

    // Keywords
    Import,
    Var,
    Faker,
    Relation,
    Anonymize,
    Exclude,
    Ignore,
    Aggregate,
    Include,
    Preserve,
    Set,
    After,
    Root,
    Where,
    Order,
    By,
    Limit,
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
            Token::Ident(s) => write!(f, "{s}"),
            Token::Import => write!(f, "import"),
            Token::Var => write!(f, "var"),
            Token::Faker => write!(f, "faker"),
            Token::Relation => write!(f, "relation"),
            Token::Anonymize => write!(f, "anonymize"),
            Token::Exclude => write!(f, "exclude"),
            Token::Ignore => write!(f, "ignore"),
            Token::Aggregate => write!(f, "aggregate"),
            Token::Include => write!(f, "include"),
            Token::Preserve => write!(f, "preserve"),
            Token::Set => write!(f, "set"),
            Token::After => write!(f, "after"),
            Token::Root => write!(f, "root"),
            Token::Where => write!(f, "where"),
            Token::Order => write!(f, "order"),
            Token::By => write!(f, "by"),
            Token::Limit => write!(f, "limit"),
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

/// Create the lexer for the DSL
pub fn lexer<'src>(
) -> impl Parser<'src, &'src str, Vec<Spanned<Token<'src>>>, extra::Err<Rich<'src, char, Span>>> {
    // Multiline string (must come before regular string)
    let multiline_string = just("\"\"\"")
        .ignore_then(any().and_is(just("\"\"\"").not()).repeated().to_slice())
        .then_ignore(just("\"\"\""))
        .map(Token::MultilineString);

    // Regular string with escape sequences
    let escape = just('\\').ignore_then(choice((
        just('\\'),
        just('"'),
        just('n').to('\n'),
        just('t').to('\t'),
        just('{'),
    )));

    let string_char = none_of("\\\"").or(escape);

    let string = just('"')
        .ignore_then(string_char.repeated().to_slice())
        .then_ignore(just('"'))
        .map(Token::String);

    // Numbers (float must come before int to handle decimal points)
    let float = text::int(10)
        .then(just('.').then(text::digits(10)))
        .to_slice()
        .from_str()
        .unwrapped()
        .map(Token::Float);

    let int = just('-')
        .or_not()
        .then(text::int(10))
        .to_slice()
        .from_str()
        .unwrapped()
        .map(Token::Int);

    // Multi-character operators (must come before single-char)
    let arrow = just("->").to(Token::Arrow);
    let eq = just("==").to(Token::Eq);
    let not_eq = just("!=").to(Token::NotEq);
    let lt_eq = just("<=").to(Token::LtEq);
    let gt_eq = just(">=").to(Token::GtEq);
    let and = just("&&").to(Token::And);
    let or = just("||").to(Token::Or);

    // Single-character operators
    let lt = just('<').to(Token::Lt);
    let gt = just('>').to(Token::Gt);
    let not = just('!').to(Token::Not);
    let plus = just('+').to(Token::Plus);
    let minus = just('-').to(Token::Minus);
    let star = just('*').to(Token::Star);
    let slash = just('/').to(Token::Slash);
    let percent = just('%').to(Token::Percent);
    let assign = just('=').to(Token::Assign);

    // Delimiters
    let lbrace = just('{').to(Token::LBrace);
    let rbrace = just('}').to(Token::RBrace);
    let lbracket = just('[').to(Token::LBracket);
    let rbracket = just(']').to(Token::RBracket);
    let lparen = just('(').to(Token::LParen);
    let rparen = just(')').to(Token::RParen);

    // Punctuation
    let colon = just(':').to(Token::Colon);
    let comma = just(',').to(Token::Comma);
    let dot = just('.').to(Token::Dot);
    let hash = just('#').to(Token::Hash);
    let dollar = just('$').to(Token::Dollar);

    // Identifiers and keywords
    let ident = text::ascii::ident().map(|ident: &str| match ident {
        "import" => Token::Import,
        "var" => Token::Var,
        "faker" => Token::Faker,
        "relation" => Token::Relation,
        "anonymize" => Token::Anonymize,
        "exclude" => Token::Exclude,
        "ignore" => Token::Ignore,
        "aggregate" => Token::Aggregate,
        "include" => Token::Include,
        "preserve" => Token::Preserve,
        "set" => Token::Set,
        "after" => Token::After,
        "root" => Token::Root,
        "where" => Token::Where,
        "order" => Token::Order,
        "by" => Token::By,
        "limit" => Token::Limit,
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
        _ => Token::Ident(ident),
    });

    // All operators (order matters - longer matches first)
    let operator = choice((
        arrow, eq, not_eq, lt_eq, gt_eq, and, or,
        lt, gt, not, plus, minus, star, slash, percent, assign,
    ));

    // All delimiters and punctuation
    let delimiter = choice((
        lbrace, rbrace, lbracket, rbracket, lparen, rparen,
        colon, comma, dot, hash, dollar,
    ));

    // A single token can be one of the above
    let token = choice((
        multiline_string,
        string,
        float,
        int,
        operator,
        delimiter,
        ident,
    ));

    // Comments start with # and go to end of line
    // But we need to be careful: # followed by [ is an attribute, not a comment
    let comment = just('#')
        .then(none_of('[').rewind())
        .then(any().and_is(text::newline().not()).repeated())
        .padded();

    token
        .map_with(|tok, e| (tok, e.span()))
        .padded_by(comment.repeated())
        .padded()
        .recover_with(skip_then_retry_until(any().ignored(), end()))
        .repeated()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(input: &str) -> Vec<Token<'_>> {
        let (tokens, errors) = lexer().parse(input).into_output_errors();
        if !errors.is_empty() {
            panic!("Lexer errors: {:?}", errors);
        }
        tokens.unwrap().into_iter().map(|(t, _)| t).collect()
    }

    #[test]
    fn test_keywords() {
        let tokens = lex("import var faker relation anonymize");
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
        let tokens = lex("string int float bool");
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
        let tokens = lex("42 3.14 -100");
        assert_eq!(
            tokens,
            vec![Token::Int(42), Token::Float(3.14), Token::Int(-100),]
        );
    }

    #[test]
    fn test_strings() {
        let tokens = lex(r#""hello" "world""#);
        assert_eq!(tokens, vec![Token::String("hello"), Token::String("world"),]);
    }

    #[test]
    fn test_operators() {
        let tokens = lex("-> == != <= >= && || < > + - * / %");
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
        let tokens = lex("#[when($x)]");
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
        let tokens = lex("import # this is a comment\nvar");
        assert_eq!(tokens, vec![Token::Import, Token::Var]);
    }

    #[test]
    fn test_multiline_string() {
        let tokens = lex(r#""""SELECT * FROM table""""#);
        assert_eq!(tokens, vec![Token::MultilineString("SELECT * FROM table")]);
    }
}
