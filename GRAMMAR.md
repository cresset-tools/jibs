# Jibs DSL - Formal Grammar

This document defines the formal grammar of the Jibs DSL using Extended
Backus-Naur Form (EBNF). The reference implementation is the hand-written
recursive descent parser in `crates/parser/src/parser.rs` (statements) and
`crates/parser/src/lexer.rs` (tokens).

Code examples marked as `jibs` are parsed by the test suite
(`crates/parser/tests/corpus.rs`), so they cannot drift from the
implementation.

## Notation

| Symbol | Meaning |
|--------|---------|
| `=` | Definition |
| `\|` | Alternation (or) |
| `[ ]` | Optional (0 or 1) |
| `{ }` | Repetition (0 or more) |
| `( )` | Grouping |
| `" "` | Terminal string (literal) |
| `' '` | Terminal string (literal, alternative) |
| `(* *)` | Comment |

## Grammar

### Top-Level Structure

```ebnf
program = { attributed_statement } ;

attributed_statement = [ attribute ] statement ;

attribute = "#" "[" "when" "(" expression ")" "]" ;

statement = import_stmt
          | var_decl
          | faker_decl
          | relation_decl
          | ignore_relation_decl
          | anonymize_block
          | exclude_data_stmt
          | ignore_table_stmt
          | full_stmt
          | aggregate_block
          | get_def
          | preserve_stmt
          | set_block
          | after_block
          ;
```

The `#[when(...)]` attribute may prefix **any** statement kind.

### Import Statement

```ebnf
import_stmt = "import" raw_string ;
```

The path is a raw string: no interpolation or escape decoding is applied.

```jibs
import "magento-base.jibs"
import "common/anonymization.jibs"
```

### Variable Declaration

```ebnf
var_decl = "var" identifier ":" type [ "=" literal ] ;

type = scalar_type [ "[" "]" ] ;

scalar_type = "string" | "int" | "float" | "bool" ;

literal = string_literal
        | signed_integer
        | signed_float
        | bool_literal
        | "null"
        | array_literal
        ;

signed_integer = [ "-" ] integer_literal ;

signed_float = [ "-" ] float_literal ;

array_literal = "[" [ array_elements ] "]" ;

array_elements = array_element { "," array_element } [ "," ] ;

array_element = string_literal | signed_integer | signed_float | bool_literal ;

bool_literal = "true" | "false" ;
```

Array elements must all have the same type, determined by the first element.
An empty array literal `[]` is a string array.

```jibs
var base_domain: string
var admin_email: string = "admin@local.test"
var order_limit: int = 100
var tax_rate: float = 0.21
var offset: int = -3
var factor: float = -0.5
var skip_payments: bool = true

// Array types
var emails: string[] = ["user1@test.com", "user2@test.com"]
var ids: int[] = [1, -2, 3, 4, 5]
var prices: float[] = [9.99, 19.99, 29.99]
var flags: bool[] = [true, false, true]
```

### Faker Declaration

```ebnf
faker_decl = "faker" identifier faker_source ;

faker_source = faker_array | variable_ref ;

faker_array = "[" [ faker_values ] "]" ;

faker_values = faker_value { "," faker_value } [ "," ] ;

faker_value = string_literal | spread_expr ;

spread_expr = "..." variable_ref ;
```

```jibs
// Inline array of strings
faker names ["John", "Jane", "Bob", "Alice"]

// Using a string[] variable directly
var base_emails: string[] = ["admin@test.com", "user@test.com"]
faker admin_emails $base_emails

// Spread operator to combine values
faker all_emails [...$base_emails, "extra@test.com"]
```

### Relation Declarations

```ebnf
relation_decl = "relation" column_ref "->" column_ref ;

ignore_relation_decl = "ignore_relation" column_ref "->" column_ref ;

column_ref = identifier "." identifier ;
```

```jibs
relation customer_entity.group_id -> customer_group.customer_group_id
ignore_relation sales_order.store_id -> store.store_id
```

### Anonymize Block

```ebnf
anonymize_block = "anonymize" identifier "{" { anonymize_rule } "}" ;

anonymize_rule = identifier "->" ( identifier | "null" ) ;
```

```jibs
anonymize customer_entity {
    email     -> emails
    firstname -> names
    password  -> null
}
```

### Table Handling Statements

```ebnf
exclude_data_stmt = "exclude_data" table_pattern ;

ignore_table_stmt = "ignore_table" table_pattern ;

full_stmt = "full" table_pattern { "," table_pattern } ;

table_pattern = identifier | regex_literal ;
```

```jibs
exclude_data sales_order_payment
exclude_data /_log$/

ignore_table report_event
ignore_table /^cache/

full customer_group, store, /^catalog_category/
```

### Aggregate Block

```ebnf
aggregate_block = "aggregate" identifier "{" "root" identifier query_clauses "}" ;

query_clauses = [ where_clause ]
                [ order_by_clause ]
                [ limit_clause ]
                [ exclude_clause ]
                [ "root_only" ]
              ;

where_clause = "where" string_literal ;

order_by_clause = "order" "by" identifier [ "asc" | "desc" ] ;

limit_clause = "limit" ( integer_literal | variable_ref ) ;

exclude_clause = "exclude" table_pattern { "," table_pattern } ;
```

Clauses must appear in the order shown.

```jibs
aggregate orders {
    root sales_order
    where "created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)"
    order by created_at desc
    limit 100
}

aggregate products {
    root catalog_product_entity
    where "entity_id IN (SELECT product_id FROM catalog_category_product WHERE category_id = 42)"
    exclude /^url_rewrite/, catalog_product_flat_1
    root_only
}
```

### Get Function Definition

```ebnf
get_def = "get" identifier "(" [ params ] ")"
          "{" identifier query_clauses "}" ;

params = param { "," param } [ "," ] ;

param = identifier ":" type [ "=" literal ] ;
```

The identifier at the start of the body names the aggregate to fetch; the
query clauses use the same grammar (and ordering) as aggregate blocks.

```jibs
aggregate products {
    root catalog_product_entity
    where "FALSE"
}

get product_by_sku (sku: string) {
    products where "sku = '{$sku}'"
}

get recent_products (days: int = 7, max: int = 100) {
    products
    where "updated_at > DATE_SUB(NOW(), INTERVAL {$days} DAY)"
    limit $max
}
```

### Preserve Statement

```ebnf
preserve_stmt = "preserve" identifier "where" string_literal ;
```

```jibs
preserve core_config_data where "path LIKE 'dev/%'"
```

### Set Block

```ebnf
set_block = "set" identifier "{" match_clause { assignment } "}" ;

match_clause = "match" assignment { "," assignment } ;

assignment = identifier "=" value ;

value = string_literal
      | signed_integer
      | signed_float
      | bool_literal
      | variable_ref
      ;
```

Note that the assignments after the match clause are *not* comma-separated;
a comma continues the match clause instead.

```jibs
var base_domain: string = "local.test"

set core_config_data {
    match path = "web/secure/base_url", scope = "default", scope_id = 0
    value = "https://{$base_domain}/"
}
```

### After Block

```ebnf
after_block = "after" "{" { sql_string } "}" ;

sql_string = multiline_string | raw_string ;
```

SQL strings are raw: no interpolation or escape decoding is applied to
either form.

```jibs
after {
    """
    UPDATE sales_order
    SET created_at = DATE_ADD(created_at, INTERVAL 10 YEAR)
    """

    "TRUNCATE TABLE sessions"
}
```

### Expressions

Expressions are used in conditional attributes and string interpolation.
Both contexts share one grammar and implementation, with two differences
inside interpolations: statement keywords act as plain identifiers after `$`
(so `{$limit}` works), and `unique()` is available.

```ebnf
expression = or_expr ;

or_expr = and_expr { "||" and_expr } ;

and_expr = equality_expr { "&&" equality_expr } ;

equality_expr = comparison_expr { ( "==" | "!=" ) comparison_expr } ;

comparison_expr = additive_expr { ( "<" | ">" | "<=" | ">=" ) additive_expr } ;

additive_expr = multiplicative_expr { ( "+" | "-" ) multiplicative_expr } ;

multiplicative_expr = unary_expr { ( "*" | "/" | "%" ) unary_expr } ;

unary_expr = ( "!" | "-" ) unary_expr | primary_expr ;

primary_expr = integer_literal
             | float_literal
             | bool_literal
             | string_literal
             | variable_ref
             | "(" expression ")"
             | unique_call        (* interpolation contexts only *)
             ;

unique_call = "unique" "(" ")" ;

variable_ref = "$" identifier ;
```

**Operator precedence (highest to lowest):**

| Precedence | Operators | Associativity |
|------------|-----------|---------------|
| 1 | `!` `-` (unary) | Right |
| 2 | `*` `/` `%` | Left |
| 3 | `+` `-` | Left |
| 4 | `<` `>` `<=` `>=` | Left |
| 5 | `==` `!=` | Left |
| 6 | `&&` | Left |
| 7 | `\|\|` | Left |

```jibs
var skip_payments: bool = false
var order_limit: int = 100
var env: string = "staging"
var debug_mode: bool = true

#[when($skip_payments)]
exclude_data sales_order_payment

#[when($order_limit / 2 > 25)]
ignore_table order_stats

#[when(($env == "staging" || $env == "development") && $debug_mode)]
exclude_data customer_log
```

### String Interpolation

Inside regular (double-quoted) strings, `{...}` delimits an interpolated
expression. Escape sequences are decoded in the same pass.

```ebnf
string_content = { string_char | interpolation } ;

interpolation = "{" expression "}" ;

string_char = escape_sequence | (* any character except '"' or '\' *) ;

escape_sequence = "\\" | '\"' | "\n" | "\t" | "\{"
                | "\" (* any other character: both characters kept as-is *)
                ;
```

A `{` that does not begin a well-formed interpolation expression is a parse
error; write `\{` for a literal brace. Unknown escapes such as `\%` keep the
backslash, so SQL escape sequences pass through to MySQL unchanged.

```jibs
var base_domain: string = "example.com"
var base_port: int = 8000
var instance: int = 1

set core_config_data {
    match path = "web/unsecure/base_url", scope = "default", scope_id = 0
    value = "http://{$base_domain}:{$base_port + $instance}/"
}

set docs {
    match id = 1
    value = "Use \{$var} syntax for interpolation"
}
```

### Lexical Elements

```ebnf
identifier = ident_start { ident_start | digit }
           | backtick_identifier
           ;

ident_start = letter | "_" ;

backtick_identifier = "`" (* one or more characters except '`' *) "`" ;

integer_literal = digit { digit } ;           (* unsigned; '-' is an operator *)

float_literal = digit { digit } "." digit { digit } ;

string_literal = '"' string_content '"' ;     (* interpolated + escapes decoded *)

raw_string = '"' (* same syntax, content kept raw *) '"' ;

multiline_string = '"""' (* any characters except '"""' *) '"""' ;

regex_literal = "/" (* one or more characters, no whitespace, no '/' *) "/" ;

comment = "//" (* to end of line *) ;
```

Notes:

- Integer and float literals are unsigned at the lexer level; negative
  numbers are expressed with the `-` operator, which literal positions
  (`var` defaults, array elements, `set` values) and expressions both
  accept. Integer literals outside the 64-bit signed range are an error.
- Regex literal bodies may not contain whitespace (use `\s` or `[ ]` for a
  literal space). This disambiguates them from the division operator:
  `$a / 2` is division, `/^cache/` is a regex. Inside interpolations regex
  literals do not exist at all — `/` is always division there.
- Backtick identifiers allow table names that clash with keywords or contain
  special characters: `` ignore_table `quote_2023-08-17` ``.
- Multiline strings have no escape processing at all.
- Strings may span multiple lines.

### Reserved Keywords

The following words are keywords and cannot be used as bare identifiers
(use backticks for table names, or `$name` inside interpolations where
keywords are permitted as variable names):

```text
after        aggregate    anonymize    asc          bool
by           desc         exclude      exclude_data faker
false        float        full         get          ignore_relation
ignore_table import       int          limit        match
null         order        preserve     relation     root
root_only    set          string       true         var
when         where
```

## Semantic Notes

### Identifier Scope

- **Variables** (`var`): global scope; later declarations with the same name
  override earlier ones (import order matters, see SPEC.md).
- **Faker names**: global scope; referenced by `anonymize` rules.
- **Aggregate names**: global scope; referenced by `get` function bodies.
- **Table names**: not validated at parse time; validated at runtime against
  the remote database schema.

### String Literal Contexts

| Context | Interpolation | Escapes decoded | Multiline |
|---------|---------------|-----------------|-----------|
| `import` path | No | No | No |
| `faker` values | Yes | Yes | No |
| `where` clauses | Yes | Yes | No |
| `set` / `match` values | Yes | Yes | No |
| Variable defaults | Yes | Yes | No |
| Strings in `#[when(...)]` | Yes | Yes | No |
| `after` SQL | No | No | Yes (`"""`) |

### Error Recovery

The parser reports **all** errors in a file, not just the first: after a
statement fails to parse it re-synchronizes at the next statement keyword.
Malformed interpolations inside strings are reported with the enclosing
statement otherwise intact.

### Whitespace and Comments

- Whitespace is ignored except as a token separator.
- Comments start with `//` and extend to the end of the line; they can
  appear anywhere between tokens.
- `//` inside strings is not a comment.
