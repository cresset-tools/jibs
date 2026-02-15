# MySQL Import DSL - Formal Grammar

This document defines the formal grammar of the MySQL Import DSL using Extended Backus-Naur Form (EBNF).

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
| `/* */` | Comment |
| `?` | Optional (suffix notation) |
| `+` | One or more (suffix notation) |
| `*` | Zero or more (suffix notation) |

## Grammar

### Top-Level Structure

```ebnf
program = { statement } ;

statement = import_stmt
          | var_decl
          | faker_decl
          | relation_decl
          | anonymize_block
          | exclude_data_stmt
          | ignore_table_stmt
          | aggregate_block
          | include_stmt
          | preserve_stmt
          | set_block
          | after_block
          ;

/* Statements can be prefixed with a conditional attribute */
attributed_statement = [ attribute ] statement ;

attribute = "#[" "when" "(" expression ")" "]" ;
```

### Import Statement

```ebnf
import_stmt = "import" string_literal ;
```

**Examples:**
```
import "magento-base.dsl"
import "common/anonymization.dsl"
```

### Variable Declaration

```ebnf
var_decl = "var" identifier ":" type [ "=" literal ] ;

type = "string" | "int" | "float" | "bool" ;

literal = string_literal
        | integer_literal
        | float_literal
        | bool_literal
        ;

bool_literal = "true" | "false" ;
```

**Examples:**
```
var base_domain: string
var admin_email: string = "admin@local.test"
var order_limit: int = 100
var tax_rate: float = 0.21
var skip_payments: bool = true
```

### Faker Declaration

```ebnf
faker_decl = "faker" identifier "[" string_list "]" ;

string_list = string_literal { "," string_literal } ;
```

**Examples:**
```
faker names ["John", "Jane", "Bob", "Alice"]
faker emails ["user1@example.test", "user2@example.test"]
```

### Relation Declaration

```ebnf
relation_decl = "relation" column_ref "->" column_ref ;

column_ref = identifier "." identifier ;
```

**Examples:**
```
relation customer_entity.group_id -> customer_group.customer_group_id
relation sales_order.customer_id -> customer_entity.entity_id
```

### Anonymize Block

```ebnf
anonymize_block = "anonymize" identifier "{" { anonymize_rule } "}" ;

anonymize_rule = identifier "->" ( identifier | "null" ) ;
```

**Examples:**
```
anonymize customer_entity {
    email     -> emails
    firstname -> names
    lastname  -> names
    password  -> null
}
```

### Exclude Data Statement

```ebnf
exclude_data_stmt = "exclude_data" identifier ;
```

**Examples:**
```
exclude_data sales_order_payment
exclude_data customer_log
```

### Ignore Table Statement

```ebnf
ignore_table_stmt = "ignore_table" identifier ;
```

**Examples:**
```
ignore_table report_event
ignore_table sales_bestsellers_aggregated_daily
```

### Aggregate Block

```ebnf
aggregate_block = "aggregate" identifier "{" aggregate_body "}" ;

aggregate_body = root_clause
                 [ where_clause ]
                 [ order_by_clause ]
                 [ limit_clause ]
               ;

root_clause = "root" identifier ;

where_clause = "where" string_literal ;

order_by_clause = "order" "by" identifier [ sort_direction ] ;

sort_direction = "asc" | "desc" ;

limit_clause = "limit" ( integer_literal | variable_ref ) ;

variable_ref = "$" identifier ;
```

**Examples:**
```
aggregate orders {
    root sales_order
    where "created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)"
    order by created_at desc
    limit 100
}

aggregate products {
    root catalog_product_entity
    where "entity_id IN (SELECT product_id FROM catalog_category_product WHERE category_id = 42)"
}
```

### Include Statement

```ebnf
include_stmt = "include" identifier "where" string_literal ;
```

**Examples:**
```
include products where "sku = 'HERO-PRODUCT'"
include orders where "increment_id = '100000999'"
```

### Preserve Statement

```ebnf
preserve_stmt = "preserve" identifier "where" string_literal ;
```

**Examples:**
```
preserve core_config_data where "path LIKE 'dev/%'"
preserve admin_user where "username = 'localadmin'"
```

### Set Block

```ebnf
set_block = "set" identifier "{" match_clause { assignment } "}" ;

match_clause = "match" assignment_list ;

assignment_list = assignment { "," assignment } ;

assignment = identifier "=" value ;

value = string_literal
      | integer_literal
      | float_literal
      | bool_literal
      | variable_ref
      ;
```

**Examples:**
```
set core_config_data {
    match path = "web/secure/base_url", scope = "default", scope_id = 0
    value = "https://{$base_domain}/"
}

set core_config_data {
    match path = "dev/debug/enabled", scope = "default", scope_id = 0
    value = "1"
}
```

### After Block

```ebnf
after_block = "after" "{" { sql_string } "}" ;

sql_string = multiline_string | string_literal ;

multiline_string = '"""' < any characters except """ > '"""' ;
```

Both multiline strings (`"""..."""`) and regular strings (`"..."`) are accepted.

**Examples:**
```
after {
    """
    UPDATE sales_order
    SET created_at = DATE_ADD(created_at, INTERVAL 10 YEAR)
    """

    "UPDATE customer_entity SET password_hash = NULL"
    "TRUNCATE TABLE sessions"
}
```

### Expressions

Expressions are used in conditional attributes and string interpolation.

```ebnf
expression = or_expr ;

or_expr = and_expr { "||" and_expr } ;

and_expr = equality_expr { "&&" equality_expr } ;

equality_expr = comparison_expr { ( "==" | "!=" ) comparison_expr } ;

comparison_expr = additive_expr { ( "<" | ">" | "<=" | ">=" ) additive_expr } ;

additive_expr = multiplicative_expr { ( "+" | "-" ) multiplicative_expr } ;

multiplicative_expr = unary_expr { ( "*" | "/" | "%" ) unary_expr } ;

unary_expr = [ "!" | "-" ] primary_expr ;

primary_expr = literal
             | variable_ref
             | "(" expression ")"
             ;
```

**Operator Precedence (highest to lowest):**

| Precedence | Operators | Associativity |
|------------|-----------|---------------|
| 1 | `!` `-` (unary) | Right |
| 2 | `*` `/` `%` | Left |
| 3 | `+` `-` | Left |
| 4 | `<` `>` `<=` `>=` | Left |
| 5 | `==` `!=` | Left |
| 6 | `&&` | Left |
| 7 | `\|\|` | Left |

**Examples:**
```
#[when($skip_payments)]
#[when($order_limit > 50)]
#[when($env == "staging")]
#[when(!$debug_mode)]
#[when($debug_mode && $env != "production")]
#[when(($env == "staging" || $env == "development") && $debug_mode)]
```

### String Interpolation

```ebnf
interpolated_string = '"' { string_char | interpolation } '"' ;

string_char = < any character except " \ { >
            | escape_sequence
            ;

escape_sequence = "\\" | '\"' | "\n" | "\t" | "\{" ;

interpolation = "{" expression "}" ;
```

**Examples:**
```
"https://{$base_domain}/"
"http://{$base_domain}:{$port}/"
"http://localhost:{$base_port + $instance}/"
"api-{$env}.example.com/v{$version}/"
"Use \{$var} for interpolation"  /* Escaped brace */
```

### Lexical Elements

```ebnf
identifier = letter { letter | digit | "_" } ;

letter = "a" | "b" | ... | "z" | "A" | "B" | ... | "Z" ;

digit = "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" ;

integer_literal = [ "-" ] digit { digit } ;

float_literal = [ "-" ] digit { digit } "." digit { digit } ;

string_literal = '"' { string_char } '"' ;

variable_ref = "$" identifier ;

comment = "//" { < any character except newline > } newline ;

whitespace = " " | "\t" | "\n" | "\r" ;
```

### Reserved Keywords

The following identifiers are reserved keywords and cannot be used as user-defined names:

```
aggregate    after       anonymize       asc           bool
by           desc        exclude_data    faker         false
float        ignore_table import         include       int
limit        match       null            order         preserve
relation     root        set             string        true
var          when        where
```

## Complete Grammar (Consolidated)

```ebnf
(* Program Structure *)
program = { attributed_statement } ;

attributed_statement = [ attribute ] statement ;

attribute = "#[" "when" "(" expression ")" "]" ;

statement = import_stmt
          | var_decl
          | faker_decl
          | relation_decl
          | anonymize_block
          | exclude_data_stmt
          | ignore_table_stmt
          | aggregate_block
          | include_stmt
          | preserve_stmt
          | set_block
          | after_block
          ;

(* Statements *)
import_stmt = "import" string_literal ;

var_decl = "var" identifier ":" type [ "=" literal ] ;

faker_decl = "faker" identifier "[" string_list "]" ;

relation_decl = "relation" column_ref "->" column_ref ;

anonymize_block = "anonymize" identifier "{" { anonymize_rule } "}" ;

exclude_data_stmt = "exclude_data" identifier ;

ignore_table_stmt = "ignore_table" identifier ;

aggregate_block = "aggregate" identifier "{" aggregate_body "}" ;

include_stmt = "include" identifier "where" string_literal ;

preserve_stmt = "preserve" identifier "where" string_literal ;

set_block = "set" identifier "{" match_clause { assignment } "}" ;

after_block = "after" "{" { sql_string } "}" ;

sql_string = multiline_string | string_literal ;

(* Aggregate Clauses *)
aggregate_body = root_clause [ where_clause ] [ order_by_clause ] [ limit_clause ] ;

root_clause = "root" identifier ;

where_clause = "where" string_literal ;

order_by_clause = "order" "by" identifier [ sort_direction ] ;

sort_direction = "asc" | "desc" ;

limit_clause = "limit" ( integer_literal | variable_ref ) ;

(* Anonymize Rules *)
anonymize_rule = identifier "->" ( identifier | "null" ) ;

(* Set Block Components *)
match_clause = "match" assignment_list ;

assignment_list = assignment { "," assignment } ;

assignment = identifier "=" value ;

(* Types and Values *)
type = "string" | "int" | "float" | "bool" ;

literal = string_literal | integer_literal | float_literal | bool_literal ;

value = literal | variable_ref ;

bool_literal = "true" | "false" ;

(* References *)
column_ref = identifier "." identifier ;

variable_ref = "$" identifier ;

string_list = string_literal { "," string_literal } ;

(* Expressions *)
expression = or_expr ;

or_expr = and_expr { "||" and_expr } ;

and_expr = equality_expr { "&&" equality_expr } ;

equality_expr = comparison_expr { ( "==" | "!=" ) comparison_expr } ;

comparison_expr = additive_expr { ( "<" | ">" | "<=" | ">=" ) additive_expr } ;

additive_expr = multiplicative_expr { ( "+" | "-" ) multiplicative_expr } ;

multiplicative_expr = unary_expr { ( "*" | "/" | "%" ) unary_expr } ;

unary_expr = [ "!" | "-" ] primary_expr ;

primary_expr = literal | variable_ref | "(" expression ")" ;

(* String Interpolation *)
interpolated_string = '"' { string_char | interpolation } '"' ;

string_char = < any character except " \ { > | escape_sequence ;

escape_sequence = "\\" | '\"' | "\n" | "\t" | "\{" ;

interpolation = "{" expression "}" ;

(* Lexical Elements *)
identifier = letter { letter | digit | "_" } ;

letter = "a" | ... | "z" | "A" | ... | "Z" ;

digit = "0" | ... | "9" ;

integer_literal = [ "-" ] digit { digit } ;

float_literal = [ "-" ] digit { digit } "." digit { digit } ;

string_literal = interpolated_string ;

multiline_string = '"""' { < any character > } '"""' ;

comment = "//" { < any character except newline > } ;

whitespace = " " | "\t" | "\n" | "\r" ;
```

## Semantic Notes

### Identifier Scope

- **Variables** (`var`): Global scope, must be declared before use
- **Faker names**: Global scope, must be declared before use in `anonymize`
- **Aggregate names**: Global scope, must be declared before use in `include`
- **Table names**: Not validated at parse time; validated at runtime against database schema

### Type Coercion

| Context | Allowed Types | Coercion |
|---------|---------------|----------|
| String interpolation | `string`, `int`, `float`, `bool` | All convert to string |
| Arithmetic (`+`, `-`, `*`, `/`) | `int`, `float` | `int` promotes to `float` if mixed |
| Comparison (`<`, `>`, etc.) | `int`, `float`, `string` | Numeric comparison or lexicographic |
| Equality (`==`, `!=`) | All | Type must match |
| Logical (`&&`, `\|\|`, `!`) | `bool` | No coercion |
| `limit` clause | `int` | Must be positive integer |

### String Literal Contexts

| Context | Interpolation | Multiline |
|---------|---------------|-----------|
| `import` path | Yes | No |
| `faker` values | Yes | No |
| `where` clause | Yes | No |
| `set` values | Yes | No |
| `match` values | Yes | No |
| `after` SQL | Yes | Yes (`"""`) |
| Variable defaults | Yes | No |

### Order Independence

Statements can appear in any order in the file, with these exceptions:
- `var` declarations should appear before their use in expressions
- `faker` declarations should appear before their use in `anonymize`
- `aggregate` declarations should appear before their use in `include`
- `import` statements are typically at the top but can appear anywhere

### Whitespace and Comments

- Whitespace is ignored except as token separator
- Comments start with `//` and extend to end of line
- Comments can appear on their own line or after a statement
- Comments inside strings are not treated as comments
