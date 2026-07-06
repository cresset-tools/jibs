# Jibs DSL Specification

A domain-specific language for directing MySQL database imports from remote
(production) databases to local development environments. Configuration files
use the `.jibs` extension.

Code examples in this document marked as `jibs` are parsed by the test suite
(`crates/parser/tests/corpus.rs`), so they cannot drift from the
implementation.

## Overview

This DSL enables:
- Selective table imports with row filtering
- Automatic foreign key relationship following (aggregates)
- Manual soft relation definitions
- Data anonymization with fake data
- On-demand fetches of specific rows (`get` functions)
- Preserving local rows and post-import transformations
- Conditional configuration

## Table of Contents

1. [Variables](#variables)
2. [String Interpolation](#string-interpolation)
3. [Imports](#imports)
4. [Faker Data Sources](#faker-data-sources)
5. [Relations](#relations)
6. [Anonymization](#anonymization)
7. [Table Handling](#table-handling)
8. [Aggregates](#aggregates)
9. [Get Functions](#get-functions)
10. [Preserve](#preserve)
11. [Set (Upsert)](#set-upsert)
12. [After Transformations](#after-transformations)
13. [Conditionals](#conditionals)
14. [Comments](#comments)
15. [CLI Interface](#cli-interface)
16. [Variable Files](#variable-files)
17. [Complete Example](#complete-example)

---

## Variables

Variables allow runtime configuration of the import. They are typed and can
have default values.

### Syntax

```text
var <name>: <type>
var <name>: <type> = <default_value>
```

### Supported Types

| Type | Description | Example Values |
|------|-------------|----------------|
| `string` | Text value | `"https://example.com"` |
| `int` | Integer number | `100`, `-5`, `0` |
| `bool` | Boolean | `true`, `false` |
| `float` | Floating point | `3.14`, `-0.5` |
| `string[]` | Array of strings | `["a", "b", "c"]` |
| `int[]` | Array of integers | `[1, -2, 3]` |
| `bool[]` | Array of booleans | `[true, false]` |
| `float[]` | Array of floats | `[1.5, -2.5]` |

### Examples

```jibs
// Required variable (must be provided at runtime)
var base_url: string

// Variables with default values
var admin_email: string = "admin@local.test"
var debug_mode: bool = false
var order_limit: int = 100
var tax_rate: float = 0.21
var offset: int = -3

// Array variables
var test_emails: string[] = ["user1@test.com", "user2@test.com"]
var allowed_ids: int[] = [1, 2, 3, 4, 5]
var feature_flags: bool[] = [true, false, true]
var discount_rates: float[] = [0.1, 0.15, 0.2]
```

### Usage

Variables are referenced with the `$` prefix:

```jibs
var order_limit: int = 100
var base_url: string = "https://local.test/"

aggregate orders {
    root sales_order
    limit $order_limit
}

set core_config_data {
    match path = "web/secure/base_url", scope = "default", scope_id = 0
    value = $base_url
}
```

A variable without a default must be provided at runtime with `--var` or
`--var-file`, otherwise resolution fails. Values from `--var` are strings and
are converted to the declared type.

---

## String Interpolation

Strings support interpolation using `{$variable}` or `{expression}` syntax.

### Syntax

```text
"text {$variable} more text"
"text {$variable + 1} more text"
```

### Basic Variable Interpolation

```jibs
var base_domain: string = "example.com"
var port: int = 8080

set core_config_data {
    match path = "web/unsecure/base_url", scope = "default", scope_id = 0
    value = "http://{$base_domain}:{$port}/"
}
```

### Type Conversion

All types are automatically converted to strings when interpolated:

| Type | Conversion |
|------|------------|
| `string` | As-is |
| `int` | Decimal representation (`100` → `"100"`) |
| `float` | Decimal representation (`3.14` → `"3.14"`) |
| `bool` | `"true"` or `"false"` |

### Expressions in Interpolation

Interpolation supports full expressions including arithmetic and comparisons:

```jibs
var base_port: int = 8000
var instance: int = 1

// Results in: "http://localhost:8001/"
set core_config_data {
    match path = "web/unsecure/base_url", scope = "default", scope_id = 0
    value = "http://localhost:{$base_port + $instance}/"
}
```

### Supported Expression Operators

| Type | Operators |
|------|-----------|
| Arithmetic | `+`, `-`, `*`, `/`, `%` |
| Comparison | `==`, `!=`, `>`, `<`, `>=`, `<=` |
| Logical | `&&`, `\|\|`, `!` |
| Grouping | `(`, `)` |

Statement keywords (like `limit` or `order`) are valid variable names inside
interpolations: `"{$limit}"` works even though `limit` is a keyword in the
statement grammar.

### unique()

Inside faker pool values, `{unique()}` is replaced with an incrementing
counter when rows are written, letting a small pool generate values that
satisfy UNIQUE constraints:

```jibs
faker emails ["user{unique()}@example.test"]

anonymize customer_entity {
    email -> emails
}
```

### Escaping

| Escape | Meaning |
|--------|---------|
| `\{` | Literal `{` (prevents interpolation) |
| `\\` | Literal backslash |
| `\"` | Literal double quote |
| `\n` | Newline |
| `\t` | Tab |
| `\<other>` | Kept as-is (both characters) |

Unknown escapes passing through unchanged means SQL escape sequences like
`\%` in `LIKE` patterns reach MySQL untouched:

```jibs
preserve core_config_data where "path LIKE 'dev\_%'"
```

A `{` that does not start a valid interpolation is a parse error (with a
`did you mean {$name}?` hint for the common `${name}` typo) — write `\{`
for a literal brace.

### Where Interpolation Works

String interpolation is supported in:
- Variable default values (including string arrays)
- `where` clause strings (aggregates, get functions, preserve)
- `set` block values and `match` clause string values
- `faker` list values
- Strings inside `#[when(...)]` conditions

It is **not** applied in:
- `import` paths (raw strings)
- `after` block SQL statements (raw strings, both `"..."` and `"""..."""`)

---

## Imports

Import other `.jibs` files to compose configurations. Useful for sharing
common rules across projects.

### Syntax

```text
import "<path>"
```

Paths are resolved relative to the importing file. Import paths are raw
strings: no interpolation is applied.

### Examples

```jibs
// Import from same directory
import "anonymization-rules.jibs"

// Import from subdirectory
import "magento/base.jibs"
```

### Import Resolution

- Each file is processed at most once: if a file is imported again (directly,
  via a diamond `A -> B,C -> D`, or via a cycle), the repeat import is
  silently skipped.
- Resolution is depth-first: each import is fully resolved (including its
  nested imports) before proceeding to the next statement.

### Statement Ordering

When multiple files are involved, statements are collected in depth-first
order:

1. First import's statements (including its nested imports, depth-first)
2. Second import's statements (including its nested imports)
3. ... and so on for each import in the file
4. Current file's statements (top to bottom)

This ordering affects:
- **Variable declarations**: later declarations with the same name override earlier ones
- **Faker definitions**: later definitions with the same name override earlier ones
- **Anonymization rules**: later rules for the same table override earlier ones
- **After blocks**: executed in collection order (see [After Transformations](#after-transformations))

---

## Faker Data Sources

Define lists of fake data to use for anonymization.

### Syntax

```text
// Inline array of values
faker <name> [<value1>, <value2>, ...]

// Using a string[] variable directly
faker <name> $<variable>

// Using spread operator to combine values
faker <name> [...$<variable>, <value>, ...]
```

### Examples

```jibs
faker names ["John", "Jane", "Bob", "Alice", "Charlie", "Diana"]
faker emails ["user1@example.test", "user2@example.test"]
faker phones ["+31600000001", "+31600000002", "+31600000003"]

// Define the values in a variable and use it directly
var base_emails: string[] = ["admin@test.com", "user@test.com"]
faker admin_emails $base_emails

// Spread operator combines variables and inline values
var team_emails: string[] = ["alice@company.com", "bob@company.com"]
faker all_emails [...$team_emails, "extra@test.com", ...$base_emails]
```

### Behavior

- A **random** value from the pool is chosen for each row. The same input
  value does not map to the same fake value; runs are not deterministic.
- For columns with UNIQUE constraints, use `{unique()}` in the pool values
  (see [String Interpolation](#unique)) — plain pools will produce duplicate
  values as soon as there are more rows than pool entries.
- The spread operator (`...$var`) requires a `string[]` variable.

---

## Relations

Foreign key relationships drive aggregate traversal. Single-column foreign
key constraints are discovered automatically from the remote schema;
`relation` adds soft relationships that are not declared in the schema, and
`ignore_relation` removes discovered ones.

### Syntax

```text
relation <table>.<column> -> <referenced_table>.<referenced_column>
ignore_relation <table>.<column> -> <referenced_table>.<referenced_column>
```

Direction is from child table to parent table: the arrow points at the
referenced (parent) table.

### Examples

```jibs
// Customer group reference (not a formal FK in Magento)
relation customer_entity.group_id -> customer_group.customer_group_id

// Order to customer reference
relation sales_order.customer_id -> customer_entity.entity_id

// Drop a discovered FK that would pull in too much data
ignore_relation sales_order.store_id -> store.store_id
```

### Behavior

- Explicit relations are used in addition to auto-discovered foreign keys.
- Composite (multi-column) foreign keys are not auto-discovered.
- `ignore_relation` filters a relation out of the discovered set, so
  aggregate traversal will not follow it.

---

## Anonymization

Define rules for replacing sensitive data with fake values. Anonymization is
applied **on the remote server as rows are read**, so sensitive values never
leave the remote host. It applies to every import mode, including `get`.

### Syntax

```text
anonymize <table> {
    <column> -> <faker_name>
    <column> -> null
}
```

### Examples

```jibs
faker emails ["user1@example.test", "user2@example.test"]
faker names ["John", "Jane", "Bob", "Alice"]

anonymize customer_entity {
    email       -> emails
    firstname   -> names
    lastname    -> names
    password    -> null
    rp_token    -> null
}
```

### Special Values

| Value | Behavior |
|-------|----------|
| `<faker_name>` | Replace with a random value from the faker pool |
| `null` | Set column to NULL |

---

## Table Handling

Control how tables are handled during import. All three statements accept
either an exact table name or a `/regex/` pattern that is matched against
all table names on the remote server.

### Exclude Data

Import table structure but no data. Creates an empty table locally.

```jibs
exclude_data sales_order_payment
exclude_data customer_log

// All tables whose name ends in _log
exclude_data /_log$/
```

### Ignore Table

Completely ignore the table: don't create structure, don't import data,
don't touch any existing local table.

```jibs
ignore_table report_event

// Regex: all report and cache tables
ignore_table /^report_/
ignore_table /^cache/
```

An aggregate whose root table is ignored is a configuration error. Relations
into ignored tables are not followed.

### Full Tables

Force tables to be imported in full even when they are reachable from an
aggregate. During aggregate traversal they act as dead ends (their rows do
not pull in further related rows). Accepts a comma-separated list.

```jibs
full customer_group, store
full /^catalog_category/
```

### Table Name Quirks

Table names that clash with keywords or contain unusual characters can be
written with backticks:

```jibs
ignore_table `quote_2023-08-17`
```

Regex pattern bodies cannot contain whitespace (use `\s` or `[ ]` for a
literal space) — this is what distinguishes them from the division operator
in expressions.

### Use Cases

| Scenario | Use |
|----------|-----|
| Sensitive data (payments, tokens) | `exclude_data` - keep structure for app compatibility |
| Report/analytics tables | `ignore_table` - not needed locally, may be huge |
| Cache tables | `exclude_data` - keep structure, data regenerates |
| Small reference tables (statuses, groups) | `full` - always complete |

---

## Aggregates

Define a root entity and automatically import all related data following
foreign key relationships (both schema-defined and soft relations).

### Syntax

```text
aggregate <name> {
    root <table>
    where "<sql_condition>"
    order by <column> [asc|desc]
    limit <number or $variable>
    exclude <table_or_pattern>, ...
    root_only
}
```

Only `root` is required. Clauses must appear in the order shown.

| Clause | Description |
|--------|-------------|
| `root` | The main table (aggregate root) |
| `where` | SQL WHERE condition to filter root rows (interpolated) |
| `order by` | Sort order for limit selection |
| `limit` | Maximum root rows to import (literal or `$variable`) |
| `exclude` | Tables (or `/regex/` patterns) to skip during traversal |
| `root_only` | Import only the root table's rows, no traversal |

### Examples

```jibs
var order_limit: int = 100

// Import recent orders and everything they reference
aggregate orders {
    root sales_order
    where "created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)"
    order by created_at desc
    limit $order_limit
}

// Import products from one category, but skip the url_rewrite subtree
aggregate products {
    root catalog_product_entity
    where "entity_id IN (SELECT product_id FROM catalog_category_product WHERE category_id = 42)"
    exclude /^url_rewrite/, catalog_product_flat_1
}

// Only the matching rows themselves
aggregate flagged_customers {
    root customer_entity
    where "is_active = 0"
    root_only
}
```

### Traversal Behavior

Starting from the root rows:

1. **Forward relations** (rows this row references — its parents/dependencies)
   are always followed, so imported rows never dangle.
2. **Backward relations** (rows that reference this row — its children) are
   followed from the root table and from tables reached via backward
   relations. They are *not* followed from tables that were only reached
   forward — otherwise importing one order would pull in its customer and
   then *all* of that customer's other data.
3. Rows are deduplicated: each row is transferred at most once, even when
   multiple aggregates or paths reach it.
4. Tables named in `full` statements are imported completely and act as
   traversal dead ends. Tables in `exclude` clauses or `exclude_data` /
   `ignore_table` statements are skipped.
5. Anonymization rules apply during the transfer.

Tables that are *reachable* from an aggregate root through relations are
reserved for aggregate traversal; all other tables are imported in full
(unless excluded/ignored).

---

## Get Functions

Define parameterized fetches that can be invoked from the command line with
`jibs get`. Use them to pull specific rows (and everything they reference)
into an existing local database without redoing a full import — e.g. "get me
this one production order".

### Syntax

```text
get <name> (<param>: <type> [= default], ...) {
    <aggregate_name>
    where "<sql_condition>"
    order by <column> [asc|desc]
    limit <number or $variable>
    exclude <table_or_pattern>, ...
    root_only
}
```

The body names an aggregate defined elsewhere in the file; the optional
clauses override or replace that aggregate's clauses for this fetch. The
`where` string can interpolate the function's parameters.

### Examples

```jibs
aggregate products {
    root catalog_product_entity
    where "FALSE"
}

get product_by_sku (sku: string) {
    products where "sku = '{$sku}'"
}

get products_updated_since (days: int = 7, max: int = 100) {
    products
    where "updated_at > DATE_SUB(NOW(), INTERVAL {$days} DAY)"
    limit $max
}
```

The `where "FALSE"` pattern makes an aggregate import nothing during a
regular `jibs import`, reserving it for `get` invocations.

### Invocation

```bash
jibs get shop.jibs [connection options] -- product_by_sku --sku HERO-PRODUCT

# Multiple invocations in one run
jibs get shop.jibs [connection options] -- \
    product_by_sku --sku HERO-PRODUCT \
    products_updated_since --days 30
```

### Behavior

- Only the named aggregates are fetched (`aggregates_only` mode); full-table
  imports are skipped.
- Traversal, deduplication, and **anonymization** work exactly as in a
  regular import.
- Fetched tables are dropped and recreated locally with the fetched rows.
- `get` refuses to run if state from an interrupted import exists (backup
  tables or a checkpoint); pass `--clean` to discard that state explicitly.

---

## Preserve

Keep local values for specific rows; don't overwrite them during import.

### Syntax

```text
preserve <table> where "<sql_condition>"
```

### Examples

```jibs
// Keep local development settings
preserve core_config_data where "path LIKE 'dev/%'"

// Keep local admin account
preserve admin_user where "username = 'localadmin'"
```

### Behavior

1. Before the table is imported, matching local rows are copied to a backup
   table (`_jibs_preserve_<table>`).
2. The table is imported from remote as usual.
3. After the import, the preserved rows are written back (`REPLACE INTO`)
   and the backup table is dropped.

If an import is interrupted, the backup tables remain so a later `--resume`
can finish the restore. `--clean` discards them (destroying preserved rows
whose originals were already overwritten) — hence the explicit flag.

---

## Set (Upsert)

Set specific values after import. If the row doesn't exist, it is created.

### Syntax

```text
set <table> {
    match <column> = <value>, <column> = <value>, ...
    <column> = <value>
    <column> = <value>
}
```

| Clause | Description |
|--------|-------------|
| `match` | Columns used to find the existing row (or create a new one) |
| Other assignments | Columns to set/update |

Values can be literals, `$variables`, or interpolated strings.

### Examples

```jibs
var base_url: string = "https://local.test/"
var admin_email: string = "admin@local.test"

set core_config_data {
    match path = "web/secure/base_url", scope = "default", scope_id = 0
    value = $base_url
}

set core_config_data {
    match path = "trans_email/ident_general/email", scope = "default", scope_id = 0
    value = $admin_email
}
```

### Behavior

1. Look for a row matching all `match` column values.
2. If found: update the other columns.
3. If not found: insert a new row with the match columns plus the other
   columns.

---

## After Transformations

Execute arbitrary SQL after import completes. SQL strings are **raw**: no
interpolation or escape processing is applied, so SQL wildcards and quotes
can be written directly.

### Syntax

```text
after {
    """<multiline_sql_statement>"""
    "<single_line_sql_statement>"
}
```

Both multiline strings (`"""..."""`) and regular strings (`"..."`) are
supported.

### Examples

```jibs
after {
    """
    UPDATE sales_order
    SET created_at = DATE_ADD(created_at, INTERVAL 10 YEAR),
        updated_at = DATE_ADD(updated_at, INTERVAL 10 YEAR)
    """

    "TRUNCATE TABLE cache"
    "DELETE FROM sessions WHERE expired_at < NOW()"
}
```

### Behavior

- Statements execute in order after all data imports, preserve restores, and
  `set` blocks complete.
- Each statement is executed as a separate query.
- An error stops execution; already-executed statements are not rolled back.

### Execution Order with Imports

When using imports, `after` blocks are collected and executed in depth-first
order based on import position:

1. First import's `after` statements (including its nested imports, depth-first)
2. Second import's `after` statements (including its nested imports)
3. ... and so on for each import in the file
4. Current file's `after` statements (top to bottom)

**Example:** given `root.jibs` importing `a.jibs` then `b.jibs`, where
`a.jibs` imports `c.jibs`, and every file has an `after` block, the
execution order is: `c`, `a`, `b`, `root`.

---

## Conditionals

Apply any statement conditionally based on variable values using Rust-like
attributes.

### Syntax

```text
#[when(<expression>)]
<statement>
```

The attribute can be applied to **any** statement kind. Expressions use the
same operators as string interpolation (see
[Supported Expression Operators](#supported-expression-operators)).

### Examples

```jibs
var skip_payments: bool = true
var order_limit: int = 100
var env: string = "staging"
var debug_mode: bool = true

#[when($skip_payments)]
exclude_data sales_order_payment

#[when($order_limit > 50)]
aggregate large_orders {
    root sales_order
    order by created_at desc
    limit $order_limit
}

#[when($env == "staging")]
set core_config_data {
    match path = "dev/debug/enabled", scope = "default", scope_id = 0
    value = "1"
}

#[when(!$skip_payments)]
aggregate payments {
    root sales_order_payment
}

#[when($debug_mode && $env != "production")]
after {
    "UPDATE core_config_data SET value = '1' WHERE path = 'dev/log/active'"
}

#[when(($env == "staging" || $env == "development") && $debug_mode)]
exclude_data customer_log
```

Note that an attribute applies to exactly one following statement.

---

## Comments

Single-line comments start with `//` and run to the end of the line. They
can appear on their own line or after a statement.

```jibs
// === VARIABLES ===
var base_url: string  // Required: no default

aggregate orders {
    root sales_order
    // Only import recent orders to keep the database small
    where "created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)"
    limit 100  // Adjust as needed
}
```

---

## CLI Interface

The `jibs` binary connects to a remote host over SSH, runs a helper binary
there (uploaded automatically), streams the selected data, and loads it into
a local MySQL server.

### Commands

```text
jibs import [CONFIG] [options]        Import data from a remote database
jibs get <CONFIG> [options] -- ...    Fetch specific aggregates (get functions)
jibs check <CONFIG>                   Parse and validate a config file
jibs plan <CONFIG> [--var ...]        Print the resolved execution plan (JSON)
```

`jibs import` without a config file imports all tables.

### Connection Options (import and get)

| Option | Description |
|--------|-------------|
| `--host <user@host>` | Remote SSH host (required; set the SSH port with `--port`) |
| `--port <port>` | SSH port (default: 22) |
| `--identity <file>` | Path to SSH private key |
| `--remote-mysql <url>` | MySQL URL on the remote host (default: `mysql://root@localhost:3306`) |
| `--local-mysql <url>` | Local MySQL URL (default: `mysql://root@localhost:3306`) |
| `--var <name>=<value>` | Set a variable (repeatable) |
| `--var-file <file>` | Load variables from a JSON file |
| `--parallel <n>` | Parallel server-side workers (default: 1) |
| `--client-parallel <n>` | Parallel local loader workers (defaults to `--parallel`) |
| `--no-compress` | Disable zstd compression |
| `--strict-host-key-checking` | Reject unknown SSH host keys |
| `--accept-new-host-keys` | Accept and save new host keys, reject mismatches |
| `--no-host-key-checking` | Disable host key checking (insecure) |
| `--max-message-size <bytes>` | Maximum protocol message size (default: 100MB) |
| `--metrics` | Print detailed timing metrics |
| `--report` | Print a report of the slowest tables |

### Import-Specific Options

| Option | Description |
|--------|-------------|
| `--dry-run` | Report what would be imported without touching the local database |
| `--resume` | Resume a previously interrupted import |
| `--clean` | Discard state from a previous interrupted import and start fresh |

`--dry-run` connects to the remote host, classifies every table the way the
import would (aggregate / full / excluded), and counts how many root rows
currently match each aggregate's where clause — without connecting to the
local database at all.

An interrupted import leaves a checkpoint table and possibly preserve-backup
tables in the local database; the next import must choose `--resume` or
`--clean`. Foreign key constraints in the local schema are dropped during an
import and automatically restored afterwards (or on the next successful run
after an interruption).

### Examples

```bash
# Full import according to shop.jibs
jibs import shop.jibs \
    --host deploy@prod-db.example.com \
    --remote-mysql 'mysql://reader:secret@localhost:3306/shop' \
    --local-mysql 'mysql://root:root@127.0.0.1:3306/shop_dev' \
    --parallel 4

# Override variables
jibs import shop.jibs --host deploy@prod \
    --var base_domain=local.test \
    --var order_limit=50

# Resume after an interruption
jibs import shop.jibs --host deploy@prod --resume

# Fetch one production order into the local database
jibs get shop.jibs --host deploy@prod -- order_by_increment_id --id 100000999

# Validate a config file (parse errors show source snippets)
jibs check shop.jibs

# Inspect what would be imported
jibs plan shop.jibs --var base_domain=local.test
```

---

## Variable Files

Variables can be loaded from a JSON file with `--var-file`:

```json
{
    "base_domain": "local.dev",
    "base_port": 8080,
    "admin_email": "admin@local.test",
    "skip_payments": true,
    "order_limit": 100,
    "tax_rate": 0.21
}
```

### Precedence

1. Default values in the DSL (`var x: int = 100`)
2. Variable file (`--var-file`)
3. CLI overrides (`--var x=200`)

Later sources override earlier ones.

---

## Complete Example

**magento-shop.jibs:**
```jibs
// Base configuration for a Magento shop import

import "magento-anonymization.jibs"

// === VARIABLES ===
var base_domain: string
var base_port: int = 80
var admin_email: string = "admin@local.test"
var env: string = "development"
var skip_payments: bool = true
var order_limit: int = 100
var product_category: int = 42

// === SOFT RELATIONS ===
relation customer_entity.group_id -> customer_group.customer_group_id
relation sales_order.customer_id -> customer_entity.entity_id

// === TABLE HANDLING ===
// Always ignore reporting tables
ignore_table /^report_/
ignore_table /^sales_bestsellers_aggregated/

// Small reference tables: always import completely
full customer_group, store

// Conditionally skip payment data
#[when($skip_payments)]
exclude_data sales_order_payment

// === AGGREGATES ===
aggregate orders {
    root sales_order
    where "created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)"
    order by created_at desc
    limit $order_limit
}

aggregate products {
    root catalog_product_entity
    where "entity_id IN (SELECT product_id FROM catalog_category_product WHERE category_id = {$product_category})"
    exclude /^url_rewrite/
}

// === ON-DEMAND FETCHES ===
get product_by_sku (sku: string) {
    products where "sku = '{$sku}'"
}

// === PRESERVE LOCAL VALUES ===
preserve core_config_data where "path LIKE 'dev/%'"

// === SET VALUES ===
set core_config_data {
    match path = "web/secure/base_url", scope = "default", scope_id = 0
    value = "https://{$base_domain}/"
}

set core_config_data {
    match path = "web/unsecure/base_url", scope = "default", scope_id = 0
    value = "http://{$base_domain}:{$base_port}/"
}

set core_config_data {
    match path = "trans_email/ident_general/email", scope = "default", scope_id = 0
    value = $admin_email
}

// === POST-IMPORT ===
#[when($env == "development")]
after {
    """
    UPDATE sales_order
    SET created_at = DATE_ADD(created_at, INTERVAL 10 YEAR),
        updated_at = DATE_ADD(updated_at, INTERVAL 10 YEAR)
    """
}
```

**magento-anonymization.jibs:**
```jibs
// Shared anonymization rules for Magento

var base_names: string[] = ["John", "Jane", "Bob", "Alice"]
var extra_names: string[] = ["Charlie", "Diana", "Eve", "Frank"]

// Spread operator combines arrays; unique() guarantees unique emails
faker names [...$base_names, ...$extra_names]
faker emails ["user{unique()}@example.test"]
faker phones ["+31600000001", "+31600000002", "+31600000003"]

anonymize customer_entity {
    email       -> emails
    firstname   -> names
    lastname    -> names
    password    -> null
    rp_token    -> null
}

anonymize sales_order_address {
    email       -> emails
    firstname   -> names
    lastname    -> names
    telephone   -> phones
    fax         -> null
}

anonymize sales_order {
    customer_email     -> emails
    customer_firstname -> names
    customer_lastname  -> names
}
```

**Usage:**
```bash
jibs import magento-shop.jibs \
    --host deploy@prod-db \
    --var-file local.vars.json \
    --var base_domain=local.dev
```
