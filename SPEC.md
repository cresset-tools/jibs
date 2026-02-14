# MySQL Import DSL Specification

A domain-specific language for directing MySQL database imports from remote (production) databases to local development environments.

## Overview

This DSL enables:
- Selective table imports with row filtering
- Automatic foreign key relationship following (aggregates)
- Manual soft relation definitions
- Data anonymization with fake data
- Incremental imports
- Post-import transformations
- Conditional configuration

## Table of Contents

1. [Variables](#variables)
2. [String Interpolation](#string-interpolation)
3. [Imports](#imports)
5. [Faker Data Sources](#faker-data-sources)
6. [Relations](#relations)
7. [Anonymization](#anonymization)
8. [Table Handling](#table-handling)
9. [Aggregates](#aggregates)
10. [Incremental Imports](#incremental-imports)
11. [Preserve](#preserve)
12. [Set (Upsert)](#set-upsert)
13. [After Transformations](#after-transformations)
14. [Conditionals](#conditionals)
15. [Comments](#comments)
16. [CLI Interface](#cli-interface)
17. [Variable Files](#variable-files)

---

## Variables

Variables allow runtime configuration of the import. They are typed and can have default values.

### Syntax

```
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

### Examples

```
# Required variable (must be provided at runtime)
var base_url: string

# Variable with default value
var admin_email: string = "admin@local.test"
var debug_mode: bool = false
var order_limit: int = 100
var tax_rate: float = 0.21
```

### Usage

Variables are referenced with the `$` prefix:

```
aggregate orders {
    root sales_order
    limit $order_limit
}

set core_config_data {
    match path = "web/secure/base_url", scope = "default", scope_id = 0
    value = $base_url
}
```

---

## String Interpolation

Strings support variable interpolation using `{$variable}` syntax. This allows building dynamic strings from variables and expressions.

### Syntax

```
"text {$variable} more text"
"text {$variable + 1} more text"
"text {expression} more text"
```

### Basic Variable Interpolation

```
var base_domain: string = "example.com"
var port: int = 8080

set core_config_data {
    match path = "web/secure/base_url", scope = "default", scope_id = 0
    value = "https://{$base_domain}/"
}

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

```
var order_limit: int = 100
var debug: bool = true

# Results in: "Importing 100 orders"
set core_config_data {
    match path = "import/status/message", scope = "default", scope_id = 0
    value = "Importing {$order_limit} orders"
}
```

### Expressions in Interpolation

Interpolation supports full expressions including arithmetic and comparisons:

```
var base_port: int = 8000
var instance: int = 1

# Results in: "http://localhost:8001/"
set core_config_data {
    match path = "web/unsecure/base_url", scope = "default", scope_id = 0
    value = "http://localhost:{$base_port + $instance}/"
}
```

```
var env: string = "staging"
var version: int = 2

# Build dynamic paths
set core_config_data {
    match path = "api/endpoint", scope = "default", scope_id = 0
    value = "https://api-{$env}.example.com/v{$version}/"
}
```

### Supported Expression Operators

| Type | Operators |
|------|-----------|
| Arithmetic | `+`, `-`, `*`, `/`, `%` |
| String | `+` (concatenation inside interpolation) |
| Comparison | `==`, `!=`, `>`, `<`, `>=`, `<=` |
| Logical | `&&`, `||`, `!` |
| Grouping | `(`, `)` |

### Complex Examples

**Building URLs with multiple components:**
```
var protocol: string = "https"
var domain: string = "shop.example.com"
var port: int = 443
var path: string = "api/v2"

set core_config_data {
    match path = "api/base_url", scope = "default", scope_id = 0
    value = "{$protocol}://{$domain}:{$port}/{$path}/"
}
```

**Using in WHERE clauses:**
```
var category_base: int = 100
var category_offset: int = 5

aggregate products {
    root catalog_product_entity
    where "entity_id IN (
        SELECT product_id
        FROM catalog_category_product
        WHERE category_id = {$category_base + $category_offset}
    )"
}
```

**Conditional string building (via ternary - if supported):**
```
var use_ssl: bool = true
var domain: string = "example.com"

# If ternary is supported:
# value = "{$use_ssl ? "https" : "http"}://{$domain}/"
```

### Escaping

To include a literal `{` in a string, escape it with a backslash:

```
value = "Use \{$variable} for interpolation"
# Results in: "Use {$variable} for interpolation"
```

### Where Interpolation Works

String interpolation is supported in:
- Variable default values
- `where` clause strings
- `set` block values
- `match` clause string values
- `after` block SQL statements
- `faker` list values
- `import` paths

---

## Imports

Import other DSL files to compose configurations. Useful for sharing common rules across projects.

### Syntax

```
import "<path>"
```

### Path Resolution

1. Relative to the current file
2. Relative to directories in `MYSQLIMPORT_INCLUDE_PATH` environment variable
3. Relative to directories specified via `--include-dir` CLI flag

### Examples

```
# Import from same directory
import "anonymization-rules.dsl"

# Import from subdirectory
import "magento/base.dsl"

# Import shared configuration (resolved via include path)
import "common/gdpr-anonymization.dsl"
```

### Composition Example

**magento-base.dsl:**
```
faker names ["John", "Jane", "Bob", "Alice"]
faker emails ["user1@example.test", "user2@example.test"]

anonymize customer_entity {
    email     -> emails
    firstname -> names
    lastname  -> names
}

ignore report_event
ignore sales_bestsellers_aggregated_daily
```

**shop-specific.dsl:**
```
import "magento-base.dsl"

var base_url: string
var order_limit: int = 100

aggregate orders {
    root sales_order
    limit $order_limit
}
```

---

## Faker Data Sources

Define lists of fake data to use for anonymization.

### Syntax

```
faker <name> [<value1>, <value2>, ...]
```

### Examples

```
faker names ["John", "Jane", "Bob", "Alice", "Charlie", "Diana"]
faker emails ["user1@example.test", "user2@example.test", "user3@example.test"]
faker phones ["+31600000001", "+31600000002", "+31600000003"]
faker streets ["123 Main St", "456 Oak Ave", "789 Pine Rd"]
faker cities ["Amsterdam", "Rotterdam", "Utrecht", "Den Haag"]
faker companies ["Acme Corp", "Globex Inc", "Initech", "Umbrella Co"]
```

### Behavior

- Values are selected deterministically based on a hash of the original value
- The same input always produces the same fake output (for referential consistency)
- The list wraps around if there are more unique values than faker entries

---

## Relations

Define soft foreign key relationships that are not declared in the database schema.

### Syntax

```
relation <table>.<column> -> <referenced_table>.<referenced_column>
```

### Examples

```
# Customer group reference (not a formal FK in Magento)
relation customer_entity.group_id -> customer_group.customer_group_id

# Order to customer reference
relation sales_order.customer_id -> customer_entity.entity_id

# Product to category (through junction table)
relation catalog_category_product.product_id -> catalog_product_entity.entity_id
relation catalog_category_product.category_id -> catalog_category_entity.entity_id
```

### Behavior

- Relations are used in addition to auto-discovered foreign keys from the schema
- When importing an aggregate, these relations are followed to import related data
- Direction is from child table to parent table (the arrow points to the referenced/parent table)

---

## Anonymization

Define rules for replacing sensitive data with fake values.

### Syntax

```
anonymize <table> {
    <column> -> <faker_name>
    <column> -> null
}
```

### Examples

```
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
    company     -> companies
    street      -> streets
    city        -> cities
    telephone   -> phones
    fax         -> null
}

anonymize newsletter_subscriber {
    subscriber_email -> emails
}
```

### Special Values

| Value | Behavior |
|-------|----------|
| `<faker_name>` | Replace with value from faker list |
| `null` | Set column to NULL |

---

## Table Handling

Control how tables are handled during import.

### Exclude

Import table structure but no data. Creates an empty table locally.

```
exclude <table>
```

**Examples:**
```
exclude sales_order_payment
exclude customer_log
exclude persistent_session
```

### Ignore

Completely ignore the table. Don't create structure, don't import data, don't touch existing local table.

```
ignore <table>
```

**Examples:**
```
ignore report_event
ignore report_viewed_product_index
ignore sales_bestsellers_aggregated_daily
ignore sales_bestsellers_aggregated_monthly
ignore sales_bestsellers_aggregated_yearly
```

### Use Cases

| Scenario | Use |
|----------|-----|
| Sensitive data (payments, tokens) | `exclude` - keep structure for app compatibility |
| Report/analytics tables | `ignore` - not needed locally, may be huge |
| Cache tables | `exclude` - keep structure, data regenerates |
| Log tables | `exclude` or `ignore` depending on need |

---

## Aggregates

Define a root entity and automatically import all related data following foreign key relationships (both schema-defined and soft relations).

### Syntax

```
aggregate <name> {
    root <table>
    where "<sql_condition>"
    order by <column> [asc|desc]
    limit <number>
}
```

### Clauses

| Clause | Required | Description |
|--------|----------|-------------|
| `root` | Yes | The main table (aggregate root) |
| `where` | No | SQL WHERE condition to filter rows |
| `order by` | No | Sort order for limit selection |
| `limit` | No | Maximum rows to import (can be variable) |

### Examples

**Import recent orders:**
```
aggregate orders {
    root sales_order
    where "created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)"
    order by created_at desc
    limit 100
}
```

**Import products from specific category:**
```
aggregate products {
    root catalog_product_entity
    where "entity_id IN (
        SELECT product_id
        FROM catalog_category_product
        WHERE category_id = 42
    )"
}
```

**Import active customers only:**
```
aggregate customers {
    root customer_entity
    where "is_active = 1"
    limit 500
}
```

**Import with variable limit:**
```
var order_limit: int = 100

aggregate orders {
    root sales_order
    order by created_at desc
    limit $order_limit
}
```

### Behavior

1. Select rows from root table matching the filter criteria
2. Auto-discover foreign key relationships from schema
3. Include soft relations defined via `relation`
4. Recursively import all child entities that reference the selected root rows
5. Import all parent entities referenced by the selected rows
6. Apply anonymization rules during import

---

## Incremental Imports

Add specific rows to an already-defined aggregate without reimporting everything.

### Syntax

```
include <aggregate_name> where "<sql_condition>"
```

### Examples

```
# First, define the aggregate
aggregate products {
    root catalog_product_entity
    where "entity_id IN (
        SELECT product_id FROM catalog_category_product WHERE category_id = 42
    )"
}

# Later, add specific products
include products where "sku = 'HERO-PRODUCT'"
include products where "sku IN ('PROMO-2024-A', 'PROMO-2024-B')"
include products where "entity_id = 12345"
```

```
aggregate orders {
    root sales_order
    limit 100
}

# Add a specific order that a developer needs
include orders where "increment_id = '100000999'"
```

### Behavior

1. The `where` clause applies to the aggregate's root table
2. Foreign key relationships are followed just like with the main aggregate
3. If the data already exists locally, it is updated
4. Anonymization rules apply to incremental imports

---

## Preserve

Keep local values for specific rows; don't overwrite during import.

### Syntax

```
preserve <table> where "<sql_condition>"
```

### Examples

```
# Keep local development settings
preserve core_config_data where "path LIKE 'dev/%'"

# Keep local URLs
preserve core_config_data where "path IN (
    'web/secure/base_url',
    'web/unsecure/base_url',
    'web/secure/base_link_url',
    'web/unsecure/base_link_url'
)"

# Keep local admin settings
preserve admin_user where "username = 'localadmin'"

# Keep local API keys
preserve core_config_data where "path LIKE 'payment/%/api_key'"
```

### Behavior

1. Row is imported from remote
2. After import, local value is restored
3. Effectively: row exists in sync but keeps local data

---

## Set (Upsert)

Set specific values after import. If the row doesn't exist, it is created.

### Syntax

```
set <table> {
    match <column> = <value>, <column> = <value>, ...
    <column> = <value>
    <column> = <value>
}
```

### Clauses

| Clause | Description |
|--------|-------------|
| `match` | Columns used to find existing row or create new one |
| Other assignments | Columns to set/update |

### Examples

**Set base URL:**
```
set core_config_data {
    match path = "web/secure/base_url", scope = "default", scope_id = 0
    value = $base_url
}
```

**Set multiple config values:**
```
set core_config_data {
    match path = "trans_email/ident_general/email", scope = "default", scope_id = 0
    value = $admin_email
}

set core_config_data {
    match path = "trans_email/ident_general/name", scope = "default", scope_id = 0
    value = "Local Admin"
}

set core_config_data {
    match path = "dev/debug/enabled", scope = "default", scope_id = 0
    value = "1"
}
```

**Using variables:**
```
var base_url: string
var admin_email: string = "admin@local.test"

set core_config_data {
    match path = "web/secure/base_url", scope = "default", scope_id = 0
    value = $base_url
}

set core_config_data {
    match path = "contact/email/recipient_email", scope = "default", scope_id = 0
    value = $admin_email
}
```

### Behavior

1. Look for row matching all `match` column values
2. If found: update the other columns
3. If not found: insert new row with match columns + other columns

---

## After Transformations

Execute arbitrary SQL after import completes.

### Syntax

```
after {
    """
    <sql_statement>
    """
    """
    <sql_statement>
    """
}
```

### Examples

**Move orders to future (for testing date-sensitive logic):**
```
after {
    """
    UPDATE sales_order
    SET created_at = DATE_ADD(created_at, INTERVAL 10 YEAR),
        updated_at = DATE_ADD(updated_at, INTERVAL 10 YEAR)
    """

    """
    UPDATE sales_order_grid
    SET created_at = DATE_ADD(created_at, INTERVAL 10 YEAR)
    """
}
```

**Enable all products locally:**
```
after {
    """
    UPDATE catalog_product_entity_int
    SET value = 1
    WHERE attribute_id = (
        SELECT attribute_id FROM eav_attribute
        WHERE attribute_code = 'status' AND entity_type_id = 4
    )
    """
}
```

**Clear sensitive data missed by anonymization:**
```
after {
    """
    UPDATE customer_entity SET password_hash = NULL, rp_token = NULL
    """

    """
    TRUNCATE table customer_log
    """
}
```

### Behavior

- Statements execute in order after all imports complete
- Each statement is executed as a separate query
- Errors stop execution (transaction rollback behavior TBD)

---

## Conditionals

Apply configuration conditionally based on variable values using Rust-like attributes.

### Syntax

```
#[when(<expression>)]
<statement>
```

### Supported Operators

| Type | Operators |
|------|-----------|
| Comparison | `==`, `!=`, `>`, `<`, `>=`, `<=` |
| Logical | `&&`, `||`, `!` |
| Grouping | `(`, `)` |

### Examples

**Simple boolean:**
```
var skip_payments: bool = true

#[when($skip_payments)]
exclude sales_order_payment
```

**Comparison:**
```
var order_limit: int = 100

#[when($order_limit > 50)]
aggregate large_orders {
    root sales_order
    order by created_at desc
    limit $order_limit
}
```

**Equality:**
```
var env: string = "staging"

#[when($env == "staging")]
set core_config_data {
    match path = "dev/debug/enabled", scope = "default", scope_id = 0
    value = "1"
}
```

**Negation:**
```
#[when(!$skip_payments)]
aggregate payments {
    root sales_order_payment
}
```

**Complex expressions:**
```
var debug_mode: bool = true
var env: string = "development"

#[when($debug_mode && $env != "production")]
after {
    """
    UPDATE core_config_data SET value = '1' WHERE path = 'dev/log/active'
    """
}

#[when($env == "staging" || $env == "development")]
exclude sales_order_payment
```

**Grouped expressions:**
```
#[when(($env == "staging" || $env == "development") && $debug_mode)]
set core_config_data {
    match path = "dev/template/allow_symlink", scope = "default", scope_id = 0
    value = "1"
}
```

### Applicable Statements

Conditionals can be applied to:
- `exclude`
- `ignore`
- `aggregate`
- `include`
- `preserve`
- `set`
- `after`
- `anonymize`

---

## Comments

Single-line comments start with `#`.

### Syntax

```
# This is a comment
```

### Examples

```
# === VARIABLES ===
var base_url: string  # Required: no default

# Import base configuration
import "magento-base.dsl"

# Skip payment data in non-production environments
#[when($env != "production")]
exclude sales_order_payment

aggregate orders {
    root sales_order
    # Only import recent orders to keep database small
    where "created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)"
    limit 100  # Adjust as needed
}
```

---

## CLI Interface

### Basic Usage

```bash
mysqlimport --config <config.dsl> [options]
```

### Options

| Option | Description |
|--------|-------------|
| `--config <file>` | Path to DSL configuration file |
| `--var-file <file>` | Load variables from file |
| `--var <name>=<value>` | Set variable value (overrides var-file) |
| `--include-dir <dir>` | Add directory to import search path |
| `--dry-run` | Show what would be imported without executing |
| `--verbose` | Show detailed progress |

### Examples

```bash
# Basic usage
mysqlimport --config shop.dsl

# With variable file
mysqlimport --config shop.dsl --var-file production.vars

# Override specific variables
mysqlimport --config shop.dsl --var-file production.vars --var base_url="https://staging.dev/"

# Multiple variable overrides
mysqlimport --config shop.dsl \
    --var base_url="https://local.dev/" \
    --var admin_email="me@local.test" \
    --var order_limit=50

# With include directories
mysqlimport --config shop.dsl \
    --include-dir /etc/mysqlimport/common \
    --include-dir ./shared

# Dry run
mysqlimport --config shop.dsl --dry-run --verbose
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `MYSQLIMPORT_INCLUDE_PATH` | Colon-separated list of directories for import resolution |

```bash
export MYSQLIMPORT_INCLUDE_PATH="/etc/mysqlimport/common:/home/user/.mysqlimport"
mysqlimport --config shop.dsl
```

---

## Variable Files

### Simple Format (.vars)

```
base_domain = "local.dev"
base_port = 8080
admin_email = "admin@local.test"
skip_payments = true
order_limit = 100
tax_rate = 0.21
```

**Rules:**
- One variable per line
- Format: `name = value`
- Strings must be quoted
- Booleans: `true` or `false`
- Numbers: integer or floating point
- Comments with `#`

**Example with comments:**
```
# Local development settings
base_domain = "local.dev"
base_port = 8080
admin_email = "admin@local.test"

# Import settings
skip_payments = true    # Don't import payment data
order_limit = 100       # Only import 100 most recent orders
```

### JSON Format (.vars.json)

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

1. Default values in DSL (`var x: int = 100`)
2. Variable file (`--var-file`)
3. CLI overrides (`--var x=200`)

Later sources override earlier ones.

---

## Complete Example

**magento-shop.dsl:**
```
# Base configuration for Magento shop import

import "magento-anonymization.dsl"

# === VARIABLES ===
var base_domain: string
var base_port: int = 80
var admin_email: string = "admin@local.test"
var env: string = "development"
var skip_payments: bool = true
var skip_logs: bool = true
var order_limit: int = 100
var product_category: int = 42

# === SOFT RELATIONS ===
relation customer_entity.group_id -> customer_group.customer_group_id
relation sales_order.customer_id -> customer_entity.entity_id

# === TABLE HANDLING ===
# Always ignore reporting tables
ignore report_event
ignore report_viewed_product_index
ignore sales_bestsellers_aggregated_daily
ignore sales_bestsellers_aggregated_monthly
ignore sales_bestsellers_aggregated_yearly

# Conditionally handle payment data
#[when($skip_payments)]
exclude sales_order_payment

#[when($skip_logs)]
exclude customer_log
exclude visitor_log

# === AGGREGATES ===
aggregate orders {
    root sales_order
    where "created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)"
    order by created_at desc
    limit $order_limit
}

aggregate products {
    root catalog_product_entity
    where "entity_id IN (
        SELECT product_id
        FROM catalog_category_product
        WHERE category_id = $product_category
    )"
}

aggregate customers {
    root customer_entity
    where "is_active = 1"
    limit 500
}

# === INCREMENTAL ===
include products where "sku = 'HERO-PRODUCT'"

# === PRESERVE LOCAL VALUES ===
preserve core_config_data where "path LIKE 'dev/%'"
preserve core_config_data where "path LIKE 'web/%base_url%'"

# === SET VALUES (using string interpolation) ===
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

set core_config_data {
    match path = "web/cookie/cookie_domain", scope = "default", scope_id = 0
    value = ".{$base_domain}"
}

# === POST-IMPORT ===
#[when($env == "development")]
after {
    """
    UPDATE sales_order
    SET created_at = DATE_ADD(created_at, INTERVAL 10 YEAR),
        updated_at = DATE_ADD(updated_at, INTERVAL 10 YEAR)
    """
}
```

**magento-anonymization.dsl:**
```
# Shared anonymization rules for Magento

faker names ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank"]
faker emails ["user1@example.test", "user2@example.test", "user3@example.test"]
faker phones ["+31600000001", "+31600000002", "+31600000003"]
faker streets ["123 Main St", "456 Oak Ave", "789 Pine Rd", "321 Elm Blvd"]
faker cities ["Amsterdam", "Rotterdam", "Utrecht", "Den Haag"]
faker companies ["Acme Corp", "Globex Inc", "Initech", "Umbrella Co"]

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
    company     -> companies
    street      -> streets
    city        -> cities
    telephone   -> phones
    fax         -> null
}

anonymize newsletter_subscriber {
    subscriber_email -> emails
}

anonymize sales_order {
    customer_email    -> emails
    customer_firstname -> names
    customer_lastname  -> names
}
```

**production.vars:**
```
base_domain = "local.dev"
base_port = 80
admin_email = "developer@company.com"
env = "development"
skip_payments = true
skip_logs = true
order_limit = 100
product_category = 42
```

**Usage:**
```bash
mysqlimport --config magento-shop.dsl --var-file production.vars
```
