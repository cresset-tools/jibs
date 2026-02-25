# MySQL Import DSL - VS Code Extension

Syntax highlighting for MySQL Import DSL files (`.dsl`, `.mysqlimport`).

## Features

- Syntax highlighting for all DSL keywords
- String interpolation highlighting (`{$variable}`)
- Variable highlighting (`$name`)
- Comment highlighting (`# comment`)
- Conditional attribute highlighting (`#[when(...)]`)
- Multiline SQL string highlighting (`"""..."""`)

## Installation

### From Source

1. Copy the `vscode-extension` folder to your VS Code extensions directory:
   - **macOS**: `~/.vscode/extensions/mysqlimport-dsl`
   - **Linux**: `~/.vscode/extensions/mysqlimport-dsl`
   - **Windows**: `%USERPROFILE%\.vscode\extensions\mysqlimport-dsl`

2. Restart VS Code

### Development

1. Open the `vscode-extension` folder in VS Code
2. Press `F5` to open a new Extension Development Host window
3. Open a `.dsl` file to see syntax highlighting

## Supported Syntax

```
# Variables
var base_domain: string
var order_limit: int = 100

# Faker data sources
faker names ["John", "Jane", "Bob"]

# Relations
relation customer.group_id -> customer_group.id

# Anonymization
anonymize customer {
    email -> emails
    password -> null
}

# Table handling
exclude payments
ignore logs

# Aggregates
aggregate orders {
    root sales_order
    where "created_at > NOW() - INTERVAL 90 DAY"
    order by created_at desc
    limit $order_limit
}

# Get functions (parameterized queries for CLI)
get orders_for_user(user_id: int) {
    orders where "user_id = {$user_id}"
}

# Set values with interpolation
set config {
    match path = "web/url"
    value = "https://{$domain}:{$port}/"
}

# Conditional statements
#[when($env == "development")]
after {
    """
    UPDATE orders SET status = 'test'
    """
}
```

## License

MIT
