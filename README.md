# jibs

jibs copies a production MySQL database into a local development environment. Instead of
dumping 200 GB and waiting, you describe what you need — "a hundred recent orders and
everything they reference" — and jibs walks the foreign keys, anonymizes the PII **on the
production side before anything leaves the host**, and streams the result compressed over
SSH into parallel `LOAD DATA` workers.

```console
$ jibs import shop.jibs --host deploy@prod --parallel 8 \
    --remote-mysql 'mysql://reader:...@localhost/shop' \
    --local-mysql  'mysql://root:...@127.0.0.1/shop_dev'
Import complete: 214 tables, 1,204,551 rows
```

There is no agent to install: the client uploads a ~3 MB static helper binary to the
remote host over the same SSH connection, runs it, and removes nothing but its own
temporary state. Interrupted imports resume where they left off.

## Install

```console
# Linux and macOS
$ curl -LsSf https://bougie.tools/jibs.sh | sh
```

For PHP/Magento projects, there is also a Composer package that fetches the same binary
on first use:

```console
$ composer require --dev cresset/jibs
$ vendor/bin/jibs check shop.jibs
```

Prebuilt client binaries (Linux gnu/musl x86_64, macOS arm64) are attached to every
[GitHub Release](https://github.com/cresset-tools/jibs/releases) and mirrored to cresset
infrastructure. The remote side needs nothing preinstalled — the embedded helper covers
Linux x86_64 and aarch64. There are no Windows binaries: the client drives the system
OpenSSH binary and is untested there.

Building from source needs a Rust toolchain plus `cargo-zigbuild` and `zig` (the client
embeds cross-compiled helper binaries, so a plain `cargo install` produces a client that
cannot deploy):

```console
$ rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
$ ./scripts/build.sh    # builds the helpers, then the client with them embedded
```

Container images are published to GHCR on every release — including an aarch64 Linux
client the tarballs don't ship. The runnable variants bundle the `openssh-client` the
client drives; see [docker/README.md](docker/README.md) for usage:

```console
$ docker run --rm -it --network host -v ~/.ssh:/root/.ssh:ro -v "$PWD":/work -w /work \
    ghcr.io/cresset-tools/jibs:alpine import shop.jibs --host user@prod ...
```

## Configuration

Imports are described in a `.jibs` file. The core concept is the **aggregate**: a root
query plus automatic traversal of foreign keys (schema-declared and hand-declared), so a
subset of rows arrives with everything it references and everything that references it.

```jibs
var order_limit: int = 100
var base_url: string

// Soft relations the schema doesn't declare
relation sales_order.customer_id -> customer_entity.entity_id

// Skip what a dev copy doesn't need
ignore_table /^report_/
exclude_data /_log$/
full customer_group, store

// The selective part: recent orders and their whole object graph
aggregate orders {
    root sales_order
    where "created_at > DATE_SUB(NOW(), INTERVAL 90 DAY)"
    order by created_at desc
    limit $order_limit
}

// PII never leaves production: applied remotely during the transfer
faker emails ["user{unique()}@example.test"]
anonymize customer_entity {
    email         -> emails
    password_hash -> null
}

// Keep local values, fix config for local use
preserve core_config_data where "path LIKE 'dev/%'"
set core_config_data {
    match path = "web/secure/base_url", scope = "default", scope_id = 0
    value = $base_url
}
```

On-demand fetches pull specific production rows into an existing local database without
redoing the import:

```jibs
aggregate products {
    root catalog_product_entity
    where "FALSE"
}

get product_by_sku (sku: string) {
    products where "sku = '{$sku}'"
}
```

```console
$ jibs get shop.jibs --host deploy@prod -- product_by_sku --sku HERO-PRODUCT
```

The full language — variables, interpolation, imports, conditionals — is specified in
[SPEC.md](SPEC.md) and [GRAMMAR.md](GRAMMAR.md); every example in those documents (and
this one) is parsed by the test suite. A syntax-highlighting extension for VS Code lives
in [vscode-extension/](vscode-extension/).

## Commands

| Command | Purpose |
|---------|---------|
| `jibs import <config>` | Run the import (`--dry-run`, `--resume`, `--clean`, `--parallel N`, `--var k=v`) |
| `jibs get <config> -- fn --arg v` | Fetch specific aggregates into the local database |
| `jibs check <config>` | Parse, resolve, and validate a config without connecting |
| `jibs plan <config>` | Print the resolved execution plan as JSON |

Before trusting a tool that drops and recreates local tables, see what it would do:

```console
$ jibs import shop.jibs --host deploy@prod --dry-run
DRY RUN — no changes were made to the local database

Aggregates:
  orders (root sales_order): 3,406 matching root row(s), limited to 100

Aggregate tables — rows selected by traversal (61): ...
```

`--dry-run` connects to the remote, classifies every table the way the import would, and
counts matching root rows — without ever touching the local database. `jibs check`
validates configs fully offline: variable types, faker references, regex patterns, with
source-annotated errors.

Safety properties worth knowing: anonymization runs remotely in every mode (including
`get`); local foreign-key constraints are captured before the import and restored after
it; rows matching `preserve` rules survive; interrupted imports leave a checkpoint that
`--resume` continues from; and a client/server version mismatch is a clear error, not
corrupted data.

## Requirements

- **Local:** an OpenSSH client on `PATH`, and a MySQL server to import into with
  `local_infile` enabled (the loader uses `LOAD DATA LOCAL INFILE`).
- **Remote:** SSH access and MySQL credentials. The helper binary runs on Linux x86_64
  or aarch64; nothing needs to be installed.

## Development

The end-to-end test environment is two MySQL containers and an SSH server:

```console
$ docker compose up -d
$ cargo test --workspace   # runs in seconds
$ ./scripts/build.sh       # full cross-compile + client build
```

E2E invocations against the containers are documented in [CLAUDE.md](CLAUDE.md).

## License

[EUPL-1.2](LICENSE)
