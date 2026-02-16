# Claude Memory for Jibs Project

## Build Instructions

**Always use the build script when making changes to the server:**

```bash
./scripts/build.sh
```

This script:
1. Cross-compiles the server for Linux (aarch64 and x86_64 musl targets)
2. Builds the client natively

**Important:** The server binary is uploaded to remote hosts via SSH. If you only build the client, changes to server code won't be reflected. Always run the full build script.

## Testing

### Start Test Environment
```bash
# Start test containers (MySQL remote, MySQL local, SSH server)
docker-compose up -d

# Check containers are healthy
docker-compose ps
```

### Run E2E Tests
```bash
# Clear cached server binary (required after rebuilding server)
ssh -p 2222 -i test/ssh-keys/id_ed25519 -o StrictHostKeyChecking=no testuser@localhost "rm -f /tmp/jibs-*"

# Basic aggregate test (with parallel loading)
cargo run -p jibs_client -- import test/import-aggregate.jibs \
  --host testuser@localhost --port 2222 \
  --remote-mysql 'mysql://root:remote_root_pass@mysql-remote:3306/production' \
  --local-mysql 'mysql://root:local_root_pass@127.0.0.1:3308/imported' \
  --identity test/ssh-keys/id_ed25519 \
  --parallel 4 \
  --clean

# Full table import test
cargo run -p jibs_client -- import test/import-all.jibs \
  --host testuser@localhost --port 2222 \
  --remote-mysql 'mysql://root:remote_root_pass@mysql-remote:3306/production' \
  --local-mysql 'mysql://root:local_root_pass@127.0.0.1:3308/imported' \
  --identity test/ssh-keys/id_ed25519 \
  --parallel 4 \
  --clean
```

### Test Resume Functionality
```bash
# Run with --fail-after-tables to simulate crash (requires test-utils feature)
cargo run -p jibs_client --features test-utils -- import test/import-resume-test.jibs \
  --host testuser@localhost --port 2222 \
  --remote-mysql 'mysql://root:remote_root_pass@mysql-remote:3306/production' \
  --local-mysql 'mysql://root:local_root_pass@127.0.0.1:3308/imported' \
  --identity test/ssh-keys/id_ed25519 \
  --parallel 4 \
  --clean \
  --fail-after-tables 2

# Resume the failed import
cargo run -p jibs_client -- import test/import-resume-test.jibs \
  --host testuser@localhost --port 2222 \
  --remote-mysql 'mysql://root:remote_root_pass@mysql-remote:3306/production' \
  --local-mysql 'mysql://root:local_root_pass@127.0.0.1:3308/imported' \
  --identity test/ssh-keys/id_ed25519 \
  --parallel 4 \
  --resume
```

### Available Test Files
- `test/import-aggregate.jibs` - Tests aggregate with relations (orders for user)
- `test/import-all.jibs` - Full table import (no aggregates)
- `test/import-resume-test.jibs` - For testing resume functionality
- `test/import-overlapping-aggregates.jibs` - Multiple aggregates with overlap
- `test/import-different-roots.jibs` - Aggregates from different root tables

### Cleanup
```bash
# Stop containers
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

## Project Structure

- `crates/parser/` - DSL parser (lexer, parser, AST)
- `crates/protocol/` - Shared protocol types (bincode messages)
- `crates/server/` - Remote server binary (sync, cross-compiled for Linux)
- `crates/client/` - CLI client (async, native build)
- `vscode-extension/` - VS Code extension for .jibs syntax highlighting
