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
# Optional: clear cached server binaries to reclaim space. Not needed for
# correctness — binaries are content-addressed (a rebuilt server uploads under
# a new hash) and the protocol handshake rejects version mismatches loudly.
ssh -p 2222 -i test/ssh-keys/id_ed25519 -o StrictHostKeyChecking=no testuser@localhost "rm -f /tmp/jibs-*"

# Basic aggregate test (with parallel loading)
cargo run -p jibs -- import test/import-aggregate.jibs \
  --host testuser@localhost --port 2222 \
  --remote-mysql 'mysql://root:remote_root_pass@mysql-remote:3306/production' \
  --local-mysql 'mysql://root:local_root_pass@127.0.0.1:3308/imported' \
  --identity test/ssh-keys/id_ed25519 \
  --parallel 4 \
  --clean

# Full table import test
cargo run -p jibs -- import test/import-all.jibs \
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
cargo run -p jibs --features test-utils -- import test/import-resume-test.jibs \
  --host testuser@localhost --port 2222 \
  --remote-mysql 'mysql://root:remote_root_pass@mysql-remote:3306/production' \
  --local-mysql 'mysql://root:local_root_pass@127.0.0.1:3308/imported' \
  --identity test/ssh-keys/id_ed25519 \
  --parallel 4 \
  --clean \
  --fail-after-tables 2

# Resume the failed import
cargo run -p jibs -- import test/import-resume-test.jibs \
  --host testuser@localhost --port 2222 \
  --remote-mysql 'mysql://root:remote_root_pass@mysql-remote:3306/production' \
  --local-mysql 'mysql://root:local_root_pass@127.0.0.1:3308/imported' \
  --identity test/ssh-keys/id_ed25519 \
  --parallel 4 \
  --resume
```

### Test Dump Export / Load
`import --dump-to <file>` writes the (already anonymized, aggregate-resolved)
stream to a versioned, zstd-compressed `.jibsdump` file instead of a local DB.
`jibs load <file>` replays it into a local DB using the parallel loader pool
(`--parallel` defaults to 4), reproducing a live import: `preserve` backups,
`set` upserts and `after` statements all run. No SSH is needed to load. Loading
is atomic-ish: an incomplete/truncated dump (missing its `End` terminator)
fails loudly instead of loading partial data. `--clean` discards leftover state
from a previous interrupted import; `--max-message-size` matches a dump exported
with a raised wire cap.

```bash
# Export to a .jibsdump file (connects to the remote like a normal import)
cargo run -p jibs -- import test/import-all.jibs \
  --host testuser@localhost --port 2222 \
  --remote-mysql 'mysql://root:remote_root_pass@mysql-remote:3306/production' \
  --identity test/ssh-keys/id_ed25519 \
  --parallel 4 \
  --dump-to /tmp/all.jibsdump

# Load the dump into local MySQL (parallelized)
cargo run -p jibs -- load /tmp/all.jibsdump \
  --local-mysql 'mysql://root:local_root_pass@127.0.0.1:3308/imported' \
  --parallel 4
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
