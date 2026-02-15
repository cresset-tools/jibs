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

Run the E2E tests with Docker:
```bash
# Start test containers
docker-compose -f test/docker-compose.yml up -d

# Run aggregate test
cargo run -p jibs_client -- import test/import-aggregate.jibs \
  --host testuser@localhost --port 2222 \
  --remote-mysql 'mysql://root:remote_root_pass@mysql-remote:3306/production' \
  --local-mysql 'mysql://root:local_root_pass@127.0.0.1:3308/imported' \
  --identity test/ssh-keys/id_ed25519

# Clear cached server binary (to force re-upload after rebuild)
ssh -p 2222 -i test/ssh-keys/id_ed25519 testuser@localhost "rm -f /tmp/jibs-*"
```

## Project Structure

- `crates/parser/` - DSL parser (lexer, parser, AST)
- `crates/protocol/` - Shared protocol types (bincode messages)
- `crates/server/` - Remote server binary (sync, cross-compiled for Linux)
- `crates/client/` - CLI client (async, native build)
- `vscode-extension/` - VS Code extension for .jibs syntax highlighting
