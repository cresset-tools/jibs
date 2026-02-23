#!/bin/bash
# Build script for jibs
# Builds both server binary (cross-compiled for Linux) and client

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Build server for Linux (cross-compiled)
echo "Building server for aarch64-unknown-linux-musl..."
cargo zigbuild -p jibs_server --release --target aarch64-unknown-linux-musl

echo "Building server for x86_64-unknown-linux-musl..."
cargo zigbuild -p jibs_server --release --target x86_64-unknown-linux-musl

# Build client (native)
echo "Building client..."
cargo build --release -p jibs_client

echo "Build complete!"
echo "Server binaries:"
echo "  - target/aarch64-unknown-linux-musl/release/jibs-server"
echo "  - target/x86_64-unknown-linux-musl/release/jibs-server"
echo "Client binary:"
echo "  - target/release/jibs"
