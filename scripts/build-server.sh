#!/bin/bash
# Build the server binary for Linux targets
#
# Prerequisites:
#   brew install zig
#   cargo install cargo-zigbuild
#   rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
#
# Usage:
#   ./scripts/build-server.sh          # Build for x86_64
#   ./scripts/build-server.sh aarch64  # Build for ARM64
#   ./scripts/build-server.sh all      # Build both

set -e

TARGETS=""

case "${1:-x86_64}" in
    x86_64)
        TARGETS="x86_64-unknown-linux-musl"
        ;;
    aarch64|arm64)
        TARGETS="aarch64-unknown-linux-musl"
        ;;
    all)
        TARGETS="x86_64-unknown-linux-musl aarch64-unknown-linux-musl"
        ;;
    *)
        echo "Usage: $0 [x86_64|aarch64|all]"
        exit 1
        ;;
esac

for TARGET in $TARGETS; do
    echo "Building jibs-server for $TARGET..."
    cargo zigbuild -p jibs_server --release --target "$TARGET"

    BINARY="target/$TARGET/release/jibs-server"
    SIZE=$(ls -lh "$BINARY" | awk '{print $5}')
    echo "Built: $BINARY ($SIZE)"
done

echo ""
echo "Now rebuild the client to embed the server binary:"
echo "  cargo build -p jibs_client --release"
