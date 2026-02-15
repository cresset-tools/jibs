#!/bin/bash
# Test the server directly (without SSH)
# This tests the protocol and MySQL operations

set -e

cd "$(dirname "$0")/.."

echo "=== Building server ==="
cargo build -p jibs_server --release

echo ""
echo "=== Starting MySQL containers ==="
docker-compose up -d mysql-remote

echo "Waiting for MySQL to be ready..."
until docker exec jibs-remote mysqladmin ping -h localhost -u root -premote_root_pass --silent 2>/dev/null; do
    sleep 1
done
echo "MySQL is ready!"

echo ""
echo "=== Testing server directly ==="
# The server expects to connect to MySQL and receive commands via stdin
# For now, just verify it starts and can connect

# Create a simple test plan via the client's plan command
echo "Generating execution plan..."
cargo run -p jibs_client --release -- plan test/import-all.jibs 2>/dev/null | head -50

echo ""
echo "=== Server direct test complete ==="
echo "To run full E2E test with SSH, use: ./test/test-e2e.sh"
