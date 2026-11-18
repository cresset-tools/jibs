#!/bin/bash
# `preserve ... match unique` / `match key (...)`: preserved rows are restored by
# their unique key, so a local auto-increment id that the remote uses for a
# different row must not delete that row.

set -e

cd "$(dirname "$0")/.."

echo "=== Preserve match Test ==="

echo "Building..."
./scripts/build.sh 2>&1 | tail -1
cargo build -p jibs 2>&1 | tail -1

local_sql() {
    docker exec jibs-local mysql -u root -plocal_root_pass imported -N -e "$1" 2>/dev/null
}

expect() {
    if [ "$2" = "$3" ]; then
        echo "OK: $1"
    else
        echo "ERROR: $1: expected '$3', got '$2'"
        exit 1
    fi
}

REMOTE_USERS=$(docker exec jibs-remote mysql -u root -premote_root_pass production -N -e "SELECT COUNT(*) FROM users" 2>/dev/null)
REMOTE_ID1=$(docker exec jibs-remote mysql -u root -premote_root_pass production -N -e "SELECT email FROM users WHERE id = 1" 2>/dev/null)
REMOTE_BOB_ID=$(docker exec jibs-remote mysql -u root -premote_root_pass production -N -e "SELECT id FROM users WHERE email = 'bob@example.com'" 2>/dev/null)

for MODE in unique key; do
    echo ""
    echo "=== match $MODE ==="

    # Local ids 1 and 5 are used by other users on the remote.
    docker exec jibs-local mysql -u root -plocal_root_pass -e "
        DROP DATABASE IF EXISTS imported;
        CREATE DATABASE imported;
        USE imported;
        CREATE TABLE users (
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            email VARCHAR(255) NOT NULL UNIQUE,
            name VARCHAR(100) NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );
        INSERT INTO users (id, email, name, password_hash) VALUES
            (1, 'local-admin@test.local', 'Local Admin', 'local_secret_hash'),
            (5, 'bob@example.com', 'Local Bob', 'local_bob_hash');
    " 2>/dev/null

    ./target/debug/jibs import test/import-preserve-match.jibs \
        --host testuser@localhost \
        --port 2222 \
        --identity test/ssh-keys/id_ed25519 \
        --remote-mysql "mysql://jibs:jibs_pass@mysql-remote:3306/production" \
        --local-mysql "mysql://root:local_root_pass@localhost:3308/imported" \
        --var "match_mode=$MODE" \
        --parallel 4 \
        --clean > /dev/null

    expect "remote user with id 1 kept" "$(local_sql "SELECT email FROM users WHERE id = 1")" "$REMOTE_ID1"
    expect "all remote users plus local-admin" "$(local_sql "SELECT COUNT(*) FROM users")" "$((REMOTE_USERS + 1))"
    expect "local-admin restored" "$(local_sql "SELECT password_hash FROM users WHERE email = 'local-admin@test.local'")" "local_secret_hash"
    expect "bob updated in place, keeps the remote id" "$(local_sql "SELECT CONCAT(id, ' ', name) FROM users WHERE email = 'bob@example.com'")" "$REMOTE_BOB_ID Local Bob"
    expect "backup table removed" "$(local_sql "SHOW TABLES LIKE '\\_jibs\\_preserve\\_%'")" ""
done

echo ""
echo "=== Preserve match Test PASSED ==="
