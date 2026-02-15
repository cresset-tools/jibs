#!/bin/bash
# Test for --fail-after-tables and --resume functionality

set -e

cd "$(dirname "$0")/.."

echo "=== Resume Test ==="
echo ""

# Build
echo "Building..."
./scripts/build.sh 2>&1 | tail -3

# Ensure local database is clean and has a preserved row
echo ""
echo "Setting up local database with preserved row..."
docker exec jibs-local mysql -u root -plocal_root_pass -e "
    DROP DATABASE IF EXISTS imported;
    CREATE DATABASE imported;
    USE imported;
    CREATE TABLE users (
        id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        email VARCHAR(255) NOT NULL,
        name VARCHAR(100) NOT NULL,
        password_hash VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO users (email, name, password_hash) VALUES
        ('local-admin@test.local', 'Local Admin', 'local_secret_hash');
"
echo "Preserved row created:"
docker exec jibs-local mysql -u root -plocal_root_pass imported -e "SELECT * FROM users"

# Step 1: Run import with --fail-after-tables 4
# Table order is: order_items, orders, products, users
# We fail after users (table 4) so the backup table exists but restore hasn't happened
echo ""
echo "=== Step 1: Run import with --fail-after-tables 4 ==="
echo "(This should fail after users table, leaving _jibs_preserve_users)"
./target/debug/jibs import test/import-resume-test.jibs \
    --host testuser@localhost \
    --port 2222 \
    --identity test/ssh-keys/id_ed25519 \
    --remote-mysql "mysql://jibs:jibs_pass@mysql-remote:3306/production" \
    --local-mysql "mysql://root:local_root_pass@localhost:3308/imported" \
    --fail-after-tables 4 2>&1 || echo "(Expected failure)"

# Step 2: Check backup tables exist
echo ""
echo "=== Step 2: Check for backup tables ==="
docker exec jibs-local mysql -u root -plocal_root_pass imported -e "SHOW TABLES LIKE '_jibs_preserve_%'"

# Step 3: Try to run without --resume (should fail)
echo ""
echo "=== Step 3: Try without --resume (should error) ==="
./target/debug/jibs import test/import-resume-test.jibs \
    --host testuser@localhost \
    --port 2222 \
    --identity test/ssh-keys/id_ed25519 \
    --remote-mysql "mysql://jibs:jibs_pass@mysql-remote:3306/production" \
    --local-mysql "mysql://root:local_root_pass@localhost:3308/imported" 2>&1 || echo "(Expected error about backup tables)"

# Step 4: Resume the import
echo ""
echo "=== Step 4: Resume with --resume ==="
./target/debug/jibs import test/import-resume-test.jibs \
    --host testuser@localhost \
    --port 2222 \
    --identity test/ssh-keys/id_ed25519 \
    --remote-mysql "mysql://jibs:jibs_pass@mysql-remote:3306/production" \
    --local-mysql "mysql://root:local_root_pass@localhost:3308/imported" \
    --resume

# Step 5: Verify backup tables are gone
echo ""
echo "=== Step 5: Verify backup tables cleaned up ==="
BACKUP_TABLES=$(docker exec jibs-local mysql -u root -plocal_root_pass imported -N -e "SHOW TABLES LIKE '_jibs_preserve_%'" 2>/dev/null || true)
if [ -z "$BACKUP_TABLES" ]; then
    echo "OK: No backup tables remain"
else
    echo "ERROR: Backup tables still exist: $BACKUP_TABLES"
    exit 1
fi

# Step 6: Verify preserved row was restored
echo ""
echo "=== Step 6: Verify preserved row ==="
PRESERVED=$(docker exec jibs-local mysql -u root -plocal_root_pass imported -N -e "SELECT email, password_hash FROM users WHERE email = 'local-admin@test.local'" 2>/dev/null)
echo "Preserved row: $PRESERVED"
if echo "$PRESERVED" | grep -q "local_secret_hash"; then
    echo "OK: Preserved row has original password_hash"
else
    echo "ERROR: Preserved row was not restored correctly"
    exit 1
fi

# Step 7: Verify other data was imported
echo ""
echo "=== Step 7: Verify imported data ==="
docker exec jibs-local mysql -u root -plocal_root_pass imported -e "
    SELECT 'users' as tbl, COUNT(*) as cnt FROM users
    UNION ALL SELECT 'products', COUNT(*) FROM products
    UNION ALL SELECT 'orders', COUNT(*) FROM orders
    UNION ALL SELECT 'order_items', COUNT(*) FROM order_items;
"

echo ""
echo "=== Resume Test PASSED ==="
