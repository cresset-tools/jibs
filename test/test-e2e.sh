#!/bin/bash
# End-to-end test for jibs importer
# Tests the full flow: SSH connection -> server deployment -> data transfer -> local MySQL load

set -e

cd "$(dirname "$0")/.."

echo "=== Jibs E2E Test ==="
echo ""

# Step 1: Setup SSH keys
echo "Step 1: Setting up SSH keys..."
./test/setup-ssh-keys.sh
chmod 600 test/ssh-keys/id_ed25519

# Step 2: Build binaries
echo ""
echo "Step 2: Building binaries..."
./scripts/build-server.sh x86_64
cargo build -p jibs_client --release

# Step 3: Start docker containers
echo ""
echo "Step 3: Starting docker containers..."
docker-compose down -v 2>/dev/null || true
docker-compose up -d

echo "Waiting for services to be ready..."
sleep 5

# Wait for MySQL remote
echo "  Waiting for mysql-remote..."
until docker exec jibs-remote mysqladmin ping -h localhost -u root -premote_root_pass --silent 2>/dev/null; do
    sleep 1
done

# Wait for MySQL local
echo "  Waiting for mysql-local..."
until docker exec jibs-local mysqladmin ping -h localhost -u root -plocal_root_pass --silent 2>/dev/null; do
    sleep 1
done

# Wait for SSH server
echo "  Waiting for ssh-server..."
until nc -z localhost 2222 2>/dev/null; do
    sleep 1
done
sleep 2  # Give SSH a moment to fully initialize

echo "All services ready!"

# Step 4: Verify remote database has data
echo ""
echo "Step 4: Verifying remote database..."
docker exec jibs-remote mysql -u jibs -pjibs_pass production -e "
    SELECT 'users' as tbl, COUNT(*) as cnt FROM users
    UNION ALL SELECT 'products', COUNT(*) FROM products
    UNION ALL SELECT 'orders', COUNT(*) FROM orders
    UNION ALL SELECT 'order_items', COUNT(*) FROM order_items;
"

# Step 5: Test SSH connection
echo ""
echo "Step 5: Testing SSH connection..."
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    -i test/ssh-keys/id_ed25519 -p 2222 testuser@localhost \
    "echo 'SSH connection successful!'; uname -a"

# Step 6: Run the import
echo ""
echo "Step 6: Running jibs import..."
./target/release/jibs import test/import-docker.jibs \
    --host testuser@localhost \
    --port 2222 \
    --identity test/ssh-keys/id_ed25519 \
    --remote-mysql "mysql://jibs:jibs_pass@mysql-remote:3306/production" \
    --local-mysql "mysql://root:local_root_pass@localhost:3308/imported"

# Step 7: Verify imported data
echo ""
echo "Step 7: Verifying imported data..."
docker exec jibs-local mysql -u root -plocal_root_pass imported -e "
    SELECT 'users' as tbl, COUNT(*) as cnt FROM users
    UNION ALL SELECT 'products', COUNT(*) FROM products
    UNION ALL SELECT 'orders', COUNT(*) FROM orders
    UNION ALL SELECT 'order_items', COUNT(*) FROM order_items;
"

# Step 8: Verify anonymization worked
echo ""
echo "Step 8: Verifying anonymization..."
docker exec jibs-local mysql -u root -plocal_root_pass imported -e "
    SELECT id, email, name,
           CASE WHEN password_hash IS NULL THEN 'NULL'
                WHEN password_hash LIKE '\$2b\$10\$test%' THEN 'RESET'
                ELSE 'ORIGINAL' END as password_status
    FROM users;
"

# Step 9: Verify excluded/ignored tables
echo ""
echo "Step 9: Verifying excluded/ignored tables..."
echo "audit_log should exist but be empty:"
docker exec jibs-local mysql -u root -plocal_root_pass imported -e "SELECT COUNT(*) as audit_log_rows FROM audit_log;" 2>/dev/null || echo "  (table doesn't exist - that's also fine)"
echo "sessions should not exist:"
docker exec jibs-local mysql -u root -plocal_root_pass imported -e "SELECT COUNT(*) FROM sessions;" 2>/dev/null && echo "  ERROR: sessions table exists!" || echo "  OK: sessions table does not exist"

echo ""
echo "=== E2E Test Complete ==="
echo ""
echo "To clean up: docker-compose down -v"
