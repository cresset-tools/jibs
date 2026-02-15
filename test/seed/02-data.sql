-- Seed data for testing

USE production;

-- Insert users
INSERT INTO users (email, name, password_hash) VALUES
('alice@example.com', 'Alice Johnson', '$2b$10$abcdefghijklmnopqrstuv'),
('bob@example.com', 'Bob Smith', '$2b$10$abcdefghijklmnopqrstuv'),
('carol@example.com', 'Carol Williams', '$2b$10$abcdefghijklmnopqrstuv'),
('dave@example.com', 'Dave Brown', '$2b$10$abcdefghijklmnopqrstuv'),
('eve@example.com', 'Eve Davis', '$2b$10$abcdefghijklmnopqrstuv');

-- Insert products
INSERT INTO products (sku, name, description, price, stock) VALUES
('LAPTOP-001', 'Pro Laptop 15"', 'High-performance laptop with 16GB RAM', 1299.99, 50),
('PHONE-001', 'SmartPhone X', 'Latest smartphone with 5G', 899.99, 100),
('TABLET-001', 'Tab Pro 10"', '10-inch tablet with stylus support', 599.99, 75),
('HEADPHONES-001', 'Wireless Headphones', 'Noise-cancelling wireless headphones', 249.99, 200),
('CHARGER-001', 'USB-C Charger', '65W USB-C fast charger', 49.99, 500),
('CASE-001', 'Laptop Sleeve', 'Protective laptop sleeve', 29.99, 300),
('MOUSE-001', 'Wireless Mouse', 'Ergonomic wireless mouse', 79.99, 150),
('KEYBOARD-001', 'Mechanical Keyboard', 'RGB mechanical keyboard', 149.99, 100);

-- Insert orders for Alice (user_id = 1)
INSERT INTO orders (user_id, status, total, shipping_address) VALUES
(1, 'delivered', 1349.98, '123 Main St, New York, NY 10001'),
(1, 'shipped', 899.99, '123 Main St, New York, NY 10001');

-- Insert orders for Bob (user_id = 2)
INSERT INTO orders (user_id, status, total, shipping_address) VALUES
(2, 'paid', 329.98, '456 Oak Ave, Los Angeles, CA 90001'),
(2, 'pending', 1299.99, '456 Oak Ave, Los Angeles, CA 90001');

-- Insert orders for Carol (user_id = 3)
INSERT INTO orders (user_id, status, total, shipping_address) VALUES
(3, 'delivered', 599.99, '789 Pine Rd, Chicago, IL 60601');

-- Order items for order 1 (Alice's delivered order)
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(1, 1, 1, 1299.99),  -- Laptop
(1, 5, 1, 49.99);     -- Charger

-- Order items for order 2 (Alice's shipped order)
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(2, 2, 1, 899.99);    -- Phone

-- Order items for order 3 (Bob's paid order)
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(3, 4, 1, 249.99),    -- Headphones
(3, 7, 1, 79.99);     -- Mouse

-- Order items for order 4 (Bob's pending order)
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(4, 1, 1, 1299.99);   -- Laptop

-- Order items for order 5 (Carol's delivered order)
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
(5, 3, 1, 599.99);    -- Tablet

-- Insert some audit log entries (should be excluded)
INSERT INTO audit_log (table_name, record_id, action, new_values) VALUES
('users', 1, 'insert', '{"email": "alice@example.com"}'),
('orders', 1, 'insert', '{"user_id": 1, "total": 1349.98}'),
('orders', 1, 'update', '{"status": "delivered"}');

-- Insert some sessions (should be ignored)
INSERT INTO sessions (id, user_id, data, expires_at) VALUES
('sess_abc123', 1, 'session data blob', DATE_ADD(NOW(), INTERVAL 1 DAY)),
('sess_def456', 2, 'session data blob', DATE_ADD(NOW(), INTERVAL 1 DAY));
