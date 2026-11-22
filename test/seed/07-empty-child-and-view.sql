-- A table an aggregate reaches without finding a row in it, and a view.
--
-- order_notes references orders and is empty: the aggregate import must still
-- create it locally (regression: the schema was only sent with a table's
-- first row, so such a table was missing after the import and every foreign
-- key into it failed to be restored).
--
-- order_totals is a view: SHOW CREATE TABLE answers four columns for one, not
-- two (regression: reading its table options panicked a server worker).

USE production;

CREATE TABLE order_notes (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    order_id INT UNSIGNED NOT NULL,
    note VARCHAR(255),
    FOREIGN KEY (order_id) REFERENCES orders(id)
) ENGINE=InnoDB;

CREATE VIEW order_totals AS
    SELECT user_id, COUNT(*) AS orders, SUM(total) AS total FROM orders GROUP BY user_id;
