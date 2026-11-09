-- Triggers, which jibs recreates from the source
-- (https://github.com/cresset-tools/jibs/issues/32).

USE production;

-- A Magento-style mview changelog table: triggers on products write into it.
-- Import it empty (`exclude_data products_cl`); ignoring it makes jibs refuse
-- the import, since every write to products would then fail locally.
CREATE TABLE products_cl (
    version_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    entity_id INT UNSIGNED NOT NULL
) ENGINE=InnoDB;

DELIMITER $$

-- A definer that only exists on the source: jibs must recreate without it.
CREATE DEFINER = 'prod_admin'@'10.0.0.%' TRIGGER trg_products_after_insert
AFTER INSERT ON products FOR EACH ROW
BEGIN
    INSERT IGNORE INTO `products_cl` (`entity_id`) VALUES (NEW.`id`);
END$$

-- Two triggers for the same event: the second must still fire second.
CREATE TRIGGER trg_products_after_update
AFTER UPDATE ON products FOR EACH ROW
    INSERT INTO products_cl (entity_id) VALUES (NEW.id)$$

CREATE TRIGGER trg_products_after_update_2
AFTER UPDATE ON products FOR EACH ROW FOLLOWS trg_products_after_update
    INSERT INTO products_cl (entity_id) VALUES (NEW.id + 100000)$$

-- A trigger on an ignored table (sessions): jibs must leave it alone.
CREATE TRIGGER trg_sessions_before_insert
BEFORE INSERT ON sessions FOR EACH ROW
    SET NEW.data = NEW.data$$

DELIMITER ;

-- A body that only parses under ANSI_QUOTES: jibs must recreate it under the
-- sql_mode it was created with.
SET SESSION sql_mode = 'ANSI_QUOTES';
CREATE TRIGGER trg_products_after_delete
AFTER DELETE ON products FOR EACH ROW
    INSERT INTO "products_cl" ("entity_id") VALUES (OLD."id");
SET SESSION sql_mode = DEFAULT;
