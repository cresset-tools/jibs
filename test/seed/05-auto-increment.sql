-- An AUTO_INCREMENT counter well past max(id), as after deleted rows: imports
-- must keep the source's counter, not reset it to max(id)+1
-- (https://github.com/cresset-tools/jibs/issues/32).

USE production;

ALTER TABLE products AUTO_INCREMENT = 1000;
