-- Source OLTP table for the demo (dummy e-commerce orders)
CREATE DATABASE IF NOT EXISTS shop_demo;
USE shop_demo;

CREATE TABLE IF NOT EXISTS orders (
    order_id     BIGINT UNSIGNED PRIMARY KEY,
    customer_id  INT UNSIGNED   NOT NULL,
    city         VARCHAR(40)    NOT NULL,
    category     VARCHAR(40)    NOT NULL,
    amount       DECIMAL(12,2)  NOT NULL,
    status       VARCHAR(20)    NOT NULL,
    is_deleted   TINYINT(1)     NOT NULL DEFAULT 0,          -- soft delete, propagated to the lake
    created_at   DATETIME(3)    NOT NULL,
    updated_at   DATETIME(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    KEY idx_updated_at (updated_at)                           -- incremental pulls depend on this index
) ENGINE=InnoDB;

-- Read-only user for the pipeline
CREATE USER IF NOT EXISTS 'etl_reader'@'%' IDENTIFIED BY 'etl_reader';
GRANT SELECT ON shop_demo.* TO 'etl_reader'@'%';
