-- ─────────────────────────────────────────────────────────────────────────
-- Source schema + seed data for the CDC demo (MySQL → PostgreSQL).
--
-- Applied to the EXISTING mysql-local container by scripts/seed-dbs.sh.
-- After this runs, Debezium snapshots these tables and then streams every
-- INSERT / UPDATE / DELETE you make to PostgreSQL in real time.
--
-- Re-runnable: drops and recreates the `ecommerce` database each time.
-- ─────────────────────────────────────────────────────────────────────────

DROP DATABASE IF EXISTS ecommerce;
CREATE DATABASE ecommerce;
USE ecommerce;

-- ── Tables ────────────────────────────────────────────────────────────────
-- Every table has a PRIMARY KEY — the Debezium JDBC sink uses it as the
-- upsert key (primary.key.mode=record_key) on the PostgreSQL side.

CREATE TABLE customers (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(120)  NOT NULL,
  email      VARCHAR(160)  NOT NULL UNIQUE,
  city       VARCHAR(80),
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE products (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(160)  NOT NULL,
  category   VARCHAR(80)   NOT NULL,
  price      DECIMAL(10,2) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE orders (
  id           INT AUTO_INCREMENT PRIMARY KEY,
  customer_id  INT NOT NULL,
  status       VARCHAR(24) NOT NULL DEFAULT 'pending',
  total_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers (id)
);

CREATE TABLE order_items (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  order_id   INT NOT NULL,
  product_id INT NOT NULL,
  qty        INT NOT NULL DEFAULT 1,
  unit_price DECIMAL(10,2) NOT NULL,
  CONSTRAINT fk_items_order   FOREIGN KEY (order_id)   REFERENCES orders (id),
  CONSTRAINT fk_items_product FOREIGN KEY (product_id) REFERENCES products (id)
);

-- ── Seed data (~20 rows total) ────────────────────────────────────────────
-- Starter rows only. Add / update / delete more yourself to watch CDC flow.

INSERT INTO customers (name, email, city) VALUES
  ('Ayu Lestari',    'ayu@example.com',    'Jakarta'),
  ('Budi Santoso',   'budi@example.com',   'Bandung'),
  ('Citra Dewi',     'citra@example.com',  'Surabaya'),
  ('Dimas Pratama',  'dimas@example.com',  'Medan'),
  ('Eka Putri',      'eka@example.com',    'Semarang'),
  ('Fajar Nugroho',  'fajar@example.com',  'Yogyakarta');

INSERT INTO products (name, category, price) VALUES
  ('Wireless Mouse',     'Electronics', 149000.00),
  ('Mechanical Keyboard','Electronics', 689000.00),
  ('USB-C Hub',          'Accessories', 329000.00),
  ('Laptop Stand',       'Accessories', 199000.00),
  ('Noise-Cancel Headset','Audio',     1250000.00),
  ('Webcam 1080p',       'Electronics', 459000.00);

INSERT INTO orders (customer_id, status, total_amount) VALUES
  (1, 'paid',      838000.00),
  (2, 'pending',   329000.00),
  (3, 'shipped',  1709000.00),
  (1, 'paid',      199000.00),
  (5, 'cancelled', 459000.00);

INSERT INTO order_items (order_id, product_id, qty, unit_price) VALUES
  (1, 1, 1, 149000.00),
  (1, 2, 1, 689000.00),
  (2, 3, 1, 329000.00),
  (3, 5, 1, 1250000.00),
  (3, 2, 1, 689000.00),
  (4, 4, 1, 199000.00),
  (5, 6, 1, 459000.00);

-- ── Debezium CDC user ─────────────────────────────────────────────────────
-- Privileges Debezium needs: SELECT (snapshot), RELOAD/FLUSH (consistent
-- snapshot), SHOW DATABASES, and REPLICATION SLAVE/CLIENT (binlog streaming).

CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'dbz';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
