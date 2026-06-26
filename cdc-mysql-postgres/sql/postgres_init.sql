-- ─────────────────────────────────────────────────────────────────────────
-- Target database bootstrap for the CDC demo.
--
-- Applied to the EXISTING local-postgres container by scripts/seed-dbs.sh.
-- It only needs to create the target database; the Debezium JDBC sink
-- connector creates the customers / products / orders / order_items tables
-- automatically (auto.create=true) when the first change events arrive.
-- ─────────────────────────────────────────────────────────────────────────

-- psql can't CREATE DATABASE inside a transaction / IF NOT EXISTS, so this is
-- run with: SELECT-guard from the seed script. Kept here for documentation.
CREATE DATABASE ecommerce;
