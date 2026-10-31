-- Logical-replication setup for Airbyte. Run as a superuser / rds_superuser:
--   psql -v ON_ERROR_STOP=1 -v airbyte_password="$AIRBYTE_CDC_PASSWORD" -f 02_cdc.sql
-- Server prerequisites (restart required): wal_level=logical, max_replication_slots>=4,
-- max_wal_senders>=4, and max_slot_wal_keep_size set (see docker-compose.yml).
-- On AWS RDS: set rds.logical_replication=1 and `GRANT rds_replication TO airbyte_cdc;`.

DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airbyte_cdc') THEN
        CREATE ROLE airbyte_cdc WITH LOGIN REPLICATION;
    END IF;
END
$do$;

ALTER ROLE airbyte_cdc WITH PASSWORD :'airbyte_password';

GRANT USAGE ON SCHEMA shop TO airbyte_cdc;
GRANT SELECT ON ALL TABLES IN SCHEMA shop TO airbyte_cdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA shop GRANT SELECT ON TABLES TO airbyte_cdc;

-- Full "before" images so DELETE/UPDATE records carry every column, not just the PK.
-- events is append-only, so the default identity is enough.
ALTER TABLE shop.customers   REPLICA IDENTITY FULL;
ALTER TABLE shop.products    REPLICA IDENTITY FULL;
ALTER TABLE shop.orders      REPLICA IDENTITY FULL;
ALTER TABLE shop.order_items REPLICA IDENTITY FULL;
ALTER TABLE shop.payments    REPLICA IDENTITY FULL;
ALTER TABLE shop.inventory   REPLICA IDENTITY FULL;

DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'shopsphere_pub') THEN
        CREATE PUBLICATION shopsphere_pub FOR TABLE
            shop.customers, shop.products, shop.orders,
            shop.order_items, shop.payments, shop.inventory, shop.events;
    END IF;
END
$do$;

SELECT pg_create_logical_replication_slot('airbyte_slot', 'pgoutput')
WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'airbyte_slot');
