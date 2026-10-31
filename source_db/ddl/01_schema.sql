-- ShopSphere OLTP schema (source of truth for CDC).
-- Idempotent: safe to re-run.
CREATE SCHEMA IF NOT EXISTS shop;
SET search_path TO shop;

-- Keep updated_at honest without relying on application code.
CREATE OR REPLACE FUNCTION shop.set_updated_at() RETURNS trigger AS $fn$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$fn$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS shop.customers (
    customer_id BIGSERIAL PRIMARY KEY,
    email       TEXT        NOT NULL UNIQUE,
    full_name   TEXT        NOT NULL,
    country     CHAR(2)     NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shop.products (
    product_id BIGSERIAL PRIMARY KEY,
    sku        TEXT          NOT NULL UNIQUE,
    name       TEXT          NOT NULL,
    category   TEXT          NOT NULL,
    price      NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    is_active  BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shop.orders (
    order_id     BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT        NOT NULL REFERENCES shop.customers (customer_id),
    status       TEXT          NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','paid','shipped','delivered','cancelled','refunded')),
    channel      TEXT          NOT NULL DEFAULT 'web'
                 CHECK (channel IN ('web','mobile_app','marketplace','social')),
    total_amount NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
    currency     CHAR(3)       NOT NULL DEFAULT 'USD',
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shop.order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id      BIGINT        NOT NULL REFERENCES shop.orders (order_id),
    product_id    BIGINT        NOT NULL REFERENCES shop.products (product_id),
    quantity      INT           NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shop.payments (
    payment_id BIGSERIAL PRIMARY KEY,
    order_id   BIGINT        NOT NULL REFERENCES shop.orders (order_id),
    method     TEXT          NOT NULL CHECK (method IN ('card','paypal','mpesa','bank_transfer')),
    status     TEXT          NOT NULL DEFAULT 'pending'
               CHECK (status IN ('pending','captured','failed','refunded')),
    amount     NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    created_at TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shop.inventory (
    product_id BIGINT      PRIMARY KEY REFERENCES shop.products (product_id),
    stock_qty  INT         NOT NULL CHECK (stock_qty >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Clickstream-lite: powers the conversion funnel (view -> cart -> checkout -> purchase).
CREATE TABLE IF NOT EXISTS shop.events (
    event_id    BIGSERIAL PRIMARY KEY,
    session_id  UUID        NOT NULL,
    customer_id BIGINT,
    event_type  TEXT        NOT NULL
                CHECK (event_type IN ('product_view','add_to_cart','begin_checkout','purchase')),
    product_id  BIGINT,
    order_id    BIGINT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_orders_status_created ON shop.orders (status, created_at);
CREATE INDEX IF NOT EXISTS ix_order_items_order     ON shop.order_items (order_id);
CREATE INDEX IF NOT EXISTS ix_payments_order        ON shop.payments (order_id);
CREATE INDEX IF NOT EXISTS ix_events_occurred       ON shop.events (occurred_at);

DO $do$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['customers','products','orders','payments','inventory'] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = 'trg_' || t || '_updated_at'
              AND tgrelid = ('shop.' || t)::regclass
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE UPDATE ON shop.%I FOR EACH ROW EXECUTE FUNCTION shop.set_updated_at()',
                'trg_' || t || '_updated_at', t);
        END IF;
    END LOOP;
END
$do$;
