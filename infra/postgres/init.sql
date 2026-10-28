-- =============================================================================
-- Order Saga + Transactional Outbox & Inbox Schema (PostgreSQL 16)
-- 100% Cloud & Orchestrator Agnostic Schema
-- =============================================================================

CREATE TABLE IF NOT EXISTS orders (
  id VARCHAR(64) PRIMARY KEY,
  sku VARCHAR(64) NOT NULL,
  quantity INTEGER NOT NULL,
  amount NUMERIC(12, 2) NOT NULL,
  zip_code VARCHAR(32) NOT NULL,
  status VARCHAR(32) NOT NULL,
  cancellation_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS stock_items (
  sku VARCHAR(64) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0),
  reserved_quantity INTEGER NOT NULL CHECK (reserved_quantity >= 0)
);

CREATE TABLE IF NOT EXISTS stock_reservations (
  id VARCHAR(64) PRIMARY KEY,
  order_id VARCHAR(64) NOT NULL UNIQUE,
  sku VARCHAR(64) NOT NULL REFERENCES stock_items(sku),
  quantity INTEGER NOT NULL,
  status VARCHAR(32) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  released_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS payments (
  id VARCHAR(64) PRIMARY KEY,
  order_id VARCHAR(64) NOT NULL UNIQUE,
  amount NUMERIC(12, 2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  reason TEXT,
  processed_at TIMESTAMPTZ NOT NULL,
  refunded_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS shipments (
  id VARCHAR(64) PRIMARY KEY,
  order_id VARCHAR(64) NOT NULL UNIQUE,
  zip_code VARCHAR(32) NOT NULL,
  tracking_code VARCHAR(64),
  status VARCHAR(32) NOT NULL,
  failure_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS inbox_messages (
  idempotency_key VARCHAR(128) PRIMARY KEY,
  order_id VARCHAR(64) NOT NULL,
  step_name VARCHAR(64) NOT NULL,
  outcome VARCHAR(16) NOT NULL,
  response_payload JSONB NOT NULL,
  wrapper JSONB NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_inbox_order_id ON inbox_messages(order_id);

CREATE TABLE IF NOT EXISTS outbox_messages (
  id VARCHAR(64) PRIMARY KEY,
  order_id VARCHAR(64) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL,
  wrapper JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_created ON outbox_messages(status, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_order_id ON outbox_messages(order_id);

-- Initial Seed for Stock Items
INSERT INTO stock_items (sku, name, available_quantity, reserved_quantity)
VALUES
  ('SKU-100', 'Mechanical Keyboard Pro', 10, 0),
  ('SKU-200', 'Ultrawide Monitor 34"', 5, 0)
ON CONFLICT (sku) DO NOTHING;
