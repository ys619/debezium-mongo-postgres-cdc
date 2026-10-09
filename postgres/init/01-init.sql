-- PostgreSQL Target Database Initialization for Debezium CDC Replication POC

-- 1. Customers Table (Target of MongoDB 'inventory.customers')
CREATE TABLE IF NOT EXISTS customers (
    id VARCHAR(64) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    address JSONB,
    status VARCHAR(50) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_document JSONB,
    deleted BOOLEAN DEFAULT FALSE,
    last_cdc_op VARCHAR(10),           -- 'c' (create), 'u' (update), 'd' (delete), 'r' (read/snapshot)
    last_cdc_timestamp TIMESTAMP
);

-- 2. Orders Table (Target of MongoDB 'inventory.orders')
CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64),
    order_number VARCHAR(100),
    total_amount NUMERIC(12, 2) DEFAULT 0.00,
    items JSONB,
    order_status VARCHAR(50) DEFAULT 'PENDING',
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_document JSONB,
    deleted BOOLEAN DEFAULT FALSE,
    last_cdc_op VARCHAR(10),
    last_cdc_timestamp TIMESTAMP
);

-- 3. CDC Audit Log Table (Tracks every CDC transaction event received)
CREATE TABLE IF NOT EXISTS cdc_audit_log (
    audit_id SERIAL PRIMARY KEY,
    source_collection VARCHAR(100),
    document_id VARCHAR(64),
    operation VARCHAR(10),
    cdc_timestamp TIMESTAMP,
    payload JSONB,
    replicated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_cdc_audit_source ON cdc_audit_log(source_collection, document_id);
