-- This is a complete, one-time setup script for the beancount database.
-- It will DROP all existing tables, CREATE the new schema, and GRANT permissions.
-- Run this entire script as the database owner (e.g., 'sagar').

-- ========= PART 1: DROP EXISTING TABLES =========
-- This ensures a clean slate before creating the new schema.

DROP TABLE IF EXISTS
    commodities,
    accounts,
    transactions,
    postings,
    posting_metadata,
    tags,
    transaction_tags,
    links,
    transaction_links,
    transaction_metadata,
    balance_assertions,
    prices,
    events,
    documents,
    import_state
CASCADE;


-- ========= PART 2: CREATE NEW SCHEMA =========
-- This section is the content of the create_schema.sql file.

-- Set timezone to UTC to ensure consistency
SET timezone = 'UTC';

-- Table: commodities
CREATE TABLE commodities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table: accounts
CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'open',
    open_date DATE,
    close_date DATE,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table: transactions
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    external_id TEXT NOT NULL UNIQUE,
    date DATE NOT NULL,
    flag VARCHAR(20) NOT NULL,
    payee TEXT,
    narration TEXT NOT NULL,
    source_file TEXT,
    source_line INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table: postings
CREATE TABLE postings (
    id SERIAL PRIMARY KEY,
    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    flag VARCHAR(20),
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    amount NUMERIC(19, 4) NOT NULL,
    currency_id INTEGER NOT NULL REFERENCES commodities(id),
    cost_amount NUMERIC(19, 4),
    cost_currency_id INTEGER REFERENCES commodities(id),
    price_amount NUMERIC(19, 4),
    price_currency_id INTEGER REFERENCES commodities(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table: posting_metadata
CREATE TABLE posting_metadata (
    id SERIAL PRIMARY KEY,
    posting_id INTEGER NOT NULL REFERENCES postings(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    UNIQUE (posting_id, key)
);

-- Table: tags
CREATE TABLE tags (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Table: transaction_tags
CREATE TABLE transaction_tags (
    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (transaction_id, tag_id)
);

-- Table: links
CREATE TABLE links (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

-- Table: transaction_links
CREATE TABLE transaction_links (
    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    link_id INTEGER NOT NULL REFERENCES links(id) ON DELETE CASCADE,
    PRIMARY KEY (transaction_id, link_id)
);

-- Table: transaction_metadata
CREATE TABLE transaction_metadata (
    id SERIAL PRIMARY KEY,
    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    UNIQUE (transaction_id, key)
);

-- Table: balance_assertions
CREATE TABLE balance_assertions (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    amount NUMERIC(19, 4) NOT NULL,
    currency_id INTEGER NOT NULL REFERENCES commodities(id),
    source_file TEXT,
    source_line INTEGER
);

-- Table: prices
CREATE TABLE prices (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    commodity_id INTEGER NOT NULL REFERENCES commodities(id) ON DELETE CASCADE,
    price_amount NUMERIC(19, 4) NOT NULL,
    price_currency_id INTEGER NOT NULL REFERENCES commodities(id),
    source_file TEXT,
    source_line INTEGER
);

-- Table: events
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    source_file TEXT,
    source_line INTEGER
);

-- Table: documents
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    file_path TEXT NOT NULL,
    source_file TEXT,
    source_line INTEGER
);

-- Table: import_state
-- Stores the state hash of the last successful import to detect changes.
CREATE TABLE import_state (
    id INT PRIMARY KEY,
    last_successful_hash TEXT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_transactions_external_id ON transactions(external_id);
CREATE INDEX idx_transactions_date ON transactions(date);
CREATE INDEX idx_postings_account_id ON postings(account_id);
CREATE INDEX idx_accounts_name ON accounts(name);
CREATE INDEX idx_accounts_type ON accounts(type);
CREATE INDEX idx_tags_name ON tags(name);
CREATE INDEX idx_links_name ON links(name);
CREATE INDEX idx_transaction_metadata_key ON transaction_metadata(key);
CREATE INDEX idx_posting_metadata_key ON posting_metadata(key);
CREATE INDEX idx_balance_assertions_account_id ON balance_assertions(account_id);
CREATE INDEX idx_prices_commodity_id ON prices(commodity_id);
CREATE INDEX idx_documents_account_id ON documents(account_id);


-- ========= PART 3: CREATE USERS AND GRANT PERMISSIONS =========

-- CREATE USERS (if they don't already exist)
-- Note: Replace 'your-password-here' with actual secure passwords when creating new users
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'beancount_loader') THEN
        CREATE USER beancount_loader WITH PASSWORD 'your-password-here';
        RAISE NOTICE 'Created user: beancount_loader';
    ELSE
        RAISE NOTICE 'User already exists: beancount_loader';
    END IF;
    
    IF NOT EXISTS (SELECT FROM pg_user WHERE usename = 'beancount_analyst') THEN
        CREATE USER beancount_analyst WITH PASSWORD 'your-password-here';
        RAISE NOTICE 'Created user: beancount_analyst';
    ELSE
        RAISE NOTICE 'User already exists: beancount_analyst';
    END IF;
END
$$;

-- PERMISSIONS FOR WRITER (beancount_loader)
GRANT CONNECT ON DATABASE beancount TO beancount_loader;
GRANT USAGE ON SCHEMA public TO beancount_loader;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO beancount_loader;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO beancount_loader;

-- PERMISSIONS FOR ANALYST (beancount_analyst)
GRANT CONNECT ON DATABASE beancount TO beancount_analyst;
GRANT USAGE ON SCHEMA public TO beancount_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO beancount_analyst;

--
-- End of Script
--
