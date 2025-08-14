-- This script creates the complete database schema for the beancount-to-sql project.
-- It is intended to be run once by a database administrator to set up the database.

-- Set timezone to UTC to ensure consistency
SET timezone = 'UTC';

--
-- Table: commodities
-- Stores all unique commodities (currencies, stocks, etc.).
--
CREATE TABLE commodities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE, -- E.g., 'USD', 'VTSAX', 'BTC'
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

--
-- Table: accounts
-- Stores all unique accounts.
--
CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    type VARCHAR(50) NOT NULL, -- E.g., 'Assets', 'Liabilities', 'Equity', 'Income', 'Expenses'
    status VARCHAR(20) NOT NULL DEFAULT 'open',
    open_date DATE,
    close_date DATE,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

--
-- Table: transactions
-- The core table for all transaction events.
--
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    external_id TEXT NOT NULL UNIQUE,
    date DATE NOT NULL,
    flag VARCHAR(20) NOT NULL, -- The transaction flag, e.g., '*' (cleared) or '!' (pending).
    payee TEXT,
    narration TEXT NOT NULL,
    source_file TEXT,
    source_line INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

--
-- Table: postings
-- Each transaction is composed of two or more postings.
--
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

--
-- Table: posting_metadata
-- Stores the key-value metadata associated with each individual posting.
--
CREATE TABLE posting_metadata (
    id SERIAL PRIMARY KEY,
    posting_id INTEGER NOT NULL REFERENCES postings(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    UNIQUE (posting_id, key)
);

--
-- Table: tags
-- Stores all unique tags.
--
CREATE TABLE tags (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

--
-- Table: transaction_tags
-- A junction table to link transactions and tags.
--
CREATE TABLE transaction_tags (
    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (transaction_id, tag_id)
);

--
-- Table: links
-- Stores all unique links.
--
CREATE TABLE links (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

--
-- Table: transaction_links
-- A junction table to link transactions and links.
--
CREATE TABLE transaction_links (
    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    link_id INTEGER NOT NULL REFERENCES links(id) ON DELETE CASCADE,
    PRIMARY KEY (transaction_id, link_id)
);

--
-- Table: transaction_metadata
-- Stores the key-value metadata associated with each transaction.
--
CREATE TABLE transaction_metadata (
    id SERIAL PRIMARY KEY,
    transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    UNIQUE (transaction_id, key)
);

--
-- Table: balance_assertions
-- Stores Beancount 'balance' assertions for data integrity checks.
--
CREATE TABLE balance_assertions (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    amount NUMERIC(19, 4) NOT NULL,
    currency_id INTEGER NOT NULL REFERENCES commodities(id),
    source_file TEXT,
    source_line INTEGER
);

--
-- Table: prices
-- Stores historical price data for commodities from 'price' directives.
--
CREATE TABLE prices (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    commodity_id INTEGER NOT NULL REFERENCES commodities(id) ON DELETE CASCADE,
    price_amount NUMERIC(19, 4) NOT NULL,
    price_currency_id INTEGER NOT NULL REFERENCES commodities(id),
    source_file TEXT,
    source_line INTEGER
);

--
-- Table: events
-- Stores 'event' directives, which associate a date with a key-value pair.
--
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    name TEXT NOT NULL,
    value TEXT NOT NULL,
    source_file TEXT,
    source_line INTEGER
);

--
-- Table: documents
-- Stores 'document' directives, linking an account to an external file path.
--
CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    file_path TEXT NOT NULL,
    source_file TEXT,
    source_line INTEGER
);

--
-- Table: import_state
-- Stores the state hash of the last successful import to detect changes.
--
CREATE TABLE import_state (
    id INT PRIMARY KEY,
    last_successful_hash TEXT NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

--
-- Indexes
-- Indexes for faster querying on commonly searched columns.
--
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

--
-- End of Schema
--
