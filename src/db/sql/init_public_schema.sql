-- Init/Migrate Public Schema
-- Adds row_hash, source, and soft delete columns to public tables

-- 1. CLIENTS
CREATE TABLE IF NOT EXISTS clients (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    legacy_id TEXT UNIQUE,
    name TEXT,
    phone TEXT,
    status TEXT,
    child_name TEXT,
    child_dob DATE,
    age TEXT,
    balance NUMERIC,
    debt NUMERIC,
    spent NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE clients ADD COLUMN IF NOT EXISTS row_hash TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
ALTER TABLE clients ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE;

-- 2. SALES
CREATE TABLE IF NOT EXISTS sales (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sale_date DATE,
    client_id BIGINT REFERENCES clients(id),
    client_name TEXT, 
    product TEXT,
    amount NUMERIC, 
    price NUMERIC,
    total NUMERIC,
    category TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE sales ADD COLUMN IF NOT EXISTS row_hash TEXT;
ALTER TABLE sales ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE sales ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
ALTER TABLE sales ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE;

-- 3. SCHEDULE
CREATE TABLE IF NOT EXISTS schedule (
     id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
     date DATE,
     start_time TIME,
     end_time TIME,
     client_name TEXT,
     status TEXT,
     trainer TEXT,
     created_at TIMESTAMPTZ DEFAULT NOW(),
     updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE schedule ADD COLUMN IF NOT EXISTS row_hash TEXT;
ALTER TABLE schedule ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE schedule ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
ALTER TABLE schedule ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE;

-- 4. EXPENSES
CREATE TABLE IF NOT EXISTS expenses (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date DATE,
    category TEXT,
    amount NUMERIC,
    comment TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE expenses ADD COLUMN IF NOT EXISTS row_hash TEXT;
ALTER TABLE expenses ADD COLUMN IF NOT EXISTS source TEXT;
ALTER TABLE expenses ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
ALTER TABLE expenses ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE;
