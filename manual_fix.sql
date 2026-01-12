-- !!! ЗАПУСТИТЬ В SUPABASE SQL EDITOR !!!
-- Этот скрипт создает таблицы в схемах lookups и выдает права.

-- 1. Инициализация Схем (на случай, если чего-то не хватает)
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg_gsheets;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS lookups;
CREATE SCHEMA IF NOT EXISTS analytics;

-- 2. Таблицы LOOKUPS (Справочники)
CREATE TABLE IF NOT EXISTS lookups.employees (
    id SERIAL PRIMARY KEY,
    full_name TEXT UNIQUE NOT NULL,
    role TEXT,
    aliases TEXT[],
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lookups.products (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    category TEXT,
    aliases TEXT[],
    base_price NUMERIC,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lookups.expense_categories (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    parent_category TEXT,
    aliases TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Таблицы CORE (если еще нет)
CREATE TABLE IF NOT EXISTS core.clients (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    legacy_id TEXT UNIQUE,
    name TEXT,
    phone TEXT,
    status TEXT,
    child_name TEXT,
    child_dob DATE,
    age TEXT,
    balance NUMERIC DEFAULT 0,
    debt NUMERIC DEFAULT 0,
    spent NUMERIC DEFAULT 0,
    row_hash TEXT,
    source TEXT,
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.sales (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    legacy_id TEXT UNIQUE,
    date TIMESTAMPTZ,
    client_id BIGINT REFERENCES core.clients(id),
    product_name TEXT,
    type TEXT,
    category TEXT,
    quantity INTEGER DEFAULT 1,
    full_price NUMERIC DEFAULT 0,
    discount NUMERIC DEFAULT 0,
    final_price NUMERIC DEFAULT 0,
    cash NUMERIC DEFAULT 0,
    transfer NUMERIC DEFAULT 0,
    terminal NUMERIC DEFAULT 0,
    debt NUMERIC DEFAULT 0,
    comment TEXT,
    row_hash TEXT,
    source TEXT,
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. Права доступа
GRANT USAGE ON SCHEMA core TO authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA core TO service_role;

GRANT USAGE ON SCHEMA ops TO authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA ops TO service_role;

GRANT USAGE ON SCHEMA stg_gsheets TO authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA stg_gsheets TO service_role;

GRANT USAGE ON SCHEMA lookups TO authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA lookups TO authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA lookups TO service_role;

GRANT USAGE ON SCHEMA analytics TO authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA analytics TO service_role;
