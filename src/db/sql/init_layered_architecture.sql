-- Инициализация слоеной архитектуры БД (DWH Layering)

-- 1. СХЕМЫ
CREATE SCHEMA IF NOT EXISTS raw;        -- Сырые JSONB данные из Sheets
CREATE SCHEMA IF NOT EXISTS stg_gsheets;    -- Типизированные данные (соответствуют листам)
CREATE SCHEMA IF NOT EXISTS lookups; -- Справочники (mapping/normalization)
CREATE SCHEMA IF NOT EXISTS analytics;  -- Витрины для экспорта и дашбордов

-- 2. СИСТЕМНЫЕ ТАБЛИЦЫ
-- [LOOKUPS / REFERENCES]

-- Сотрудники (Маппинг имен)
CREATE TABLE IF NOT EXISTS lookups.employees (
    id SERIAL PRIMARY KEY,
    full_name TEXT UNIQUE NOT NULL,
    role TEXT,
    aliases TEXT[], -- Массив альтернативных имен для маппинга
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Продукты и услуги
CREATE TABLE IF NOT EXISTS lookups.products (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    category TEXT,
    aliases TEXT[],
    base_price NUMERIC,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Категории расходов
CREATE TABLE IF NOT EXISTS lookups.expense_categories (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    parent_category TEXT,
    aliases TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. СЛУЖЕБНЫЕ ТАБЛИЦЫ RAW (Для аудита)
CREATE TABLE IF NOT EXISTS raw.sheets_dump (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    spreadsheet_id TEXT NOT NULL,
    sheet_name TEXT NOT NULL,
    data JSONB NOT NULL,
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);

-- 5. CORE LAYER (Normalized Business Data)
CREATE SCHEMA IF NOT EXISTS core;

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

CREATE TABLE IF NOT EXISTS core.schedule (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    legacy_id TEXT UNIQUE,
    date DATE,
    start_time TIME,
    end_time TIME,
    client_id BIGINT REFERENCES core.clients(id),
    status TEXT,
    type TEXT,
    category TEXT,
    comment TEXT,
    row_hash TEXT,
    source TEXT,
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.expenses (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    legacy_id TEXT UNIQUE,
    date DATE,
    category TEXT,
    amount NUMERIC DEFAULT 0,
    comment TEXT,
    row_hash TEXT,
    source TEXT,
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 6. ПРАВА ДОСТУПА
GRANT USAGE ON SCHEMA core TO authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO authenticated;
GRANT ALL ON ALL TABLES IN SCHEMA core TO service_role;
