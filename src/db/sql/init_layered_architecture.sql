-- Инициализация слоеной архитектуры БД (DWH Layering)

-- 1. СХЕМЫ
CREATE SCHEMA IF NOT EXISTS raw;        -- Сырые JSONB данные из Sheets
CREATE SCHEMA IF NOT EXISTS staging;    -- Типизированные данные (соответствуют листам)
CREATE SCHEMA IF NOT EXISTS "references"; -- Справочники (mapping/normalization)
CREATE SCHEMA IF NOT EXISTS analytics;  -- Витрины для отчетов

-- 2. ТАБЛИЦЫ СПРАВОЧНИКИ (REFERENCES)

-- Сотрудники (Маппинг имен)
CREATE TABLE IF NOT EXISTS "references".employees (
    id SERIAL PRIMARY KEY,
    full_name TEXT UNIQUE NOT NULL,
    role TEXT,
    aliases TEXT[],              -- Массив альтернативных написаний (Саша, Санёк)
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Продукты и услуги
CREATE TABLE IF NOT EXISTS "references".products (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    category TEXT,
    aliases TEXT[],
    base_price NUMERIC,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Категории расходов
CREATE TABLE IF NOT EXISTS "references".expense_categories (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    parent_category TEXT,
    aliases TEXT[],
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. СЛУЖЕБНЫЕ ТАБЛИЦЫ RAW (Для аудита)
CREATE TABLE IF NOT EXISTS raw.sheets_dump (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    spreadsheet_id TEXT NOT NULL,
    sheet_name TEXT NOT NULL,
    data JSONB NOT NULL,
    extracted_at TIMESTAMPTZ DEFAULT NOW()
);

-- Колонки в public (Core) для совместимости с текущим кодом
-- Мы оставляем public как основной слой для нормализованных данных,
-- но добавим поддержку работы через staging.
