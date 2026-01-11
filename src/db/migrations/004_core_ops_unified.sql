-- MIGRATION PART 2: Core & Ops
-- Run this in Supabase SQL Editor

BEGIN;

-- 1. Создаем схемы
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS ops;

-- 2. Переносим служебные таблицы в ops
-- Проверяем существование перед переносом
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'elt_runs') THEN
        ALTER TABLE public.elt_runs SET SCHEMA ops;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'elt_table_stats') THEN
        ALTER TABLE public.elt_table_stats SET SCHEMA ops;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'validation_logs') THEN
        ALTER TABLE public.validation_logs SET SCHEMA ops;
    END IF;
END $$;

-- 3. Создаем CORE View (Unified Customers)
-- Предполагаем, что uuid-ossp расширение включено (обычно да в Supabase)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ВАЖНО: У нас есть зависимость от normalize_phone. 
-- Если функция в public/telegram, нужно ее найти.
-- Предположим, она в public. Если нет - заменим на простое сравнение.

CREATE OR REPLACE VIEW core.unified_customers AS
SELECT 
    -- Генерируем стабильный ID
    COALESCE(
        c.id, 
        uuid_generate_v5(uuid_ns_url(), 'tg:' || t.id::text)
    ) as id,
    
    -- CRM данные (приоритет)
    c.name as crm_name,
    c.phone as crm_phone,
    
    -- TG данные
    t.username as tg_username,
    t.phone as tg_phone,
    t.id as tg_user_id,
    
    -- Метаданные
    CASE 
        WHEN c.id IS NOT NULL THEN 'client'
        WHEN t.phone IS NOT NULL THEN 'warm_lead'
        ELSE 'guest'
    END as lifecycle_stage,
    
    COALESCE(c.updated_at, t.updated_at) as last_activity
    
FROM telegram.telegram_users t
FULL OUTER JOIN public.clients c 
    ON (
        -- Простая нормализация: удаляем все кроме цифр
        REGEXP_REPLACE(t.phone, '\D', '', 'g') = REGEXP_REPLACE(c.phone, '\D', '', 'g')
        AND t.phone IS NOT NULL 
        AND c.phone IS NOT NULL
        AND LENGTH(t.phone) > 5 -- Игнорируем короткие/битые номера
    );

-- 4. Права доступа
GRANT USAGE ON SCHEMA core TO service_role;
GRANT USAGE ON SCHEMA core TO anon;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO service_role;
-- Anon доступ к Core (только если нужно)
-- GRANT SELECT ON ALL TABLES IN SCHEMA core TO anon; 

GRANT USAGE ON SCHEMA ops TO service_role;
GRANT ALL ON ALL TABLES IN SCHEMA ops TO service_role;

COMMIT;
