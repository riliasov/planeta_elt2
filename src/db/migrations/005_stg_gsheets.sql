-- MIGRATION PART 3: Harmonization (stg_gsheets)
-- Run this in Supabase SQL Editor

BEGIN;

-- 1. Создаем схему для Google Sheets
CREATE SCHEMA IF NOT EXISTS stg_gsheets;

-- 2. Переносим таблицы (если они существуют в public)
DO $$
DECLARE
    t text;
    tables text[] := ARRAY[
        'clients', 'sales', 'expenses', 'trainings', -- Основные таблицы (без суффиксов в базе, как выяснилось)
        -- Также проверим старые суффиксы на всякий случай
        'clients_cur', 'clients_hst', 
        'sales_cur', 'sales_hst',
        'trainings_cur', 'trainings_hst',
        'expenses_cur', 'expenses_hst',
        'rates', 'price_reference'
    ];
BEGIN
    FOREACH t IN ARRAY tables LOOP
        -- Check public
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = t) THEN
            EXECUTE format('ALTER TABLE public.%I SET SCHEMA stg_gsheets', t);
            RAISE NOTICE 'Moved table % from public to stg_gsheets', t;
        END IF;
        -- Check staging (legacy name)
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'staging' AND table_name = t) THEN
            EXECUTE format('ALTER TABLE staging.%I SET SCHEMA stg_gsheets', t);
            RAISE NOTICE 'Moved table % from staging to stg_gsheets', t;
        END IF;
    END LOOP;
END $$;

-- 3. Права доступа
GRANT USAGE ON SCHEMA stg_gsheets TO service_role;
GRANT ALL ON ALL TABLES IN SCHEMA stg_gsheets TO service_role;
-- Anon доступ лучше пока не давать, если не просили

COMMIT;
