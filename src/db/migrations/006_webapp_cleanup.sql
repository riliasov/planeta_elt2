-- MIGRATION PART 4: Clean Public & Raw
-- Run this in Supabase SQL Editor

BEGIN;

-- 1. Создаем схему для Веб-приложения
CREATE SCHEMA IF NOT EXISTS webapp;

-- 2. Переносим таблицы приложения из public в webapp
DO $$
DECLARE
    t text;
    tables text[] := ARRAY[
        'auth_attempts', 'auth_events', 'auth_requests', 'auth_sessions', 'auth_whitelist',
        'employees', 'products', 'schedule', 
        'notification_queue', 'notification_rules'
    ];
BEGIN
    FOREACH t IN ARRAY tables LOOP
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = t) THEN
            EXECUTE format('ALTER TABLE public.%I SET SCHEMA webapp', t);
            RAISE NOTICE 'Moved table % to webapp schema', t;
        END IF;
    END LOOP;
END $$;

-- 3. Удаляем Legacy таблицы из Raw (если они не используются)
-- ВАЖНО: Убедитесь, что эти таблицы действительно не нужны. 
-- Если raw.sheets_dump используется, его НЕ трогаем.
DROP TABLE IF EXISTS raw.clients_hst;
DROP TABLE IF EXISTS raw.sales_cur;
DROP TABLE IF EXISTS raw.sales_hst;
DROP TABLE IF EXISTS raw.expenses_cur;
DROP TABLE IF EXISTS raw.expenses_hst;
DROP TABLE IF EXISTS raw.trainings_cur;
DROP TABLE IF EXISTS raw.trainings_hst;

-- 4. Права доступа
GRANT USAGE ON SCHEMA webapp TO service_role;
GRANT USAGE ON SCHEMA webapp TO anon; -- Если веб-апп использует анонимный доступ
GRANT ALL ON ALL TABLES IN SCHEMA webapp TO service_role;
-- GRANT SELECT ON ALL TABLES IN SCHEMA webapp TO anon; -- Настройте при необходимости

COMMIT;
