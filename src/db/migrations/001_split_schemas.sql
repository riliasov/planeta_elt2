-- Миграция: Разделение схем staging и public
-- Запускать один раз для создания структуры

-- 1. Создаём схему staging
CREATE SCHEMA IF NOT EXISTS staging;

-- 2. Перемещаем staging-таблицы в схему staging
-- (Примечание: выполнять для каждой *_cur и *_hst таблицы)

DO $$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND (table_name LIKE '%_cur' OR table_name LIKE '%_hst' OR table_name = 'rates' OR table_name = 'price_reference')
    LOOP
        EXECUTE format('ALTER TABLE public.%I SET SCHEMA staging', tbl);
        RAISE NOTICE 'Moved % to staging schema', tbl;
    END LOOP;
END $$;

-- 3. Обновляем search_path для удобства
-- ALTER DATABASE current_database() SET search_path TO public, staging;

-- 4. Выдаём права (если используется отдельный app user)
-- GRANT USAGE ON SCHEMA staging TO app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO app_user;

-- Проверка результата:
-- SELECT table_schema, table_name FROM information_schema.tables WHERE table_name LIKE '%_cur' OR table_name LIKE '%_hst';
