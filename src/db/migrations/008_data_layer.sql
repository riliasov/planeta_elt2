-- MIGRATION: Data Layer & Security
-- 008_core_layer.sql
-- Создание защищенного слоя core и миграция таблиц

BEGIN;

-- 1. Создаем схему core (если не создана ранее в 004)
CREATE SCHEMA IF NOT EXISTS core;

-- 2. Переносим таблицы из таблицы public в core
-- Мы используем DO блок для безопасного переноса
DO $$
DECLARE
    t text;
    tables text[] := ARRAY['clients', 'sales', 'schedule', 'expenses', 'products', 'employees'];
BEGIN
    FOR t IN SELECT unnest(tables) LOOP
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = t) THEN
            EXECUTE format('ALTER TABLE public.%I SET SCHEMA core', t);
        END IF;
    END LOOP;
END $$;

-- 3. Настройка прав доступа
-- Отзываем всё у anon (публичный доступ через API)
REVOKE ALL ON SCHEMA core FROM anon;
REVOKE ALL ON ALL TABLES IN SCHEMA core FROM anon;

-- Даем права authenticated (нормальные пользователи/приложения)
GRANT USAGE ON SCHEMA core TO authenticated;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA core TO authenticated;

-- Даем полные права service_role (для нашего пайплайна)
GRANT USAGE ON SCHEMA core TO service_role;
GRANT ALL ON ALL TABLES IN SCHEMA core TO service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA core TO service_role;

-- 4. Включаем RLS на критичных таблицах
ALTER TABLE core.clients ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.sales ENABLE ROW LEVEL SECURITY;

-- Создаем базовую политику: authenticated пользователи могут видеть всё
-- (В будущем здесь можно добавить фильтрацию по ролям)
CREATE POLICY "Allow authenticated view" ON core.clients FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow authenticated view" ON core.sales FOR SELECT TO authenticated USING (true);

COMMIT;
