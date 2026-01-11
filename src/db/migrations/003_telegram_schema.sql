-- SAFE MIGRATION SCRIPT
-- Run this in Supabase SQL Editor

BEGIN;

-- 1. Создаем схему
CREATE SCHEMA IF NOT EXISTS telegram;

-- 2. Переносим таблицы (только если они еще в public)
DO $$
DECLARE
    t text;
    tables text[] := ARRAY[
        'telegram_chats', 'telegram_messages', 'telegram_users', 
        'telegram_health', 'telegram_failed_events', 'telegram_sync_queue'
    ];
BEGIN
    FOREACH t IN ARRAY tables LOOP
        IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = t) THEN
            EXECUTE format('ALTER TABLE public.%I SET SCHEMA telegram', t);
            RAISE NOTICE 'Moved table % to telegram schema', t;
        END IF;
    END LOOP;
END $$;

-- 3. Пересоздаем RPC функцию
DROP FUNCTION IF EXISTS public.get_chats_latest_message_id();
DROP FUNCTION IF EXISTS telegram.get_chats_latest_message_id();

CREATE OR REPLACE FUNCTION telegram.get_chats_latest_message_id()
RETURNS TABLE (chat_id BIGINT, last_message_id BIGINT) 
LANGUAGE sql
SECURITY DEFINER
SET search_path = telegram, public
AS $$
  SELECT m.chat_id, MAX(m.telegram_id) as last_message_id
  FROM telegram.telegram_messages m
  GROUP BY m.chat_id;
$$;

-- 4. Права доступа (Permissions)
GRANT USAGE ON SCHEMA telegram TO service_role;
GRANT USAGE ON SCHEMA telegram TO anon;

GRANT ALL ON ALL TABLES IN SCHEMA telegram TO service_role;
GRANT SELECT ON ALL TABLES IN SCHEMA telegram TO anon;

-- Для Sequence'ов (если есть serial поля)
GRANT ALL ON ALL SEQUENCES IN SCHEMA telegram TO service_role;

COMMIT;
