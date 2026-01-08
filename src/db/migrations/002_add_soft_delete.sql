-- Миграция: Добавление поддержки Soft Delete, Row Hash и Source
-- Добавляет колонки deleted_at, is_deleted, row_hash и source в основные таблицы

DO $$
DECLARE
    tbl text;
BEGIN
    FOR tbl IN 
        SELECT unnest(ARRAY['clients', 'sales', 'schedule', 'expenses', 'trainings'])
    LOOP
        -- Проверяем существование таблицы перед изменением
        IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = tbl) THEN
            -- Soft delete columns
            EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ', tbl);
            EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT FALSE', tbl);
            
            -- Row hash for sync
            EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS row_hash TEXT', tbl);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_row_hash ON public.%I(row_hash)', tbl, tbl);
            
            -- Source tracking (e.g. 'sales_cur', 'sales_hst')
            EXECUTE format('ALTER TABLE public.%I ADD COLUMN IF NOT EXISTS source TEXT', tbl);
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%I_source ON public.%I(source)', tbl, tbl);
            
            RAISE NOTICE 'Added soft delete, hash and source columns to %', tbl;
        END IF;
    END LOOP;
END $$;
