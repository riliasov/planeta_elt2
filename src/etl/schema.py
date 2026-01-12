import logging
import asyncio
from src.config.settings import settings
from src.db.connection import DBConnection
from src.etl.extractor import GSheetsExtractor
from src.utils.helpers import slugify

log = logging.getLogger('schema')

class SchemaManager:
    def __init__(self):
        self.extractor = GSheetsExtractor()

    async def deploy_meta_tables(self):
        """Создает системные таблицы (logs, runs, stats) и базовую структуру схем."""
        ddl = f"""
        -- 1. СХЕМЫ
        CREATE SCHEMA IF NOT EXISTS {settings.schema_raw};
        CREATE SCHEMA IF NOT EXISTS {settings.schema_staging};
        CREATE SCHEMA IF NOT EXISTS {settings.schema_references};
        CREATE SCHEMA IF NOT EXISTS {settings.schema_analytics};
        CREATE SCHEMA IF NOT EXISTS {settings.schema_ops};

        -- 2. ТАБЛИЦЫ REFERENCES
        CREATE TABLE IF NOT EXISTS {settings.schema_references}.employees (
            id SERIAL PRIMARY KEY,
            full_name TEXT UNIQUE NOT NULL,
            role TEXT,
            aliases TEXT[],
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {settings.schema_references}.products (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            category TEXT,
            aliases TEXT[],
            base_price NUMERIC,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {settings.schema_references}.expense_categories (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            parent_category TEXT,
            aliases TEXT[],
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        -- 3. ТАБЛИЦЫ RAW
        CREATE TABLE IF NOT EXISTS {settings.schema_raw}.sheets_dump (
            id BIGSERIAL PRIMARY KEY,
            spreadsheet_id TEXT NOT NULL,
            sheet_name TEXT NOT NULL,
            data JSONB NOT NULL,
            extracted_at TIMESTAMPTZ DEFAULT NOW()
        );

        -- 4. СИСТЕМНЫЕ ТАБЛИЦЫ В OPS
        CREATE TABLE IF NOT EXISTS {settings.schema_ops}.validation_logs (
            id BIGSERIAL PRIMARY KEY,
            run_id UUID NOT NULL,
            table_name TEXT NOT NULL,
            row_index INTEGER NOT NULL,
            column_name TEXT,
            invalid_value TEXT,
            error_type TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_validation_logs_run_id ON {settings.schema_ops}.validation_logs(run_id);
        
        CREATE TABLE IF NOT EXISTS {settings.schema_ops}.elt_runs (
            run_id UUID PRIMARY KEY,
            started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at TIMESTAMPTZ,
            status TEXT NOT NULL DEFAULT 'running',
            mode TEXT NOT NULL DEFAULT 'cdc',
            tables_processed INTEGER DEFAULT 0,
            total_rows_synced INTEGER DEFAULT 0,
            validation_errors INTEGER DEFAULT 0,
            duration_seconds NUMERIC(10,2),
            error_message TEXT,
            CONSTRAINT elt_runs_status_check CHECK (status IN ('running', 'success', 'failed'))
        );
        CREATE INDEX IF NOT EXISTS idx_elt_runs_started_at ON {settings.schema_ops}.elt_runs(started_at DESC);
        
        CREATE TABLE IF NOT EXISTS {settings.schema_ops}.elt_table_stats (
            id BIGSERIAL PRIMARY KEY,
            run_id UUID NOT NULL REFERENCES {settings.schema_ops}.elt_runs(run_id) ON DELETE CASCADE,
            table_name TEXT NOT NULL,
            rows_extracted INTEGER DEFAULT 0,
            rows_inserted INTEGER DEFAULT 0,
            rows_updated INTEGER DEFAULT 0,
            rows_deleted INTEGER DEFAULT 0,
            rows_unchanged INTEGER DEFAULT 0,
            validation_errors INTEGER DEFAULT 0,
            duration_ms INTEGER,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_elt_table_stats_run_id ON {settings.schema_ops}.elt_table_stats(run_id);
        """
        log.info(f"Развертывание мета-таблиц и схем в {settings.schema_ops}...")
        await DBConnection.execute(ddl)
        log.info("Мета-таблицы и базовые схемы развернуты.")

    async def deploy_staging_tables(self, use_staging_schema: bool = False):
        """Пересоздает staging таблицы на основе заголовков из Sheets."""
        config = settings.sources
        if not config:
            log.warning("No configuration found")
            return
        
        async with await DBConnection.get_connection() as conn:
            for spreadsheet_id, sdata in config.get('spreadsheets', {}).items():
                for sheet_cfg in sdata.get('sheets', []):
                    target_table = sheet_cfg['target_table']
                    gid = sheet_cfg.get('gid', 0)
                    range_name = sheet_cfg.get('range', 'A:Z')
                    
                    try:
                        col_names, _ = await self.extractor.extract_sheet_data(
                            spreadsheet_id, str(gid), range_name, target_table
                        )
                        
                        if not col_names:
                            log.warning(f"No columns found for {target_table}")
                            continue
                            
                        # 2. Generate DDL
                        cols_ddl = [f'"{col}" text' for col in col_names]
                        cols_ddl.append('"_row_index" integer')
                        cols_ddl.append('"__row_hash" text')
                        cols_ddl.append('"_loaded_at" timestamp with time zone default now()')
                        
                        # Определяем имя таблицы с учетом схемы
                        if '.' in target_table:
                            schema, tbl = target_table.split('.', 1)
                            full_table_name = f'"{schema}"."{tbl}"'
                        else:
                            # Fallback для совместимости
                            prefix = 'staging.' if use_staging_schema else ''
                            full_table_name = f'{prefix}"{target_table}"'

                        ddl = f'DROP TABLE IF EXISTS {full_table_name}; CREATE TABLE {full_table_name} ({", ".join(cols_ddl)});'
                        
                        log.info(f"Deploying schema for {target_table} (DDL: {full_table_name})...")
                        await conn.execute(ddl)
                        log.info(f"Table {target_table} created/recreated.")
                        
                    except Exception as e:
                        log.error(f"Failed to deploy schema for {target_table}: {e}")
