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
        """Создает системные таблицы (logs и т.д.)."""
        ddl = """
        CREATE TABLE IF NOT EXISTS validation_logs (
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
        CREATE INDEX IF NOT EXISTS idx_validation_logs_run_id ON validation_logs(run_id);
        """
        log.info("Deploying meta tables...")
        await DBConnection.execute(ddl)
        log.info("Meta tables deployed.")

    async def deploy_staging_tables(self):
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
                        # 1. Get headers only (first row)
                        # We need to construct a range for just the header row
                        # Assuming header is the first row of the range. 
                        # Range "A2:V" -> fetch first row.
                        
                        # Use Extract to get all data (simple but potentially heavy if table is huge, 
                        # but we only need headers. GSheetsExtractor implementation fetches config range.
                        # Optimization: fetch only limited rows? 
                        # GSheetsExtractor currently fetches all. Optimizing it to fetch headers would be better 
                        # but for now let's reuse extractor logic if possible or just use gspread directly here 
                        # to be precise.
                        
                        # Let's use extractor's helper logic if we fetch empty data?
                        # No, let's just fetch everything. Staging tables are usually small enough.
                        
                        col_names, _ = await self.extractor.extract_sheet_data(
                            spreadsheet_id, str(gid), range_name, target_table
                        )
                        
                        if not col_names:
                            log.warning(f"No columns found for {target_table}")
                            continue
                            
                        # 2. Generate DDL
                        cols_ddl = [f'"{col}" text' for col in col_names]
                        
                        # System columns
                        cols_ddl.append('"_row_index" integer')
                        cols_ddl.append('"__row_hash" text')
                        cols_ddl.append('"_loaded_at" timestamp with time zone default now()')
                        
                        ddl = f'DROP TABLE IF EXISTS "{target_table}"; CREATE TABLE "{target_table}" ({", ".join(cols_ddl)});'
                        
                        log.info(f"Deploying schema for {target_table}...")
                        await conn.execute(ddl)
                        log.info(f"Table {target_table} created/recreated.")
                        
                    except Exception as e:
                        log.error(f"Failed to deploy schema for {target_table}: {e}")
