import logging
import time
import uuid
from typing import Optional, List, Dict, Any
from src.config.settings import settings
from src.etl.extractor import GSheetsExtractor
from src.etl.loader import DataLoader
from src.etl.transformer import Transformer
from src.etl.validator import ContractValidator, ValidationResult
from src.db.connection import DBConnection

log = logging.getLogger('pipeline')

class ELTPipeline:
    def __init__(self):
        self.extractor = GSheetsExtractor()
        self.loader = DataLoader()
        self.transformer = Transformer()
        self.validator = ContractValidator()
        self.run_id = uuid.uuid4()

    async def run(self, 
                  skip_load: bool = False, 
                  skip_transform: bool = False, 
                  full_refresh: bool = False):
        """
        Запуск ETL пайплайна.
        """
        start_time = time.time()
        log.info(f"=== Starting ELT Pipeline (Run ID: {self.run_id}) ===")
        
        if not skip_load:
            await self._run_load_phase(full_refresh)
        else:
            log.info("Skipping Load Phase")

        if not skip_transform:
            await self._run_transform_phase()
        else:
            log.info("Skipping Transform Phase")

        duration = time.time() - start_time
        log.info(f"=== Pipeline Finished in {duration:.2f}s ===")

    async def _run_load_phase(self, full_refresh: bool):
        log.info(f"Starting Load Phase (Mode: {'Full Refresh' if full_refresh else 'CDC'})")
        
        config = settings.sources
        if not config:
            log.warning("No configuration found in sources.yml")
            return

        for spreadsheet_id, sdata in config.get('spreadsheets', {}).items():
            for sheet_cfg in sdata.get('sheets', []):
                target_table = sheet_cfg['target_table']
                gid = sheet_cfg.get('gid', 0)
                range_name = sheet_cfg.get('range', 'A:Z')
                mode = sheet_cfg.get('mode', 'upsert')
                
                # Маппинг таблицы на имя контракта
                contract_name = target_table.replace('_cur', '').replace('_hst', '')
                if contract_name == 'trainings':
                    contract_name = 'schedule'
                
                is_full_refresh = full_refresh or (mode == 'replace')
                
                try:
                    # 1. Extract
                    col_names, rows = await self.extractor.extract_sheet_data(
                        spreadsheet_id, str(gid), range_name, target_table
                    )
                    
                    if not rows:
                        continue
                        
                    # 2. Validate
                    log.info(f"Validating {target_table} using contract '{contract_name}'...")
                    # Преобразуем список строк (списки) в список словарей для валидатора
                    dict_rows = [dict(zip(col_names, row)) for row in rows]
                    val_result = self.validator.validate_dataset(dict_rows, contract_name)
                    
                    if not val_result.is_valid:
                        log.warning(f"⚠ {target_table}: detected {len(val_result.errors)} validation errors")
                        await self._log_validation_errors(target_table, val_result)
                    else:
                        log.info(f"✓ {target_table}: all rows are valid")

                    # 3. Load (загружаем всё, валидация только логирует ошибки)
                    if is_full_refresh:
                        await self.loader.load_full_refresh(target_table, col_names, rows)
                    else:
                        await self.loader.load_cdc(target_table, col_names, rows)
                        
                except Exception as e:
                    log.error(f"Failed to process {target_table}: {e}")

    async def _log_validation_errors(self, table_name: str, result: ValidationResult):
        """Записывает ошибки валидации в БД."""
        query = """
            INSERT INTO validation_logs (
                run_id, table_name, row_index, column_name, 
                invalid_value, error_type, message
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        """
        params = [
            (
                str(self.run_id),
                table_name,
                err.row_index,
                err.column,
                str(err.value)[:255] if err.value is not None else None,
                err.error_type,
                err.message
            )
            for err in result.errors
        ]
        
        try:
            async with await DBConnection.get_connection() as conn:
                await conn.executemany(query, params)
            log.info(f"Logged {len(params)} errors to validation_logs")
        except Exception as e:
            log.error(f"Failed to log validation errors: {e}")

    async def _run_transform_phase(self):
        log.info("Starting Transform Phase")
        await self.transformer.run()
