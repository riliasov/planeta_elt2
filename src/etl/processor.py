import logging
import time
from typing import Dict, Any, List, Optional
from src.etl.extractor import GSheetsExtractor
from src.etl.loader import DataLoader
from src.etl.validator import ContractValidator, ValidationResult
from src.db.connection import DBConnection
from src.config.settings import settings

log = logging.getLogger('processor')

class TableProcessor:
    """Процессор для обработки одной таблицы: Extract -> Validate -> Load."""
    
    def __init__(self, extractor: GSheetsExtractor, loader: DataLoader, validator: ContractValidator, run_id: Any):
        self.extractor = extractor
        self.loader = loader
        self.validator = validator
        self.run_id = str(run_id)

    async def process_table(self, spreadsheet_id: str, sheet_cfg: Dict[str, Any], full_refresh: bool, dry_run: bool) -> Dict[str, Any]:
        """Полный цикл обработки одной таблицы."""
        target_table = sheet_cfg['target_table']
        gid = sheet_cfg.get('gid', 0)
        range_name = sheet_cfg.get('range', 'A:Z')
        mode = sheet_cfg.get('mode', 'upsert')
        mapping = sheet_cfg.get('column_mapping')
        pk_field = sheet_cfg.get('pk', '__row_hash')
        
        # Убираем схему и суффиксы для поиска контракта
        table_base = target_table.split('.')[-1]
        contract_name = table_base.replace('_cur', '').replace('_hst', '')
        
        if contract_name == 'trainings':
            contract_name = 'schedule'
            
        is_full_refresh = full_refresh or (mode == 'replace')
        start_time = time.time()
        
        # 1. Извлечение
        col_names, rows = await self.extractor.extract_sheet_data(
            spreadsheet_id, str(gid), range_name, target_table, mapping=mapping
        )
        
        if not rows:
            return {'table': target_table, 'status': 'skipped', 'reason': 'no_data'}
            
        # 1.5. Audit Trace (Raw Dump)
        await self._dump_raw_data(spreadsheet_id, target_table, col_names, rows)

        # 2. Валидация
        dict_rows = [dict(zip(col_names, row)) for row in rows]
        val_result = self.validator.validate_dataset(dict_rows, contract_name)
        validation_errors = len(val_result.errors)
        
        if not val_result.is_valid:
            log.warning(f"⚠ {target_table}: обнаружено {validation_errors} ошибок валидации")
            if not dry_run:
                await self._log_validation_errors(target_table, val_result)
            
            # Проверка порогов
            self._check_error_thresholds(target_table, val_result)

        # 3. Загрузка
        if dry_run:
            load_stats = await self.loader.calculate_changes(target_table, col_names, rows, pk_field)
            status = 'dry_run'
        elif is_full_refresh:
            load_stats = await self.loader.load_full_refresh(target_table, col_names, rows)
            status = 'full_refresh'
        else:
            load_stats = await self.loader.load_cdc(target_table, col_names, rows, pk_field)
            status = 'cdc'
            
        duration_ms = int((time.time() - start_time) * 1000)
        
        return {
            'table': target_table,
            'status': status,
            'extracted': len(rows),
            'inserted': load_stats.get('inserted', 0),
            'updated': load_stats.get('updated', 0),
            'deleted': load_stats.get('deleted', 0),
            'errors': validation_errors,
            'duration_ms': duration_ms,
            'load_stats': load_stats
        }

    def _check_error_thresholds(self, table: str, result: ValidationResult):
        """Проверяет, не превышены ли лимиты ошибок."""
        if len(result.errors) > 20:
            raise ValueError(f"КРИТИЧНО: {len(result.errors)} ошибок в {table} (> 20).")
        
        errors_by_row = {}
        for err in result.errors:
            errors_by_row[err.row_index] = errors_by_row.get(err.row_index, 0) + 1
        
        if any(count > 5 for count in errors_by_row.values()):
            raise ValueError(f"КРИТИЧНО: Строки с >5 ошибками в {table}. Бит формат?")

    async def _log_validation_errors(self, table_name: str, result: ValidationResult):
        query = f"""
            INSERT INTO {settings.schema_ops}.validation_logs (run_id, table_name, row_index, column_name, 
            invalid_value, error_type, message) VALUES ($1, $2, $3, $4, $5, $6, $7)
        """
        params = [(self.run_id, table_name, e.row_index, e.column, str(e.value)[:255], e.error_type, e.message) for e in result.errors]
        try:
            async with await DBConnection.get_connection() as conn:
                await conn.executemany(query, params)
        except Exception as e:
            log.error(f"Ошибка сохранения логов валидации {table_name}: {e}")

    async def _dump_raw_data(self, spreadsheet_id: str, sheet_name: str, col_names: list, rows: list):
        import json
        query = "INSERT INTO raw.sheets_dump (spreadsheet_id, sheet_name, data) VALUES ($1, $2, $3)"
        try:
            full_data = json.dumps([dict(zip(col_names, row)) for row in rows], ensure_ascii=False)
            await DBConnection.execute(query, spreadsheet_id, sheet_name, full_data)
        except Exception as e:
            log.warning(f"Ошибка дампа сырых данных {sheet_name}: {e}")
