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
    """Оркестратор ELT пайплайна с сохранением метрик."""
    
    def __init__(self):
        self.extractor = GSheetsExtractor()
        self.loader = DataLoader()
        self.transformer = Transformer()
        self.validator = ContractValidator()
        self.run_id = uuid.uuid4()
        self._run_stats = {
            'tables_processed': 0,
            'total_rows_synced': 0,
            'validation_errors': 0
        }

    async def run(self, 
                  skip_load: bool = False, 
                  skip_transform: bool = False, 
                  full_refresh: bool = False,
                  dry_run: bool = False):
        """Запуск ETL пайплайна.
        
        Args:
            skip_load: Пропустить фазу загрузки
            skip_transform: Пропустить фазу трансформации  
            full_refresh: Полная перезагрузка (TRUNCATE + INSERT)
            dry_run: Показать изменения без применения
        """
        self.dry_run = dry_run
        start_time = time.time()
        mode = 'full_refresh' if full_refresh else 'cdc'
        error_message = None
        
        log.info(f"=== Starting ELT Pipeline (Run ID: {self.run_id}) ===")
        
        # Регистрируем начало run
        await self._start_run(mode)
        
        try:
            if not skip_load:
                await self._run_load_phase(full_refresh)
            else:
                log.info("Skipping Load Phase")

            if not skip_transform:
                await self._run_transform_phase()
            else:
                log.info("Skipping Transform Phase")
                
            status = 'success'
        except Exception as e:
            status = 'failed'
            error_message = str(e)
            log.critical(f"Pipeline failed: {e}", exc_info=True)
            raise
        finally:
            duration = time.time() - start_time
            await self._finish_run(status, duration, error_message)
            log.info(f"=== Pipeline Finished in {duration:.2f}s (status: {status}) ===")

    async def _start_run(self, mode: str):
        """Регистрирует начало run в elt_runs."""
        query = """
            INSERT INTO elt_runs (run_id, mode, status)
            VALUES ($1, $2, 'running')
        """
        try:
            await DBConnection.execute(query, str(self.run_id), mode)
        except Exception as e:
            log.warning(f"Failed to register run start: {e}")

    async def _finish_run(self, status: str, duration: float, error_message: Optional[str] = None):
        """Обновляет elt_runs с результатами run."""
        query = """
            UPDATE elt_runs SET
                finished_at = NOW(),
                status = $2,
                duration_seconds = $3,
                tables_processed = $4,
                total_rows_synced = $5,
                validation_errors = $6,
                error_message = $7
            WHERE run_id = $1
        """
        try:
            await DBConnection.execute(
                query,
                str(self.run_id),
                status,
                round(duration, 2),
                self._run_stats['tables_processed'],
                self._run_stats['total_rows_synced'],
                self._run_stats['validation_errors'],
                error_message
            )
        except Exception as e:
            log.warning(f"Failed to update run finish: {e}")

    async def _log_table_stats(self, table_name: str, stats: Dict[str, int], 
                                validation_errors: int, duration_ms: int):
        """Сохраняет статистику по таблице в elt_table_stats."""
        query = """
            INSERT INTO elt_table_stats (
                run_id, table_name, rows_extracted, rows_inserted, 
                rows_updated, rows_deleted, rows_unchanged, validation_errors, duration_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """
        try:
            await DBConnection.execute(
                query,
                str(self.run_id),
                table_name,
                stats.get('extracted', 0),
                stats.get('inserted', 0),
                stats.get('updated', 0),
                stats.get('deleted', 0),
                stats.get('unchanged', 0),
                validation_errors,
                duration_ms
            )
        except Exception as e:
            log.warning(f"Failed to log table stats for {table_name}: {e}")

    async def _run_load_phase(self, full_refresh: bool):
        dry_run_mode = getattr(self, 'dry_run', False)
        mode_str = 'DRY-RUN ' if dry_run_mode else ''
        log.info(f"Starting {mode_str}Load Phase (Mode: {'Full Refresh' if full_refresh else 'CDC'})")
        
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
                
                contract_name = target_table.replace('_cur', '').replace('_hst', '')
                if contract_name == 'trainings':
                    contract_name = 'schedule'
                
                is_full_refresh = full_refresh or (mode == 'replace')
                table_start = time.time()
                validation_errors = 0
                
                try:
                    # 1. Extract
                    col_names, rows = await self.extractor.extract_sheet_data(
                        spreadsheet_id, str(gid), range_name, target_table
                    )
                    
                    if not rows:
                        continue
                        
                    # 2. Validate
                    log.info(f"Validating {target_table} using contract '{contract_name}'...")
                    dict_rows = [dict(zip(col_names, row)) for row in rows]
                    val_result = self.validator.validate_dataset(dict_rows, contract_name)
                    
                    if not val_result.is_valid:
                        validation_errors = len(val_result.errors)
                        log.warning(f"⚠ {target_table}: detected {validation_errors} validation errors")
                        
                        if not dry_run_mode:
                            await self._log_validation_errors(target_table, val_result)

                        if validation_errors > 30:
                            raise ValueError(f"CRITICAL: Too many validation errors in {target_table} ({validation_errors} > 30). Aborting.")
                    else:
                        log.info(f"✓ {target_table}: all rows are valid")

                    pk_field = sheet_cfg.get('pk', '__row_hash')

                    # 3. Load (или только показать статистику в dry-run)
                    if dry_run_mode:
                        # В dry-run режиме только вычисляем статистику без применения
                        load_stats = await self.loader.calculate_changes(target_table, col_names, rows, pk_field)
                        log.info(f"🔍 DRY-RUN {target_table}: "
                                f"would insert={load_stats.get('insert', 0)}, "
                                f"update={load_stats.get('update', 0)}, "
                                f"delete={load_stats.get('delete', 0)}, "
                                f"unchanged={load_stats.get('unchanged', 0)}")
                    elif is_full_refresh:
                        load_stats = await self.loader.load_full_refresh(target_table, col_names, rows)
                    else:
                        load_stats = await self.loader.load_cdc(target_table, col_names, rows, pk_field)
                    
                    # Добавляем extracted count
                    load_stats['extracted'] = len(rows)
                    
                    # Обновляем общую статистику
                    self._run_stats['tables_processed'] += 1
                    self._run_stats['total_rows_synced'] += load_stats.get('inserted', 0) + load_stats.get('updated', 0)
                    self._run_stats['validation_errors'] += validation_errors
                    
                    # Логируем статистику таблицы (если не dry-run)
                    if not dry_run_mode:
                        duration_ms = int((time.time() - table_start) * 1000)
                        await self._log_table_stats(target_table, load_stats, validation_errors, duration_ms)
                        
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
