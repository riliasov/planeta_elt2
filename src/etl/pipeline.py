import logging
import time
import uuid
from typing import Optional, List, Dict, Any
from src.config.settings import settings
from src.etl.extractor import GSheetsExtractor
from src.etl.loader import DataLoader
from src.etl.transformer import Transformer
from src.etl.exporter import DataMartExporter
from src.etl.validator import ContractValidator
from src.etl.processor import TableProcessor
from src.db.connection import DBConnection

log = logging.getLogger('pipeline')

class ELTPipeline:
    """Оркестратор ELT пайплайна с сохранением метрик."""
    
    def __init__(self):
        self.extractor = GSheetsExtractor()
        self.loader = DataLoader()
        self.transformer = Transformer()
        self.exporter = DataMartExporter()
        self.validator = ContractValidator()
        self.run_id = uuid.uuid4()
        
        # Новый компонент для обработки таблиц
        self.processor = TableProcessor(
            self.extractor, self.loader, self.validator, self.run_id
        )
        
        self._run_stats = {
            'tables_processed': 0,
            'total_rows_synced': 0,
            'validation_errors': 0
        }
        self._table_run_details = []

    async def run(self, 
                  skip_load: bool = False, 
                  skip_transform: bool = False, 
                  full_refresh: bool = False,
                  dry_run: bool = False,
                  scope: str = 'all',
                  run_exports: bool = True):
        """Запуск ETL пайплайна."""
        self.dry_run = dry_run
        start_time = time.time()
        mode = 'полная перезагрузка' if full_refresh else 'инкрементально (CDC)'
        error_message = None
        
        log.info(f"=== Запуск ELT Пайплайна (ID: {self.run_id}) ===")
        log.info(f"Режим: {mode}, Scope: {scope}")
        
        await self._start_run('full_refresh' if full_refresh else 'cdc')
        
        try:
            if not skip_load:
                await self._run_load_phase(full_refresh, scope)
            else:
                log.info("Пропуск фазы загрузки (skip_load=True)")

            if not skip_transform:
                await self._run_transform_phase()
            else:
                log.info("Пропуск фазы трансформации (skip_transform=True)")
            
            if run_exports and not dry_run:
                await self._run_export_phase()
                
            status = 'success'
        except Exception as e:
            status = 'failed'
            error_message = str(e)
            log.critical(f"Сбой выполнения пайплайна: {e}", exc_info=True)
            raise
        finally:
            duration = time.time() - start_time
            await self._finish_run(status, duration, error_message)
            self._print_summary_table(status, duration)
            log.info(f"=== Пайплайн завершен за {duration:.2f} сек (статус: {status}) ===")

    async def _start_run(self, mode: str):
        query = "INSERT INTO elt_runs (run_id, mode, status) VALUES ($1, $2, 'running')"
        try:
            await DBConnection.execute(query, str(self.run_id), mode)
        except Exception as e:
            log.warning(f"Не удалось зарегистрировать начало запуска: {e}")

    async def _finish_run(self, status: str, duration: float, error_message: Optional[str] = None):
        query = """
            UPDATE elt_runs SET
                finished_at = NOW(), status = $2, duration_seconds = $3,
                tables_processed = $4, total_rows_synced = $5,
                validation_errors = $6, error_message = $7
            WHERE run_id = $1
        """
        try:
            await DBConnection.execute(
                query, str(self.run_id), status, round(duration, 2),
                self._run_stats['tables_processed'], self._run_stats['total_rows_synced'],
                self._run_stats['validation_errors'], error_message
            )
        except Exception as e:
            log.warning(f"Не удалось обновить статус завершения: {e}")

    async def _run_load_phase(self, full_refresh: bool, scope: str = 'all'):
        """Фаза загрузки данных из GSheets в БД."""
        dry_run_mode = getattr(self, 'dry_run', False)
        log.info(f"Начало фазы загрузки (Scope: {scope})")
        
        config = settings.sources
        if not config:
            log.warning("Конфигурация sources.yml не найдена.")
            return

        for spreadsheet_id, sdata in config.get('spreadsheets', {}).items():
            for sheet_cfg in sdata.get('sheets', []):
                target_table = sheet_cfg['target_table']
                
                # Фильтрация по scope
                if not self._is_in_scope(target_table, scope):
                    continue
                
                try:
                    # Вызов процессора для обработки конкретной таблицы
                    result = await self.processor.process_table(
                        spreadsheet_id, sheet_cfg, full_refresh, dry_run_mode
                    )
                    
                    if result.get('status') == 'skipped':
                        continue
                        
                    # Обновление статистики пайплайна
                    self._update_run_stats(result, dry_run_mode)
                    
                    if not dry_run_mode:
                        await self._log_table_stats(result)
                        
                except Exception as e:
                    log.error(f"Ошибка при обработке таблицы {target_table}: {e}")

    def _is_in_scope(self, table: str, scope: str) -> bool:
        if scope == 'all': return True
        is_cur = table.endswith('_cur')
        is_hst = table.endswith('_hst')
        is_ref = table in ('rates', 'price_reference')
        
        if scope == 'current': return is_cur
        if scope == 'historical': return is_hst or is_ref
        return True

    def _update_run_stats(self, result: Dict[str, Any], dry_run: bool):
        self._run_stats['tables_processed'] += 1
        self._run_stats['total_rows_synced'] += result.get('inserted', 0) + result.get('updated', 0)
        self._run_stats['validation_errors'] += result.get('errors', 0)
        
        self._table_run_details.append({
            'table': result['table'],
            'extracted': result.get('extracted', 0),
            'inserted': result.get('inserted', 0),
            'updated': result.get('updated', 0),
            'deleted': result.get('deleted', 0),
            'errors': result.get('errors', 0),
            'duration_s': round(result.get('duration_ms', 0) / 1000, 2)
        })

    async def _log_table_stats(self, result: Dict[str, Any]):
        query = """
            INSERT INTO elt_table_stats (
                run_id, table_name, rows_extracted, rows_inserted, 
                rows_updated, rows_deleted, validation_errors, duration_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
        try:
            await DBConnection.execute(
                query, str(self.run_id), result['table'],
                result.get('extracted', 0), result.get('inserted', 0),
                result.get('updated', 0), result.get('deleted', 0),
                result.get('errors', 0), result.get('duration_ms', 0)
            )
        except Exception as e:
            log.warning(f"Не удалось сохранить статистику таблицы {result['table']}: {e}")

    async def _run_transform_phase(self):
        log.info("Начало фазы трансформации...")
        await self.transformer.run()

    async def _run_export_phase(self):
        log.info("Начало фазы экспорта витрин...")
        datamarts = settings.sources.get('datamarts', [])
        for dm in datamarts:
            try:
                await self.exporter.export_view_to_sheet(
                    view_name=dm['view'],
                    spreadsheet_id=dm['spreadsheet_id'],
                    gid=dm['gid']
                )
            except Exception as e:
                log.error(f"Ошибка экспорта витрины {dm.get('view')}: {e}")

    def _print_summary_table(self, status: str, duration: float):
        if not self._table_run_details: return
            
        print("\n" + "="*80)
        print(f"ИТОГОВЫЙ ОТЧЕТ ELT (Run ID: {str(self.run_id)[:8]}...)")
        print(f"Статус: {status.upper()} | Длительность: {duration:.2f} сек")
        print("-" * 80)
        print(f"{'Таблица':<20} | {'Всего':<5} | {'INS':<4} | {'UPD':<4} | {'DEL':<4} | {'ERR':<4} | {'Время':<6}")
        print("-" * 80)
        
        for d in self._table_run_details:
            print(f"{d['table']:<20} | {d['extracted']:<5} | {d['inserted']:<4} | {d['updated']:<4} | {d['deleted']:<4} | {d['errors']:<4} | {d['duration_s']:>6.2f}s")
        
        print("-" * 80)
        print(f"{'ИТОГО':<20} | {sum(d['extracted'] for d in self._table_run_details):<5} | "
              f"{sum(d['inserted'] for d in self._table_run_details):<4} | "
              f"{sum(d['updated'] for d in self._table_run_details):<4} | "
              f"{sum(d['deleted'] for d in self._table_run_details):<4} | "
              f"{self._run_stats['validation_errors']:<4} | {duration:>6.2f}s")
        print("="*80 + "\n")
