import logging
import time
import uuid
from typing import Optional, List, Dict, Any
from src.config.settings import settings
from src.etl.extractor import GSheetsExtractor
from src.etl.loader import DataLoader
from src.etl.transformer import Transformer
from src.etl.exporter import DataMartExporter
from src.etl.validator import ContractValidator, ValidationResult
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
        """Запуск ETL пайплайна.
        
        Args:
            skip_load: Пропустить фазу загрузки
            skip_transform: Пропустить фазу трансформации  
            full_refresh: Полная перезагрузка (TRUNCATE + INSERT)
            dry_run: Показать изменения без применения
            scope: Область синхронизации (all, current, historical)
        """
        self.dry_run = dry_run
        start_time = time.time()
        mode = 'полная перезагрузка' if full_refresh else 'инкрементально (CDC)'
        error_message = None
        
        log.info(f"=== Запуск ELT Пайплайна (ID: {self.run_id}) ===")
        log.info(f"Режим: {mode}, Scope: {scope}")
        
        # Регистрируем начало run
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
            
            # 4. Экспорт витрин
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
        """Регистрирует начало run в elt_runs."""
        query = """
            INSERT INTO elt_runs (run_id, mode, status)
            VALUES ($1, $2, 'running')
        """
        try:
            await DBConnection.execute(query, str(self.run_id), mode)
        except Exception as e:
            log.warning(f"Не удалось зарегистрировать начало запуска: {e}")

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
            log.warning(f"Не удалось обновить статус завершения: {e}")

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
            log.warning(f"Не удалось сохранить статистику таблицы {table_name}: {e}")

    async def _run_load_phase(self, full_refresh: bool, scope: str = 'all'):
        dry_run_mode = getattr(self, 'dry_run', False)
        mode_label = ' [DRY-RUN]' if dry_run_mode else ''
        log.info(f"Начало фазы загрузки{mode_label} (Тип: {'Full Refresh' if full_refresh else 'CDC'})")
        
        config = settings.sources
        if not config:
            log.warning("Конфигурация sources.yml не найдена.")
            return

        for spreadsheet_id, sdata in config.get('spreadsheets', {}).items():
            for sheet_cfg in sdata.get('sheets', []):
                target_table = sheet_cfg['target_table']
                
                # Фильтрация по scope
                is_cur = target_table.endswith('_cur')
                is_hst = target_table.endswith('_hst')
                is_ref = target_table in ('rates', 'price_reference')
                
                if scope == 'current' and not is_cur:
                    continue
                if scope == 'historical' and not (is_hst or is_ref):
                    continue
                
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
                    mapping = sheet_cfg.get('column_mapping')
                    
                    # 1. Извлечение
                    col_names, rows = await self.extractor.extract_sheet_data(
                        spreadsheet_id, str(gid), range_name, target_table, 
                        mapping=mapping
                    )
                    
                    if not rows:
                        log.info(f"Таблица {target_table}: данных не обнаружено, пропуск.")
                        continue
                        
                    # 1.5. Сохранение сырых данных (Audit Trace)
                    await self._dump_raw_data(spreadsheet_id, target_table, col_names, rows)

                    # 2. Валидация
                    log.info(f"Проверка контракта '{contract_name}' для таблицы {target_table}...")
                    dict_rows = [dict(zip(col_names, row)) for row in rows]
                    val_result = self.validator.validate_dataset(dict_rows, contract_name)
                    
                    if not val_result.is_valid:
                        validation_errors = len(val_result.errors)
                        log.warning(f"⚠ {target_table}: обнаружено {validation_errors} ошибок валидации")
                        
                        if not dry_run_mode:
                            await self._log_validation_errors(target_table, val_result)

                        # Проверка порогов ошибок
                        # 1. Слишком много ошибок всего
                        if validation_errors > 20:
                             raise ValueError(f"КРИТИЧНО: {validation_errors} ошибок валидации в {target_table} (> 20). Прерывание.")
                        
                        # 2. Слишком много ошибок в одной строке (битая строка)
                        errors_by_row = {}
                        for err in val_result.errors:
                            errors_by_row[err.row_index] = errors_by_row.get(err.row_index, 0) + 1
                        
                        if any(count > 5 for count in errors_by_row.values()):
                             raise ValueError(f"КРИТИЧНО: Найдены строки с >5 ошибками в {target_table}. Возможно, битый формат строк.")

                    else:
                        log.info(f"✓ {target_table}: валидация пройдена успешно")

                    pk_field = sheet_cfg.get('pk', '__row_hash')

                    # 3. Загрузка
                    if dry_run_mode:
                        load_stats = await self.loader.calculate_changes(target_table, col_names, rows, pk_field)
                        log.info(f"🔍 [DRY-RUN] {target_table}: "
                                f"было бы: дозапись={load_stats.get('insert', 0)}, "
                                f"обновление={load_stats.get('update', 0)}, "
                                f"удаление={load_stats.get('delete', 0)}, "
                                f"без изменений={load_stats.get('unchanged', 0)}")
                    elif is_full_refresh:
                        load_stats = await self.loader.load_full_refresh(target_table, col_names, rows)
                    else:
                        load_stats = await self.loader.load_cdc(target_table, col_names, rows, pk_field)
                    
                    # Статистика
                    load_stats['extracted'] = len(rows)
                    duration_ms = int((time.time() - table_start) * 1000)
                    
                    self._run_stats['tables_processed'] += 1
                    self._run_stats['total_rows_synced'] += load_stats.get('inserted', 0) + load_stats.get('updated', 0)
                    self._run_stats['validation_errors'] += validation_errors
                    
                    self._table_run_details.append({
                        'table': target_table,
                        'extracted': load_stats.get('extracted', 0),
                        'inserted': load_stats.get('inserted', 0),
                        'updated': load_stats.get('updated', 0),
                        'deleted': load_stats.get('deleted', 0),
                        'errors': validation_errors,
                        'duration_s': round(duration_ms / 1000, 2)
                    })
                    
                    if not dry_run_mode:
                        await self._log_table_stats(target_table, load_stats, validation_errors, duration_ms)
                        
                except Exception as e:
                    log.error(f"Ошибка при обработке таблицы {target_table}: {e}")

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
            log.info(f"Сохранено {len(params)} ошибок валидации в БД.")
        except Exception as e:
            log.error(f"Не удалось сохранить логи ошибок валидации: {e}")

    async def _dump_raw_data(self, spreadsheet_id: str, sheet_name: str, col_names: list, rows: list):
        """Сохраняет сырые данные в схему raw."""
        import json
        query = """
            INSERT INTO raw.sheets_dump (spreadsheet_id, sheet_name, data)
            VALUES ($1, $2, $3)
        """
        try:
            # Превращаем в список диктов для JSONB
            data_to_dump = [dict(zip(col_names, row)) for row in rows[:1000]] # Ограничим для лога если слишком много? 
            # На самом деле лучше всё, но jsonb имеет пределы. Sheets редко > 100mb.
            # Для аудита обычно достаточно заголовков и первых строк, но в идеале всё.
            full_data = json.dumps([dict(zip(col_names, row)) for row in rows], ensure_ascii=False)
            
            await DBConnection.execute(query, spreadsheet_id, sheet_name, full_data)
            log.debug(f"Сырые данные для {sheet_name} сохранены в raw.sheets_dump")
        except Exception as e:
            log.warning(f"Не удалось сохранить сырые данные для {sheet_name}: {e}")

    async def _run_transform_phase(self):
        log.info("Начало фазы трансформации...")
        await self.transformer.run()

    async def _run_export_phase(self):
        """Экспорт аналитических витрин."""
        log.info("Начало фазы экспорта витрин...")
        
        # Получаем конфиг витрин из settings (добавим его в sources.yml)
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
        """Выводит итоговую таблицу в консоль."""
        if not self._table_run_details:
            return
            
        print("\n" + "="*80)
        print(f"ИТОГОВЫЙ ОТЧЕТ ELT (Run ID: {str(self.run_id)[:8]}...)")
        print(f"Статус: {status.upper()} | Длительность: {duration:.2f} сек")
        print("-" * 80)
        # Более компактный формат
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
