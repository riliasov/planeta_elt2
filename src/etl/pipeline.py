import logging
import time
from typing import Optional
from src.config.settings import settings
from src.etl.extractor import GSheetsExtractor
from src.etl.loader import DataLoader
from src.etl.transformer import Transformer

log = logging.getLogger('pipeline')

class ELTPipeline:
    def __init__(self):
        self.extractor = GSheetsExtractor()
        self.loader = DataLoader()
        self.transformer = Transformer()

    async def run(self, 
                  skip_load: bool = False, 
                  skip_transform: bool = False, 
                  full_refresh: bool = False):
        """
        Запуск ETL пайплайна.
        :param skip_load: Пропустить этап загрузки (только трансформация)
        :param skip_transform: Пропустить этап трансформации
        :param full_refresh: Использовать Full Refresh вместо CDC
        """
        start_time = time.time()
        log.info("=== Starting ELT Pipeline ===")
        
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
                
                # Если в конфиге явно сказано replace, то всегда Full Refresh для этой таблицы
                # Или если передан глобальный флаг full_refresh
                is_full_refresh = full_refresh or (mode == 'replace')
                
                try:
                    # 1. Extract
                    col_names, rows = await self.extractor.extract_sheet_data(
                        spreadsheet_id, str(gid), range_name, target_table
                    )
                    
                    if not rows:
                        continue
                        
                    # 2. Load
                    if is_full_refresh:
                        await self.loader.load_full_refresh(target_table, col_names, rows)
                    else:
                        await self.loader.load_cdc(target_table, col_names, rows)
                        
                except Exception as e:
                    log.error(f"Failed to process {target_table}: {e}")

    async def _run_transform_phase(self):
        log.info("Starting Transform Phase")
        await self.transformer.run()
