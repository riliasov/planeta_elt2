"""Трансформация данных из staging (*_cur, *_hst) в public таблицы.

Стратегия: Unified Target Table + Soft Delete.
Использует внешние SQL файлы для трансформации и очистки.
"""
import logging
from pathlib import Path
from src.db.connection import DBConnection

log = logging.getLogger('transformer')

SQL_DIR = Path(__file__).parent.parent / 'db' / 'sql'

class Transformer:
    """Выполняет SQL-трансформации из staging в public таблицы."""
    
    async def run(self, tables: list[str] = None):
        """Запускает трансформации."""
        log.info("Starting transformations...")
        
        # Порядок важен: Clients -> Schedule -> Sales (dependencies)
        # Но у нас жестко заданы SQL файлы
        files_to_run = [
            'transform_clients.sql',
            'transform_schedule.sql', 
            'transform_sales.sql'
        ]
        
        success_count = 0
        
        for filename in files_to_run:
            file_path = SQL_DIR / filename
            if not file_path.exists():
                log.error(f"SQL file not found: {file_path}")
                continue
                
            try:
                log.info(f"Executing {filename}...")
                with open(file_path, 'r', encoding='utf-8') as f:
                    sql = f.read()
                    
                await DBConnection.execute(sql)
                log.info(f"✓ Successfully executed {filename}")
                success_count += 1
                
            except Exception as e:
                log.error(f"✗ Failed to execute {filename}: {e}")
                # Если упали clients, то sales тоже могут упасть. Но пробуем дальше.

        # Запуск Cleanup / Soft Delete
        try:
            log.info("Running cleanup (soft delete)...")
            cleanup_path = SQL_DIR / 'cleanup.sql'
            if cleanup_path.exists():
                with open(cleanup_path, 'r', encoding='utf-8') as f:
                    sql = f.read()
                await DBConnection.execute(sql)
                log.info("✓ Cleanup completed")
            else:
                log.warning("cleanup.sql not found")
        except Exception as e:
             log.error(f"✗ Cleanup failed: {e}")

        log.info(f"Transformation finished. Scripts executed: {success_count}/{len(files_to_run)}")
        return success_count, 0

async def run_all_transformations():
    """Утилита для запуска всех трансформаций."""
    transformer = Transformer()
    return await transformer.run()
