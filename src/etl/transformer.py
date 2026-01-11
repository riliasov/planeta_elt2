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
        log.info("Начало этапа трансформации данных...")
        
        # Порядок важен: Clients -> Schedule -> Sales (dependencies)
        files_to_run = [
            'transform_clients.sql',
            'transform_schedule.sql', 
            'transform_sales.sql'
        ]
        
        success_count = 0
        
        for filename in files_to_run:
            file_path = SQL_DIR / filename
            if not file_path.exists():
                log.error(f"SQL файл не найден: {file_path}")
                continue
                
            try:
                log.info(f"Выполнение {filename}...")
                with open(file_path, 'r', encoding='utf-8') as f:
                    sql = f.read()
                    
                await DBConnection.execute(sql)
                log.info(f"✓ {filename} успешно выполнен")
                success_count += 1
                
            except Exception as e:
                log.error(f"✗ Ошибка при выполнении {filename}: {e}")

        # Запуск Cleanup / Soft Delete
        try:
            log.info("Запуск очистки (soft delete)...")
            cleanup_path = SQL_DIR / 'cleanup.sql'
            if cleanup_path.exists():
                with open(cleanup_path, 'r', encoding='utf-8') as f:
                    sql = f.read()
                await DBConnection.execute(sql)
                log.info("✓ Очистка завершена")
            else:
                log.warning("Файл cleanup.sql не найден")
        except Exception as e:
             log.error(f"✗ Ошибка при очистке: {e}")

        log.info(f"Трансформация завершена. Скриптов выполнено: {success_count}/{len(files_to_run)}")
        return success_count, 0

async def run_all_transformations():
    """Утилита для запуска всех трансформаций."""
    transformer = Transformer()
    return await transformer.run()
