"""Главный скрипт ELT-пайплайна: загрузка из Sheets → трансформация → public таблицы."""

import asyncio
import argparse
import logging
import sys
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s | %(name)s | %(message)s'
)
log = logging.getLogger('pipeline')


async def run_pipeline(skip_load: bool = False, skip_transform: bool = False):
    """Запуск полного ELT-пайплайна."""
    
    start_time = datetime.now()
    log.info("=" * 60)
    log.info(f"ЗАПУСК ELT PIPELINE: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)
    
    # Этап 1: Загрузка из Sheets в staging
    if not skip_load:
        log.info("")
        log.info("ЭТАП 1: Загрузка данных из Google Sheets")
        log.info("-" * 40)
        try:
            # Импортируем здесь, чтобы не грузить модули если skip_load
            from fast_loader import load_data
            await load_data()
            log.info("✓ Загрузка завершена")
        except Exception as e:
            log.error(f"✗ Ошибка загрузки: {e}")
            return False
    else:
        log.info("ЭТАП 1: Пропущен (--skip-load)")
    
    # Этап 2: Трансформация staging → public
    if not skip_transform:
        log.info("")
        log.info("ЭТАП 2: Трансформация данных в public таблицы")
        log.info("-" * 40)
        try:
            from transform_to_public import run_transforms
            await run_transforms()
            log.info("✓ Трансформация завершена")
        except Exception as e:
            log.error(f"✗ Ошибка трансформации: {e}")
            return False
    else:
        log.info("ЭТАП 2: Пропущен (--skip-transform)")
    
    # Итоги
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    log.info("")
    log.info("=" * 60)
    log.info(f"PIPELINE ЗАВЕРШЕН за {duration:.1f} секунд")
    log.info("=" * 60)
    
    return True


def main():
    parser = argparse.ArgumentParser(description='ELT Pipeline: Sheets → Supabase')
    parser.add_argument('--skip-load', action='store_true', 
                        help='Пропустить загрузку из Sheets')
    parser.add_argument('--skip-transform', action='store_true',
                        help='Пропустить трансформацию')
    parser.add_argument('--transform-only', action='store_true',
                        help='Только трансформация (без загрузки)')
    
    args = parser.parse_args()
    
    skip_load = args.skip_load or args.transform_only
    skip_transform = args.skip_transform
    
    success = asyncio.run(run_pipeline(skip_load=skip_load, skip_transform=skip_transform))
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
