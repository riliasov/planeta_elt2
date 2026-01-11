import asyncio
import argparse
import sys
from src.utils.logger import setup_logger
from src.utils.process import ProcessLock
from src.etl.pipeline import ELTPipeline
from src.config.settings import settings
from src.db.connection import DBConnection

log = setup_logger()

async def main():
    parser = argparse.ArgumentParser(description='Planeta ELT Pipeline')
    parser.add_argument('--skip-load', action='store_true', help='Пропустить фазу загрузки данных')
    parser.add_argument('--skip-transform', action='store_true', help='Пропустить фазу трансформации')
    parser.add_argument('--transform-only', action='store_true', help='Только трансформация (пропустить загрузку)')
    parser.add_argument('--full-refresh', action='store_true', help='Полная перезагрузка (TRUNCATE + INSERT)')
    parser.add_argument('--deploy-schema', action='store_true', help='Пересоздать схему таблиц из заголовков Sheets')
    parser.add_argument('--dry-run', action='store_true', help='Режим просмотра изменений без применения')
    
    # Новые аргументы
    parser.add_argument('--scope', choices=['current', 'historical', 'all'], default='all', 
                        help='Область синхронизации: current (текущие), historical (история), all (все)')
    parser.add_argument('--kill-conflicts', action='store_true', 
                        help='Принудительно завершить другие запущенные процессы ELT перед стартом')
    parser.add_argument('--wait', type=int, default=0,
                        help='Время ожидания освобождения блокировки в секундах (по умолчанию 0 - ошибка сразу)')
    parser.add_argument('--skip-export', action='store_true', help='Пропустить фазу экспорта витрин')
    
    args = parser.parse_args()
    
    # Инициализация защиты от параллельных запусков
    lock = ProcessLock(name=f"elt_{args.scope}")
    lock.check_and_lock(kill_conflicts=args.kill_conflicts, timeout=args.wait)
    
    # Нормализация логики
    skip_load = args.skip_load or args.transform_only
    
    try:
        if args.deploy_schema:
            from src.etl.schema import SchemaManager
            manager = SchemaManager()
            log.info("Начало развертывания мета-таблиц...")
            await manager.deploy_meta_tables()
            log.info("Начало развертывания staging-таблиц...")
            await manager.deploy_staging_tables(use_staging_schema=settings.use_staging_schema)
            if not skip_load:
                 args.full_refresh = True
        
        pipeline = ELTPipeline()
        await pipeline.run(
            skip_load=skip_load,
            skip_transform=args.skip_transform,
            full_refresh=args.full_refresh,
            dry_run=args.dry_run,
            scope=args.scope,
            run_exports=not args.skip_export
        )
    except Exception as e:
        log.critical(f"Критический сбой пайплайна: {e}", exc_info=True)
        sys.exit(1)
    finally:
        await DBConnection.close()
        lock.unlock()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Завершение по требованию пользователя (KeyboardInterrupt)")
        sys.exit(0)
