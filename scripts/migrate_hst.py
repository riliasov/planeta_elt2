#!/usr/bin/env python3
"""
Быстрая миграция исторических данных из Google Sheets в Supabase.
Использует Atomic Swap для безопасной замены данных.
"""
import sys
import argparse
import asyncio
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.logger import setup_logger
from src.config.loader import load_sources_config
from src.etl.extractor import GSheetsExtractor
from src.db.connection import DBConnection

log = setup_logger()

async def get_table_count(schema: str, table: str) -> int:
    """Получает количество строк в таблице."""
    async with await DBConnection.get_connection() as conn:
        try:
            return await conn.fetchval(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        except Exception:
            return 0

async def check_duplicates(schema: str, table: str, pk_col: str = 'record_id') -> list:
    """Проверяет дубликаты по PK во временной таблице."""
    query = f"""
        SELECT "{pk_col}", COUNT(*) as cnt 
        FROM "{schema}"."{table}" 
        GROUP BY "{pk_col}" 
        HAVING COUNT(*) > 1
        LIMIT 10
    """
    async with await DBConnection.get_connection() as conn:
        return await conn.fetch(query)

async def check_hash_duplicates(schema: str, table: str) -> int:
    """Проверяет дубликаты по content_hash."""
    query = f"""
        SELECT COUNT(*) FROM (
            SELECT content_hash FROM "{schema}"."{table}" 
            GROUP BY content_hash HAVING COUNT(*) > 1
        ) sub
    """
    async with await DBConnection.get_connection() as conn:
        try:
            return await conn.fetchval(query) or 0
        except Exception:
            return 0

async def migrate_table(sheet_config: dict, extractor: GSheetsExtractor, force: bool = False):
    """Миграция одной таблицы с Atomic Swap."""
    target = sheet_config['target_table']
    schema, table = target.split('.') if '.' in target else ('public', target)
    
    log.info(f"🚀 Начало миграции: {target}")
    
    # 1. Извлекаем данные
    spreadsheet_id = sheet_config['spreadsheet_id']
    gid = sheet_config.get('gid')
    range_name = sheet_config.get('range', 'auto')
    
    log.info(f"   Извлечение данных из GSheets (gid={gid})...")
    rows, headers = await extractor.extract_sheet_data(spreadsheet_id, gid, range_name)
    source_count = len(rows)
    log.info(f"   Получено {source_count} строк")
    
    if source_count == 0:
        log.error(f"❌ Нет данных для миграции {target}")
        return False
    
    # 2. Pre-flight: сравниваем размеры
    current_count = await get_table_count(schema, table)
    if current_count > 0 and source_count < current_count * 0.8:
        if not force:
            log.error(f"❌ ВНИМАНИЕ: Новых строк ({source_count}) меньше 80% от текущих ({current_count}). Используйте --force")
            return False
        log.warning(f"⚠️ Продолжаем с --force: {source_count} vs {current_count}")
    
    # 3. Создаём временную таблицу и загружаем
    temp_table = f"{table}_new"
    backup_table = f"{table}_backup"
    
    async with await DBConnection.get_connection() as conn:
        # Удаляем старые temp/backup если есть
        await conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{temp_table}"')
        await conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{backup_table}"')
        
        # Создаём temp таблицу как копию структуры
        await conn.execute(f'CREATE TABLE "{schema}"."{temp_table}" (LIKE "{schema}"."{table}" INCLUDING ALL)')
        
        # Загружаем данные через COPY
        log.info(f"   Загрузка во временную таблицу {temp_table}...")
        col_names = [h.lower().replace(' ', '_') for h in headers]
        
        # Подготовка записей для COPY
        records = []
        for i, row in enumerate(rows):
            # Выравниваем строку
            if len(row) < len(col_names):
                row = row + [None] * (len(col_names) - len(row))
            records.append(tuple(row[:len(col_names)]))
        
        await conn.copy_records_to_table(
            temp_table,
            records=records,
            columns=col_names,
            schema_name=schema
        )
        log.info(f"   ✓ Загружено {len(records)} записей")
    
    # 4. Проверка дубликатов record_id
    pk_col = sheet_config.get('pk', 'record_id')
    duplicates = await check_duplicates(schema, temp_table, pk_col)
    if duplicates:
        log.error(f"❌ Найдены дубликаты {pk_col}: {[d[pk_col] for d in duplicates[:5]]}")
        async with await DBConnection.get_connection() as conn:
            await conn.execute(f'DROP TABLE "{schema}"."{temp_table}"')
        return False
    
    # 5. Проверка дубликатов hash (предупреждение)
    hash_dups = await check_hash_duplicates(schema, temp_table)
    if hash_dups > 0:
        log.warning(f"⚠️ Найдено {hash_dups} дубликатов content_hash (возможно, это нормально)")
    
    # 6. Atomic Swap
    log.info(f"   Выполнение atomic swap...")
    async with await DBConnection.get_connection() as conn:
        async with conn.transaction():
            # Rename old -> backup
            await conn.execute(f'ALTER TABLE "{schema}"."{table}" RENAME TO "{backup_table}"')
            # Rename new -> current
            await conn.execute(f'ALTER TABLE "{schema}"."{temp_table}" RENAME TO "{table}"')
    
    log.info(f"✅ Миграция {target} завершена! ({source_count} строк)")
    log.info(f"   Бэкап сохранён в {schema}.{backup_table}")
    return True

async def main():
    parser = argparse.ArgumentParser(description='Быстрая миграция исторических данных')
    parser.add_argument('--sheets', required=True, help='Список листов через запятую (например: sales_hst,clients_hst)')
    parser.add_argument('--confirm', action='store_true', help='Подтверждение операции')
    parser.add_argument('--force', action='store_true', help='Игнорировать предупреждение о размере')
    args = parser.parse_args()
    
    if not args.confirm:
        print("❌ Требуется подтверждение: --confirm")
        print("   Эта операция заменит данные в указанных таблицах.")
        sys.exit(1)
    
    sheets_to_migrate = [s.strip() for s in args.sheets.split(',')]
    log.info(f"📋 Миграция листов: {sheets_to_migrate}")
    
    # Загружаем конфигурацию
    sources = load_sources_config()
    extractor = GSheetsExtractor()
    
    # Находим конфигурации для указанных листов
    results = []
    for spreadsheet_id, config in sources.items():
        if not isinstance(config, dict) or 'sheets' not in config:
            continue
        for sheet in config['sheets']:
            sheet_id = sheet.get('id', '')
            if sheet_id in sheets_to_migrate or sheet.get('target_table', '').endswith(tuple(sheets_to_migrate)):
                sheet['spreadsheet_id'] = spreadsheet_id
                success = await migrate_table(sheet, extractor, force=args.force)
                results.append((sheet_id, success))
    
    # Итоги
    log.info("=" * 50)
    log.info("ИТОГИ МИГРАЦИИ:")
    for sheet_id, success in results:
        status = "✅" if success else "❌"
        log.info(f"  {status} {sheet_id}")
    
    await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
