#!/usr/bin/env python3
"""
Скрипт для проверки целостности схемы данных: Контракты (JSON) vs База данных (Supabase).
Проверяет, что все колонки из контракта существуют в БД и имеют правильные названия (English).
"""
import sys
import yaml
import json
import asyncio
import logging
from pathlib import Path
from src.db.connection import DBConnection
from src.etl.validator import ContractValidator

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
log = logging.getLogger('integrity_check')

async def get_db_columns(schema: str, table: str):
    """Получает список колонок таблицы из БД."""
    query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = $1 AND table_name = $2
    """
    async with await DBConnection.get_connection() as conn:
        return await conn.fetch(query, schema, table)

async def verify_integrity():
    root_dir = Path(__file__).resolve().parent.parent
    sources_path = root_dir / 'sources.yml'
    contracts_dir = root_dir / 'src' / 'contracts'
    
    if not sources_path.exists():
        log.error(f"sources.yml not found at {sources_path}")
        return

    with open(sources_path, 'r', encoding='utf-8') as f:
        sources = yaml.safe_load(f)

    validator = ContractValidator(contracts_dir)
    
    all_ok = True
    log.info("🔍 Запуск проверки целостности колонок (Contract vs DB)...")
    
    for spreadsheet_id, config in sources.items():
        if not isinstance(config, dict) or 'sheets' not in config:
            continue
            
        for sheet in config['sheets']:
            target_table = sheet['target_table']
            if '.' not in target_table:
                schema, table = 'public', target_table
            else:
                schema, table = target_table.split('.', 1)
            
            # Определяем имя контракта
            table_base = table.replace('_cur', '').replace('_hst', '')
            contract_name = 'schedule' if table_base == 'trainings' else table_base
            
            log.info(f"\n--- Таблица: {target_table} (Контракт: {contract_name}) ---")
            
            try:
                contract = validator.load_contract(contract_name)
                contract_cols = {c['name'].lower() for c in contract.get('columns', [])}
                # Добавляем CDC колонки, которые должны быть в БД
                contract_cols.update({'record_id', 'content_hash', 'created_at', 'updated_at', 'updated_by', '_row_index', '__row_hash'})
            except FileNotFoundError:
                log.warning(f"⚠️ Контракт {contract_name} не найден. Пропускаю.")
                continue

            db_rows = await get_db_columns(schema, table)
            if not db_rows:
                log.error(f"❌ Таблица {target_table} не найдена в базе данных!")
                all_ok = False
                continue

            db_cols = {r['column_name'].lower(): r['data_type'] for r in db_rows}
            
            # 1. Проверка отсутствующих колонок
            missing = contract_cols - set(db_cols.keys())
            if missing:
                log.error(f"❌ ОТСУТСТВУЮТ в БД: {missing}")
                all_ok = False
            else:
                log.info("✅ Все колонки из контракта присутствуют в БД.")

            # 2. Проверка лишних колонок (не в контракте и не CDC)
            extra = set(db_cols.keys()) - contract_cols
            if extra:
                log.warning(f"⚠️ ЛИШНИЕ колонки в БД (не в контракте): {extra}")
            
    if all_ok:
        log.info("\n✨ ИТОГ: Целостность названий колонок подтверждена!")
    else:
        log.error("\n❌ ИТОГ: Обнаружены расхождения в схеме данных.")

if __name__ == "__main__":
    asyncio.run(verify_integrity())
