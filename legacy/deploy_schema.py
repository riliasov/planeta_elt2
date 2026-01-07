"""Развертывание staging-таблиц в Supabase на основе заголовков из Google Sheets."""

import json
import asyncio
import asyncpg
import re
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s | %(name)s | %(message)s'
)
log = logging.getLogger('deploy_schema')


def slugify(text: str) -> str:
    """Преобразует текст заголовка в имя колонки (snake_case, русские буквы сохраняются)."""
    text = text.lower()
    text = re.sub(r'[\s\n]+', '_', text)
    text = re.sub(r'[^\w_]+', '', text)
    return text.strip('_')


async def deploy():
    """Создаёт staging-таблицы на основе headers.json."""
    
    # Загружаем заголовки
    headers_path = 'headers.json'
    if not os.path.exists(headers_path):
        log.error(f"Файл {headers_path} не найден. Сначала запустите get_headers.py")
        return
    
    with open(headers_path, 'r') as f:
        headers_map = json.load(f)
    
    dsn = os.getenv('SUPABASE_DB_URL')
    if not dsn:
        log.error("SUPABASE_DB_URL не найден в .env")
        return
    
    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    created_tables = []
    
    try:
        for table_name, headers in headers_map.items():
            if not headers:
                log.warning(f"Пропускаем {table_name}: нет заголовков")
                continue
            
            # Формируем колонки
            if table_name == 'rates':
                # Особый случай для rates — даты в заголовках
                cols = [f'"col_{i}" text' for i, _ in enumerate(headers)]
            else:
                cols = []
                seen = set()
                for h in headers:
                    col_name = slugify(h) or "unknown_col"
                    original_name = col_name
                    counter = 1
                    while col_name in seen:
                        col_name = f"{original_name}_{counter}"
                        counter += 1
                    seen.add(col_name)
                    cols.append(f'"{col_name}" text')
            
            # Служебные колонки для CDC и отладки
            cols.append('"_row_index" integer')
            cols.append('"__row_hash" text')  # Для CDC
            cols.append('"_loaded_at" timestamp with time zone default now()')
            
            # Создаём таблицу
            sql = f'DROP TABLE IF EXISTS "{table_name}"; CREATE TABLE "{table_name}" ({", ".join(cols)});'
            
            log.info(f"Создаю таблицу {table_name} ({len(headers)} колонок)...")
            await conn.execute(sql)
            created_tables.append(table_name)
        
        log.info("=" * 50)
        log.info(f"Создано таблиц: {len(created_tables)}")
        for t in created_tables:
            log.info(f"  - {t}")
            
    except Exception as e:
        log.error(f"Ошибка при создании таблиц: {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(deploy())
