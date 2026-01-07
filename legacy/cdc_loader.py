"""Инкрементальный загрузчик с CDC (Change Data Capture)."""

import gspread
import yaml
import json
import asyncio
import asyncpg
import re
import logging
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import os
from src.cdc import compute_row_hash, CDCProcessor, fetch_existing_hashes

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(levelname)s | %(name)s | %(message)s')
log = logging.getLogger('cdc_loader')


def slugify(text: str) -> str:
    """Преобразует текст в snake_case."""
    text = text.lower()
    text = re.sub(r'[\s\n]+', '_', text)
    text = re.sub(r'[^\w_]+', '', text)
    return text.strip('_')


class CDCStats:
    """Статистика CDC загрузки."""
    def __init__(self, table: str):
        self.table = table
        self.inserted = 0
        self.updated = 0
        self.deleted = 0
        self.unchanged = 0
        self.errors = 0
    
    def summary(self) -> str:
        return f"{self.table}: +{self.inserted} ~{self.updated} -{self.deleted} ={self.unchanged} !{self.errors}"


async def apply_cdc_changes(conn, table: str, processor: CDCProcessor, col_names: list) -> CDCStats:
    """Применяет изменения CDC к таблице."""
    stats = CDCStats(table)
    
    # INSERT новые записи
    if processor.to_insert:
        for item in processor.to_insert:
            try:
                data = item['data']
                cols = ', '.join([f'"{c}"' for c in col_names] + ['"_row_index"', '"__row_hash"'])
                placeholders = ', '.join([f'${i+1}' for i in range(len(col_names) + 2)])
                values = [data.get(c) for c in col_names] + [data.get('_row_index'), item['hash']]
                
                query = f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})'
                await conn.execute(query, *values)
                stats.inserted += 1
            except Exception as e:
                log.error(f"INSERT error: {e}")
                stats.errors += 1
    
    # UPDATE изменённые записи
    if processor.to_update:
        for item in processor.to_update:
            try:
                data = item['data']
                pk_col = 'pk' if 'pk' in col_names else col_names[0]  # Предполагаем pk
                pk_value = data.get(pk_col)
                
                if not pk_value:
                    continue
                
                set_clauses = []
                values = []
                idx = 1
                for c in col_names:
                    if c != pk_col:
                        set_clauses.append(f'"{c}" = ${idx}')
                        values.append(data.get(c))
                        idx += 1
                
                # Добавляем hash
                set_clauses.append(f'"__row_hash" = ${idx}')
                values.append(item['hash'])
                idx += 1
                
                # WHERE
                values.append(pk_value)
                
                query = f'UPDATE "{table}" SET {", ".join(set_clauses)} WHERE "{pk_col}" = ${idx}'
                await conn.execute(query, *values)
                stats.updated += 1
            except Exception as e:
                log.error(f"UPDATE error: {e}")
                stats.errors += 1
    
    # DELETE удалённые записи (опционально, можно отключить)
    # if processor.to_delete:
    #     for pk in processor.to_delete:
    #         try:
    #             await conn.execute(f'DELETE FROM "{table}" WHERE "pk" = $1', pk)
    #             stats.deleted += 1
    #         except Exception as e:
    #             log.error(f"DELETE error: {e}")
    #             stats.errors += 1
    
    stats.unchanged = processor.unchanged
    stats.deleted = len(processor.to_delete)  # Только логируем, не удаляем
    
    return stats


async def load_with_cdc():
    """Загрузка данных с CDC (инкрементальная)."""
    
    with open('sources.yml', 'r') as f:
        config = yaml.safe_load(f)
    
    creds_path = config.get('google_credentials', 'secrets/google-service-account.json')
    with open(creds_path, 'r') as f:
        creds_info = json.load(f)
    
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)
    
    dsn = os.getenv('SUPABASE_DB_URL')
    if not dsn:
        log.error("SUPABASE_DB_URL не найден")
        return
    
    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    all_stats: list[CDCStats] = []
    
    try:
        for ssid, sdata in config.get('spreadsheets', {}).items():
            log.info(f"Открываю таблицу {ssid}...")
            sh = gc.open_by_key(ssid)
            
            for sheet_cfg in sdata.get('sheets', []):
                target_table = sheet_cfg['target_table']
                gid = sheet_cfg.get('gid', 0)
                data_range = sheet_cfg.get('range', 'A:Z')
                
                log.info(f"CDC загрузка: {target_table} (gid={gid})")
                
                try:
                    ws = sh.get_worksheet_by_id(gid)
                    all_values = ws.get(data_range)
                except Exception as e:
                    log.error(f"Ошибка чтения {target_table}: {e}")
                    continue
                
                if not all_values or len(all_values) < 2:
                    log.warning(f"{target_table}: нет данных")
                    continue
                
                headers = all_values[0]
                rows = all_values[1:]
                
                # Нормализуем заголовки
                col_names = []
                seen = set()
                for h in headers:
                    col_name = slugify(h) or "unknown_col"
                    original = col_name
                    counter = 1
                    while col_name in seen:
                        col_name = f"{original}_{counter}"
                        counter += 1
                    seen.add(col_name)
                    col_names.append(col_name)
                
                # Получаем существующие хеши из БД
                existing_hashes = await fetch_existing_hashes(conn, target_table)
                log.info(f"Существующих записей: {len(existing_hashes)}")
                
                # Обрабатываем строки через CDC
                processor = CDCProcessor(existing_hashes.copy())
                
                for idx, row in enumerate(rows):
                    row_num = idx + 2
                    
                    # Выравниваем строку
                    full_row = list(row) + [None] * (len(col_names) - len(row))
                    full_row = full_row[:len(col_names)]
                    full_row = [str(val) if val not in (None, '') else None for val in full_row]
                    
                    # Вычисляем хеш
                    row_hash = compute_row_hash(full_row)
                    
                    # Ищем primary key (pk колонка)
                    pk_idx = col_names.index('pk') if 'pk' in col_names else None
                    legacy_id = full_row[pk_idx] if pk_idx is not None else str(row_num)
                    
                    if not legacy_id:
                        continue
                    
                    # Данные строки
                    row_data = {col_names[i]: full_row[i] for i in range(len(col_names))}
                    row_data['_row_index'] = row_num
                    
                    processor.process_row(legacy_id, row_hash, row_data)
                
                processor.finalize()
                
                # Логируем что нашли
                cdc_stats = processor.get_stats()
                log.info(f"CDC: insert={cdc_stats['insert']} update={cdc_stats['update']} delete={cdc_stats['delete']} unchanged={cdc_stats['unchanged']}")
                
                # Применяем изменения
                stats = await apply_cdc_changes(conn, target_table, processor, col_names)
                all_stats.append(stats)
                log.info(stats.summary())
        
        # Итоги
        log.info("=" * 50)
        log.info("CDC ЗАГРУЗКА ЗАВЕРШЕНА")
        for s in all_stats:
            log.info(s.summary())
            
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(load_with_cdc())
