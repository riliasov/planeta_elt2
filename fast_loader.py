"""Быстрая загрузка данных из Google Sheets в Supabase (Full Refresh с skip + log)."""

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
from datetime import datetime
from src.cdc import compute_row_hash

load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s | %(name)s | %(message)s'
)
log = logging.getLogger('fast_loader')


def slugify(text: str) -> str:
    """Преобразует текст в snake_case для имён колонок."""
    text = text.lower()
    text = re.sub(r'[\s\n]+', '_', text)
    text = re.sub(r'[^\w_]+', '', text)
    return text.strip('_')


class LoadStats:
    """Статистика загрузки для отчёта."""
    def __init__(self, table: str):
        self.table = table
        self.total_rows = 0
        self.loaded_rows = 0
        self.skipped_rows: list[dict] = []
    
    def skip(self, row_idx: int, reason: str):
        self.skipped_rows.append({'row': row_idx, 'reason': reason})
    
    def report(self):
        log.info(f"{self.table}: загружено {self.loaded_rows}/{self.total_rows} строк")
        if self.skipped_rows:
            log.warning(f"{self.table}: пропущено {len(self.skipped_rows)} строк")
            for s in self.skipped_rows[:5]:  # Показываем первые 5
                log.warning(f"  Строка {s['row']}: {s['reason']}")
            if len(self.skipped_rows) > 5:
                log.warning(f"  ... и ещё {len(self.skipped_rows) - 5} строк")


def validate_row(row: list, col_names: list[str], row_idx: int) -> tuple[bool, str]:
    """Валидация строки перед загрузкой. Возвращает (is_valid, error_message)."""
    # Пустая строка — пропускаем
    if not row or all(cell == '' or cell is None for cell in row):
        return False, "Пустая строка"
    
    # Можно добавить дополнительные проверки здесь
    return True, ""


async def load_data():
    """Основная функция загрузки данных."""
    with open('sources.yml', 'r') as f:
        config = yaml.safe_load(f)
    
    with open('secrets/google-service-account.json', 'r') as f:
        creds_info = json.load(f)
    
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)
    
    dsn = os.getenv('SUPABASE_DB_URL')
    if not dsn:
        log.error("SUPABASE_DB_URL не найден в .env")
        return
    
    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    all_stats: list[LoadStats] = []
    
    try:
        for ssid, sdata in config.get('spreadsheets', {}).items():
            log.info(f"Открываю таблицу {ssid}...")
            sh = gc.open_by_key(ssid)
            
            for sheet_cfg in sdata.get('sheets', []):
                target_table = sheet_cfg['target_table']
                range_name = sheet_cfg['range']
                gid = sheet_cfg['gid']
                
                stats = LoadStats(target_table)
                all_stats.append(stats)
                
                log.info(f"Загружаю {target_table} из листа {gid}...")
                
                try:
                    worksheet = sh.get_worksheet_by_id(int(gid))
                except Exception as e:
                    log.error(f"Не удалось открыть лист {gid}: {e}")
                    continue
                
                # Получаем данные
                data = worksheet.get(range_name)
                if not data:
                    log.warning(f"Нет данных для {target_table}")
                    continue
                
                headers = data[0]
                rows = data[1:]
                stats.total_rows = len(rows)
                
                # Нормализуем заголовки
                if target_table == 'rates':
                    col_names = [f"col_{i}" for i, _ in enumerate(headers)]
                else:
                    seen = set()
                    col_names = []
                    for h in headers:
                        col_name = slugify(h) or "unknown_col"
                        original_name = col_name
                        counter = 1
                        while col_name in seen:
                            col_name = f"{original_name}_{counter}"
                            counter += 1
                        seen.add(col_name)
                        col_names.append(col_name)
                
                # Truncate таблицу
                await conn.execute(f'TRUNCATE TABLE "{target_table}"')
                
                # Подготовка строк с валидацией
                prepared_rows = []
                for idx, r in enumerate(rows):
                    row_num = idx + 2  # +2 потому что заголовок на строке 1
                    
                    # Валидация
                    is_valid, error = validate_row(r, col_names, row_num)
                    if not is_valid:
                        stats.skip(row_num, error)
                        continue
                    
                    try:
                        # Выравниваем длину строки
                        full_row = list(r) + [None] * (len(col_names) - len(r))
                        full_row = full_row[:len(col_names)]
                        
                        # Конвертируем в строки (все колонки пока text)
                        full_row = [str(val) if val not in (None, '') else None for val in full_row]
                        
                        # Вычисляем хеш строки для CDC
                        row_hash = compute_row_hash(full_row)
                        
                        # Добавляем _row_index и __row_hash
                        prepared_rows.append(tuple(full_row + [row_num, row_hash]))
                        
                    except Exception as e:
                        stats.skip(row_num, f"Ошибка обработки: {e}")
                        continue
                
                # Загрузка
                if prepared_rows:
                    target_cols = col_names + ["_row_index", "__row_hash"]
                    await conn.copy_records_to_table(
                        target_table,
                        records=prepared_rows,
                        columns=target_cols
                    )
                    stats.loaded_rows = len(prepared_rows)
                
                stats.report()
                
    except Exception as e:
        log.error(f"Критическая ошибка: {e}")
        raise
    finally:
        await conn.close()
    
    # Итоговый отчёт
    log.info("=" * 50)
    log.info("ИТОГОВЫЙ ОТЧЁТ")
    total_loaded = sum(s.loaded_rows for s in all_stats)
    total_skipped = sum(len(s.skipped_rows) for s in all_stats)
    log.info(f"Всего загружено: {total_loaded} строк")
    log.info(f"Всего пропущено: {total_skipped} строк")


if __name__ == "__main__":
    asyncio.run(load_data())
