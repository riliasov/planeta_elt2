"""Модуль CDC (Change Data Capture) для отслеживания изменений в данных."""

import hashlib
import json
from typing import Optional


def compute_row_hash(row: list, exclude_columns: Optional[set] = None) -> str:
    """Вычисляет MD5 хеш строки данных для сравнения изменений."""
    if exclude_columns is None:
        exclude_columns = set()
    
    # Фильтруем служебные колонки
    filtered_values = []
    for i, val in enumerate(row):
        if i not in exclude_columns:
            # Нормализуем значение для стабильного хеша
            normalized = normalize_value(val)
            filtered_values.append(normalized)
    
    # Создаём строку для хеширования
    content = json.dumps(filtered_values, ensure_ascii=False, sort_keys=True)
    return hashlib.md5(content.encode('utf-8')).hexdigest()


def normalize_value(val) -> str:
    """Нормализует значение для стабильного хеширования."""
    if val is None or val == '':
        return ''
    
    # Преобразуем в строку и убираем лишние пробелы
    s = str(val).strip()
    
    # Нормализуем пробелы внутри строки
    s = ' '.join(s.split())
    
    return s


class CDCProcessor:
    """Обработчик CDC для сравнения данных между источником и БД."""
    
    def __init__(self, existing_hashes: dict[str, str]):
        """Инициализация с существующими хешами из БД (legacy_id -> hash)."""
        self.existing_hashes = existing_hashes
        self.to_insert: list[dict] = []
        self.to_update: list[dict] = []
        self.to_delete: list[str] = []
        self.unchanged: int = 0
    
    def process_row(self, legacy_id: str, row_hash: str, row_data: dict):
        """Обрабатывает одну строку и определяет действие."""
        if legacy_id in self.existing_hashes:
            if self.existing_hashes[legacy_id] == row_hash:
                # Без изменений
                self.unchanged += 1
            else:
                # Изменена — нужен UPDATE
                self.to_update.append({
                    'legacy_id': legacy_id,
                    'hash': row_hash,
                    'data': row_data
                })
            # Удаляем из existing, чтобы потом найти удалённые
            del self.existing_hashes[legacy_id]
        else:
            # Новая строка — INSERT
            self.to_insert.append({
                'legacy_id': legacy_id,
                'hash': row_hash,
                'data': row_data
            })
    
    def finalize(self):
        """Определяет удалённые записи (оставшиеся в existing_hashes)."""
        self.to_delete = list(self.existing_hashes.keys())
    
    def get_stats(self) -> dict:
        """Возвращает статистику CDC."""
        return {
            'insert': len(self.to_insert),
            'update': len(self.to_update),
            'delete': len(self.to_delete),
            'unchanged': self.unchanged
        }


async def fetch_existing_hashes(conn, table: str) -> dict[str, str]:
    """Получает существующие legacy_id и хеши из БД."""
    query = f'SELECT legacy_id, __row_hash FROM "{table}" WHERE legacy_id IS NOT NULL'
    try:
        rows = await conn.fetch(query)
        return {row['legacy_id']: row['__row_hash'] for row in rows if row['__row_hash']}
    except Exception:
        # Если колонки __row_hash нет — возвращаем пустой dict
        return {}
