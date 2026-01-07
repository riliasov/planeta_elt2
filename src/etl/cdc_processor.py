import hashlib
import json
from typing import Optional, List, Dict, Set

def compute_row_hash(row: list, exclude_columns: Optional[set] = None) -> str:
    """Вычисляет MD5 хеш строки данных для сравнения изменений."""
    if exclude_columns is None:
        exclude_columns = set()
    
    filtered_values = []
    for i, val in enumerate(row):
        if i not in exclude_columns:
            filtered_values.append(normalize_value(val))
    
    content = json.dumps(filtered_values, ensure_ascii=False, sort_keys=True)
    return hashlib.md5(content.encode('utf-8')).hexdigest()


def normalize_value(val) -> str:
    """Нормализует значение для стабильного хеширования."""
    if val is None or val == '':
        return ''
    s = str(val).strip()
    return ' '.join(s.split())


class CDCProcessor:
    """Обработчик CDC для сравнения данных между источником и БД."""
    
    def __init__(self, existing_hashes: Dict[str, str]):
        self.existing_hashes = existing_hashes
        self.to_insert: List[Dict] = []
        self.to_update: List[Dict] = []
        self.to_delete: List[str] = []
        self.unchanged: int = 0
    
    def process_row(self, legacy_id: str, row_hash: str, row_data: Dict):
        """Обрабатывает одну строку и определяет действие."""
        if legacy_id in self.existing_hashes:
            if self.existing_hashes[legacy_id] == row_hash:
                self.unchanged += 1
            else:
                self.to_update.append({
                    'legacy_id': legacy_id,
                    'hash': row_hash,
                    'data': row_data
                })
            # Удаляем из existing, чтобы потом найти удалённые
            del self.existing_hashes[legacy_id]
        else:
            self.to_insert.append({
                'legacy_id': legacy_id,
                'hash': row_hash,
                'data': row_data
            })
    
    def finalize(self):
        """Все оставшиеся в existing_hashes ID считаются удалёнными."""
        self.to_delete = list(self.existing_hashes.keys())
    
    def get_stats(self) -> Dict[str, int]:
        return {
            'insert': len(self.to_insert),
            'update': len(self.to_update),
            'delete': len(self.to_delete),
            'unchanged': self.unchanged
        }
