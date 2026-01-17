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
    """Обработчик CDC для сравнения данных между источником и БД.
    
    Разделяет понятия:
    - pk (Primary Key): уникальный идентификатор строки для поиска (стабильный).
    - row_hash (Content Hash): хеш содержимого для детекции изменений (динамичный).
    """
    
    def __init__(self, existing_hashes: Dict[str, str]):
        """existing_hashes: словарь {pk: hash} из текущего состояния БД."""
        self.existing_hashes = existing_hashes
        self.to_insert: List[Dict] = []
        self.to_update: List[Dict] = []
        self.to_delete: List[str] = []
        self.unchanged: int = 0
    
    def process_row(self, pk: str, row_hash: str, row_data: Dict):
        """Обрабатывает одну строку и определяет действие."""
        if pk in self.existing_hashes:
            if self.existing_hashes[pk] == row_hash:
                self.unchanged += 1
            else:
                self.to_update.append({
                    'pk': pk,
                    'hash': row_hash,
                    'data': row_data
                })
            # Удаляем из существующих, чтобы в конце остались только удаленные в источнике
            del self.existing_hashes[pk]
        else:
            self.to_insert.append({
                'pk': pk,
                'hash': row_hash,
                'data': row_data
            })
    
    def finalize(self):
        """Все оставшиеся в existing_hashes ID считаются удалёнными в источнике."""
        self.to_delete = list(self.existing_hashes.keys())
    
    def get_stats(self) -> Dict[str, int]:
        return {
            'inserted': len(self.to_insert),
            'updated': len(self.to_update),
            'deleted': len(self.to_delete),
            'unchanged': self.unchanged
        }
