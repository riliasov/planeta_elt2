"""Модуль очистки и нормализации данных перед загрузкой в БД."""
from typing import List, Dict, Any, Optional
from datetime import datetime
from src.config.constants import (
    NUMERIC_KEYWORDS,
    DATE_KEYWORDS,
    BOOLEAN_COLUMNS,
    SERVICE_COLUMNS
)


def normalize_value(val: Any) -> str:
    """Нормализует значение для стабильного сравнения."""
    if val is None or val == '':
        return ''
    s = str(val).strip()
    return ' '.join(s.split())


def convert_serial_date(val: Any) -> Optional[datetime]:
    """Конвертирует serial date из Google Sheets (epoch 1899-12-30) в datetime."""
    if val is None or val == '':
        return None
    
    # Если уже datetime
    if isinstance(val, datetime):
        return val
    
    # Пробуем как serial number
    try:
        serial = float(val)
        # Google Sheets epoch: 1899-12-30
        # offset = 25569 дней между 1899-12-30 и 1970-01-01
        days_since_unix = serial - 25569
        return datetime.fromtimestamp(days_since_unix * 86400)
    except (ValueError, TypeError, OSError):
        pass
    
    # Пробуем как строку даты
    if isinstance(val, str):
        for fmt in ['%d.%m.%Y', '%d.%m.%y', '%Y-%m-%d', '%d/%m/%Y']:
            try:
                return datetime.strptime(val.strip(), fmt)
            except ValueError:
                continue
    
    return None


def clean_numeric(val: Any) -> Optional[float]:
    """Очищает и конвертирует числовое значение."""
    if val is None or val == '':
        return None
    
    if isinstance(val, (int, float)):
        return float(val)
    
    # Удаляем пробелы, неразрывные пробелы, меняем запятую на точку
    cleaned = str(val).replace('\xa0', '').replace(' ', '').replace(',', '.').strip()
    
    if not cleaned:
        return None
    
    try:
        return float(cleaned)
    except ValueError:
        return None


def clean_boolean(val: Any) -> Optional[bool]:
    """Конвертирует значение в boolean."""
    if val is None:
        return None
    
    true_values = {'TRUE', 'True', 'true', '1', 'да', 'yes', 'Y', 'y'}
    false_values = {'FALSE', 'False', 'false', '0', 'нет', 'no', 'N', 'n'}
    
    str_val = str(val).strip()
    
    if str_val in true_values or val == 1 or val is True:
        return True
    if str_val in false_values or val == 0 or val is False:
        return False
    
    return None


def clean_text(val: Any) -> Optional[str]:
    """Очищает текстовое значение."""
    if val is None:
        return None
    
    cleaned = str(val).strip()
    
    if cleaned in ('', 'nan', 'None', 'null', 'NaN'):
        return None
    
    return cleaned


def is_date_column(col_name: str) -> bool:
    """Проверяет, является ли колонка датой по названию."""
    col_lower = col_name.lower()
    return any(keyword in col_lower for keyword in DATE_KEYWORDS)


def is_numeric_column(col_name: str) -> bool:
    """Проверяет, является ли колонка числовой по названию."""
    col_lower = col_name.lower()
    return any(keyword in col_lower for keyword in NUMERIC_KEYWORDS)


def is_boolean_column(col_name: str) -> bool:
    """Проверяет, является ли колонка boolean по названию."""
    return col_name in BOOLEAN_COLUMNS


def is_service_column(col_name: str) -> bool:
    """Проверяет, является ли колонка служебной."""
    return col_name in SERVICE_COLUMNS


def clean_row(row: Dict[str, Any], col_names: List[str]) -> Dict[str, Any]:
    """Очищает одну строку данных на основе названий колонок."""
    cleaned = {}
    
    for col_name in col_names:
        val = row.get(col_name)
        
        # Пропускаем служебные колонки
        if is_service_column(col_name):
            cleaned[col_name] = val
            continue
        
        # Определяем тип и очищаем
        if is_date_column(col_name):
            cleaned[col_name] = convert_serial_date(val)
        elif is_numeric_column(col_name):
            cleaned[col_name] = clean_numeric(val)
        elif is_boolean_column(col_name):
            cleaned[col_name] = clean_boolean(val)
        else:
            cleaned[col_name] = clean_text(val)
    
    return cleaned


def clean_rows(rows: List[Dict[str, Any]], col_names: List[str]) -> List[Dict[str, Any]]:
    """Очищает список строк данных."""
    return [clean_row(row, col_names) for row in rows]
