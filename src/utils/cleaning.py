import re
from typing import Any, Optional

def clean_string(val: Any) -> Optional[str]:
    """Базовая очистка строки."""
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ('nan', 'none', '', 'null'):
        return None
    return s

def normalize_numeric_string(val: Any) -> Optional[str]:
    """Очистка числовой строки (удаление пробелов, nbsp, замена запятой, валют)."""
    s = clean_string(val)
    if s is None:
        return None
    
    # Удаляем любые пробельные символы (включая nbsp \xa0)
    s = re.sub(r'\s+', '', s)
    # Заменяем десятичную запятую на точку
    s = s.replace(',', '.')
    
    # Регулярка для удаления типичных суффиксов (валюты, ед. изм.)
    # руб, руб., $, €, шт, шт., кг, кг.
    s = re.sub(r'(руб|руб\.|\$|€|шт|шт\.|\w{1,3}\.)$', '', s, flags=re.IGNORECASE)
    
    # Если то что осталось - число (с точкой или без), возвращаем только цифры/точку/минус
    # Но только если это реально похоже на число, а не на артикул типа "A-123"
    if re.match(r'^-?\d+(\.\d+)?$', s):
        return s
    
    return s

def normalize_boolean(val: Any) -> Optional[bool]:
    """Приведение к boolean."""
    s = clean_string(val)
    if s is None:
        return None
    
    s = s.lower()
    if s in ('true', '1', 'yes', 'да', 'v'):
        return True
    if s in ('false', '0', 'no', 'нет'):
        return False
    return None
