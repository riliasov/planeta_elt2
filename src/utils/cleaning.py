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
    """Очистка числовой строки (удаление пробелов, nbsp, замена запятой)."""
    s = clean_string(val)
    if s is None:
        return None
    
    # Удаляем любые пробельные символы (включая nbsp \xa0)
    s = re.sub(r'\s+', '', s)
    # Заменяем десятичную запятую на точку
    s = s.replace(',', '.')
    
    # Пытаемся оставить только числовые символы, точку и минус
    # (но осторожно, чтобы не испортить совсем валидные строки)
    # Если строка после очистки не похожа на число, возвращаем как есть?
    # Нет, лучше вернуть очищенную версию.
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
