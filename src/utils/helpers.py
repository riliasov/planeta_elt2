import re

def slugify(text: str) -> str:
    """Преобразует текст (в т.ч. кириллицу) в snake_case для имен колонок БД."""
    if not text:
        return "col_unknown"

    # Транслитерация
    mapping = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
        ' ': '_', '-': '_', '.': '', ',': '', '/': '_', '(': '', ')': ''
    }

    clean_text = str(text).lower()
    result = ''
    for char in clean_text:
        result += mapping.get(char, char)

    # Убираем лишние символы
    result = re.sub(r'[^a-z0-9_]', '', result)
    result = re.sub(r'_+', '_', result).strip('_')
    
    # Если начинается с цифры или пустой
    if not result:
        return "col_unnamed"
        
    if result[0].isdigit():
        result = 'col_' + result
        
    return result
