import re

def slugify(text: str) -> str:
    """Преобразует текст в snake_case (подходит для имен колонок БД)."""
    if not text:
        return ""
    text = str(text).lower()
    text = re.sub(r'[\s\n]+', '_', text)
    text = re.sub(r'[^\w_]+', '', text)
    return text.strip('_')
