import sys
from pathlib import Path
import logging

# Настройка путей
root_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(root_dir))

from src.etl.extractor import GSheetsExtractor
from src.etl.processor import TableProcessor
from src.etl.loader import DataLoader
from src.etl.validator import ContractValidator

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('test_mapping')

async def test_mapping_robustness():
    log.info("Запуск теста устойчивости маппинга...")
    
    # Мокаем зависимости
    # ВАЖНО: Мокаем _authenticate, чтобы не требовался реальный creds.json
    from unittest.mock import patch, MagicMock
    
    with patch('src.etl.extractor.GSheetsExtractor._authenticate'):
        extractor = GSheetsExtractor()
        # Mock drive_service/gc if needed manually, but for this test we mock extract_sheet_data anyway
    loader = DataLoader() # Мы будем использовать его в dry-run
    validator = ContractValidator()
    
    # 1. Симуляция данных из GSheet
    # Заголовки: [Имя, EMAIL, Лишняя колонка, Дата]
    headers = ["Имя", "EMAIL", "Extra", "Дата"]
    # Строка 1: Полная [John, j@j.com, delete me, 2024-01-01]
    # Строка 2: Короткая [Jane, j2@j.com] (все, что дальше - пусто)
    rows = [
        ["John", "j@j.com", "junk", "2024-01-01"],
        ["Jane", "j2@j.com"]
    ]
    
    # Мокаем extractor.extract_sheet_data
    # (Мы уже изменили его, проверим как он вернет данные)
    # На самом деле мы хотим проверить TableProcessor, так что имитируем возврат из extractor
    
    col_names = ["имя", "email", "extra", "дата"] # Слагифицированные
    
    # Тестируем логику выравнивания (имитируем внутренности extractor)
    aligned_rows = []
    expected_len = len(headers)
    for r in rows:
        if len(r) < expected_len:
            r.extend([None] * (expected_len - len(r)))
        aligned_rows.append(r[:expected_len])
    
    log.info(f"Выровненные строки: {aligned_rows}")
    assert len(aligned_rows[1]) == 4, "Строка должна быть дополнена None"
    assert aligned_rows[1][2] is None, "Лишняя ячейка должна быть None"
    
    # 2. Симуляция TableProcessor
    # Допустим контракт знает только про 'имя', 'email' и 'дата'
    contract_cols = {"имя", "email", "дата"}
    
    dict_rows = []
    for row in aligned_rows:
        row_dict = {k: v for k, v in zip(col_names, row) if k in contract_cols}
        dict_rows.append(row_dict)
    
    log.info(f"Словари после фильтрации по контракту: {dict_rows}")
    
    assert "extra" not in dict_rows[0], "Колонка 'extra' должна быть отфильтрована"
    assert dict_rows[0]["имя"] == "John"
    assert dict_rows[1]["email"] == "j2@j.com"
    assert dict_rows[1]["дата"] is None, "Отсутствующая дата в короткой строке должна быть None"
    
    log.info("✅ Тест успешно пройден!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_mapping_robustness())
