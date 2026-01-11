import pytest
import re
from src.etl.loader import DataLoader

@pytest.fixture
def loader():
    return DataLoader()

def test_validate_identifier_valid(loader):
    """Тест валидных идентификаторов."""
    valid_ids = ["users", "order_items", "v1_data", "field_123", "schema.table", "stg_gsheets.clients"]
    for ident in valid_ids:
        assert loader._validate_identifier(ident) == ident

def test_validate_identifier_invalid(loader):
    """Тест невалидных идентификаторов (попытка инъекции)."""
    invalid_ids = [
        "users; DROP TABLE users",
        "order_items --",
        "table' OR '1'='1",
        "field name with spaces",
        "table-name",
        "\"double_quotes\"",
        "`backticks`"
    ]
    for ident in invalid_ids:
        with pytest.raises(ValueError) as excinfo:
            loader._validate_identifier(ident)
        assert "Недопустимый идентификатор" in str(excinfo.value)

def test_row_preparation(loader):
    """Тест нормализации строк."""
    col_names = ["id", "name", "price"]
    row = [1, " Item ", "1 250,50 руб."]
    
    # Мы ожидаем что _prepare_row вернет (cleaned_values, hash)
    # Price должен быть очищен от ' руб.' и ' ', ',' заменена на '.'
    cleaned, row_hash = loader._prepare_row(row, col_names, 0)
    
    assert cleaned[0] == "1"
    assert cleaned[1] == "Item"
    assert cleaned[2] == "1250.50"
    assert len(row_hash) == 32 # MD5
