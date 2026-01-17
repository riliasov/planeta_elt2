import sys
import os
import re

# Добавляем корень проекта в путь
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.etl.loader import DataLoader

def test_validate_identifier_valid():
    loader = DataLoader()
    assert loader._validate_identifier("users") == "users"
    assert loader._validate_identifier("core.sales") == "core.sales"
    assert loader._validate_identifier("my_table_123") == "my_table_123"

def test_validate_identifier_invalid():
    loader = DataLoader()
    invalid_idents = [
        "users; DROP TABLE users",
        "core.sales.extra",
        "123table",
        "table-name",
        "table name",
        "",
        "core. sales",
        "schema..table"
    ]
    for ident in invalid_idents:
        try:
            loader._validate_identifier(ident)
            raise Exception(f"Failed to catch invalid identifier: {ident}")
        except ValueError:
            pass

if __name__ == "__main__":
    try:
        test_validate_identifier_valid()
        print("✅ Valid identifiers passed")
        test_validate_identifier_invalid()
        print("✅ Invalid identifiers caught")
        print("\nSQL Injection Protection Verified!")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        sys.exit(1)
