import pytest
from src.utils.cleaning import normalize_numeric_string, normalize_boolean, clean_string

def test_clean_string():
    assert clean_string("  hello  ") == "hello"
    assert clean_string("nan") is None
    assert clean_string("") is None
    assert clean_string(None) is None

def test_normalize_numeric_string():
    # nbsp check
    assert normalize_numeric_string("1\xa0000,50") == "1000.50"
    # spaces
    assert normalize_numeric_string(" 2 500 ") == "2500"
    # commas
    assert normalize_numeric_string("12,3") == "12.3"
    # mixed
    assert normalize_numeric_string(" - 1 234,56 ") == "-1234.56"

def test_normalize_boolean():
    assert normalize_boolean("Да") is True
    assert normalize_boolean("1") is True
    assert normalize_boolean("true") is True
    assert normalize_boolean("Нет") is False
    assert normalize_boolean("0") is False
    assert normalize_boolean("unknown") is None
