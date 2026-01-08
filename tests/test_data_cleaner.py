"""Тесты для модуля data_cleaner."""
import pytest
from datetime import datetime
from src.etl.data_cleaner import (
    normalize_value,
    convert_serial_date,
    clean_numeric,
    clean_boolean,
    clean_text,
    is_date_column,
    is_numeric_column,
    clean_row
)


class TestNormalizeValue:
    def test_none_returns_empty_string(self):
        assert normalize_value(None) == ''
    
    def test_empty_string_returns_empty(self):
        assert normalize_value('') == ''
    
    def test_strips_whitespace(self):
        assert normalize_value('  hello  ') == 'hello'
    
    def test_normalizes_internal_spaces(self):
        assert normalize_value('hello    world') == 'hello world'


class TestConvertSerialDate:
    def test_none_returns_none(self):
        assert convert_serial_date(None) is None
    
    def test_empty_string_returns_none(self):
        assert convert_serial_date('') is None
    
    def test_serial_number_conversion(self):
        # 44197 = 2021-01-01 in Google Sheets epoch
        result = convert_serial_date(44197)
        assert result is not None
        assert result.year == 2021
        assert result.month == 1
        assert result.day == 1
    
    def test_string_date_dd_mm_yyyy(self):
        result = convert_serial_date('15.03.2024')
        assert result is not None
        assert result.day == 15
        assert result.month == 3
        assert result.year == 2024
    
    def test_already_datetime_returns_same(self):
        dt = datetime(2024, 5, 10)
        assert convert_serial_date(dt) == dt


class TestCleanNumeric:
    def test_none_returns_none(self):
        assert clean_numeric(None) is None
    
    def test_empty_string_returns_none(self):
        assert clean_numeric('') is None
    
    def test_integer_value(self):
        assert clean_numeric(42) == 42.0
    
    def test_float_value(self):
        assert clean_numeric(3.14) == 3.14
    
    def test_string_with_spaces(self):
        assert clean_numeric('1 000') == 1000.0
    
    def test_string_with_nbsp(self):
        assert clean_numeric('1\xa0000') == 1000.0
    
    def test_string_with_comma_decimal(self):
        assert clean_numeric('3,14') == 3.14
    
    def test_invalid_string_returns_none(self):
        assert clean_numeric('abc') is None


class TestCleanBoolean:
    def test_true_values(self):
        assert clean_boolean('TRUE') is True
        assert clean_boolean('True') is True
        assert clean_boolean('1') is True
        assert clean_boolean(1) is True
        assert clean_boolean('да') is True
    
    def test_false_values(self):
        assert clean_boolean('FALSE') is False
        assert clean_boolean('0') is False
        assert clean_boolean(0) is False
        assert clean_boolean('нет') is False
    
    def test_none_returns_none(self):
        assert clean_boolean(None) is None
    
    def test_unknown_returns_none(self):
        assert clean_boolean('maybe') is None


class TestCleanText:
    def test_none_returns_none(self):
        assert clean_text(None) is None
    
    def test_strips_whitespace(self):
        assert clean_text('  hello  ') == 'hello'
    
    def test_empty_string_returns_none(self):
        assert clean_text('') is None
    
    def test_nan_string_returns_none(self):
        assert clean_text('nan') is None
        assert clean_text('None') is None


class TestColumnTypeDetection:
    def test_date_columns(self):
        assert is_date_column('data') is True
        assert is_date_column('created_date') is True
        assert is_date_column('data_rozhdeniya') is True  # транслит
        assert is_date_column('name') is False
    
    def test_numeric_columns(self):
        assert is_numeric_column('summa') is True
        assert is_numeric_column('polnaya_stoimost') is True  # транслит
        assert is_numeric_column('kolichestvo') is True  # транслит
        assert is_numeric_column('name') is False


class TestCleanRow:
    def test_cleans_row_based_on_column_names(self):
        row = {
            'data': '15.03.2024',
            'summa': '1 000',
            'active': 'TRUE',
            'name': '  John  '
        }
        col_names = ['data', 'summa', 'active', 'name']
        result = clean_row(row, col_names)
        
        assert result['data'].year == 2024
        assert result['summa'] == 1000.0
        assert result['active'] is True
        assert result['name'] == 'John'
