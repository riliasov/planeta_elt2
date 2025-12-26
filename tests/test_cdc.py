"""Тесты для модуля CDC."""

import pytest
from src.cdc import compute_row_hash, normalize_value, CDCProcessor


class TestComputeRowHash:
    """Тесты для функции compute_row_hash."""
    
    def test_same_data_same_hash(self):
        """Одинаковые данные должны давать одинаковый хеш."""
        row1 = ['Иванов', '+79991234567', 'Ваня']
        row2 = ['Иванов', '+79991234567', 'Ваня']
        
        assert compute_row_hash(row1) == compute_row_hash(row2)
    
    def test_different_data_different_hash(self):
        """Разные данные должны давать разный хеш."""
        row1 = ['Иванов', '+79991234567', 'Ваня']
        row2 = ['Петров', '+79991234567', 'Ваня']
        
        assert compute_row_hash(row1) != compute_row_hash(row2)
    
    def test_whitespace_normalized(self):
        """Пробелы должны нормализоваться."""
        row1 = ['Иванов  Иван', '+79991234567']
        row2 = ['Иванов Иван ', '+79991234567']
        
        assert compute_row_hash(row1) == compute_row_hash(row2)
    
    def test_empty_values(self):
        """Пустые значения обрабатываются корректно."""
        row1 = ['Иванов', None, 'Ваня']
        row2 = ['Иванов', '', 'Ваня']
        
        assert compute_row_hash(row1) == compute_row_hash(row2)
    
    def test_exclude_columns(self):
        """Исключённые колонки не влияют на хеш."""
        row1 = ['Иванов', '+79991234567', 'служебное1']
        row2 = ['Иванов', '+79991234567', 'служебное2']
        
        hash1 = compute_row_hash(row1, exclude_columns={2})
        hash2 = compute_row_hash(row2, exclude_columns={2})
        
        assert hash1 == hash2


class TestNormalizeValue:
    """Тесты для функции normalize_value."""
    
    def test_none_to_empty(self):
        assert normalize_value(None) == ''
    
    def test_empty_string(self):
        assert normalize_value('') == ''
    
    def test_strip_whitespace(self):
        assert normalize_value('  hello  ') == 'hello'
    
    def test_collapse_internal_whitespace(self):
        assert normalize_value('hello   world') == 'hello world'
    
    def test_number_to_string(self):
        assert normalize_value(123) == '123'


class TestCDCProcessor:
    """Тесты для класса CDCProcessor."""
    
    def test_new_row_is_insert(self):
        """Новая строка должна быть помечена для INSERT."""
        processor = CDCProcessor(existing_hashes={})
        
        processor.process_row('new-id', 'hash123', {'name': 'Test'})
        processor.finalize()
        
        assert len(processor.to_insert) == 1
        assert processor.to_insert[0]['legacy_id'] == 'new-id'
    
    def test_unchanged_row(self):
        """Строка без изменений не должна обновляться."""
        processor = CDCProcessor(existing_hashes={'id-1': 'hash123'})
        
        processor.process_row('id-1', 'hash123', {'name': 'Test'})
        processor.finalize()
        
        assert processor.unchanged == 1
        assert len(processor.to_update) == 0
    
    def test_changed_row_is_update(self):
        """Изменённая строка должна быть помечена для UPDATE."""
        processor = CDCProcessor(existing_hashes={'id-1': 'old-hash'})
        
        processor.process_row('id-1', 'new-hash', {'name': 'Test'})
        processor.finalize()
        
        assert len(processor.to_update) == 1
        assert processor.to_update[0]['legacy_id'] == 'id-1'
    
    def test_deleted_row(self):
        """Удалённая строка должна быть помечена для DELETE."""
        processor = CDCProcessor(existing_hashes={'id-1': 'hash123', 'id-2': 'hash456'})
        
        # Обрабатываем только id-1
        processor.process_row('id-1', 'hash123', {'name': 'Test'})
        processor.finalize()
        
        # id-2 должен быть помечен для удаления
        assert len(processor.to_delete) == 1
        assert 'id-2' in processor.to_delete
    
    def test_get_stats(self):
        """Статистика должна отражать все операции."""
        processor = CDCProcessor(existing_hashes={'id-1': 'old-hash', 'id-2': 'hash2'})
        
        processor.process_row('id-1', 'new-hash', {})  # UPDATE
        processor.process_row('id-3', 'hash3', {})     # INSERT
        processor.finalize()
        
        stats = processor.get_stats()
        assert stats['insert'] == 1
        assert stats['update'] == 1
        assert stats['delete'] == 1  # id-2
        assert stats['unchanged'] == 0
