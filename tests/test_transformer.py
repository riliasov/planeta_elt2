"""Тесты для модуля трансформации."""
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl.transformer import (
    TRANSFORMATIONS, 
    SALES_SQL_TEMPLATE, 
    SALES_COLS
)


class TestTransformationConfig:
    """Тесты конфигурации трансформаций."""
    
    def test_all_required_tables_present(self):
        """Все необходимые таблицы присутствуют в конфигурации."""
        required_tables = ['clients_cur', 'trainings_cur', 'sales_cur', 'sales_hst']
        for table in required_tables:
            assert table in TRANSFORMATIONS, f"Missing transformation for {table}"
    
    def test_each_transformation_has_target(self):
        """Каждая трансформация имеет целевую таблицу."""
        for source, config in TRANSFORMATIONS.items():
            assert 'target' in config, f"Missing target for {source}"
            assert 'sql' in config, f"Missing SQL for {source}"
    
    def test_sales_cur_and_hst_share_target(self):
        """sales_cur и sales_hst сливаются в одну таблицу."""
        assert TRANSFORMATIONS['sales_cur']['target'] == 'sales'
        assert TRANSFORMATIONS['sales_hst']['target'] == 'sales'
    
    def test_sql_template_contains_required_placeholders(self):
        """SQL шаблон содержит все необходимые плейсхолдеры."""
        required_placeholders = [
            'date_col', 'client_col', 'product_col', 'price_col',
            'source_table', 'type_col', 'category_col'
        ]
        for placeholder in required_placeholders:
            assert f'{{{placeholder}}}' in SALES_SQL_TEMPLATE, f"Missing {placeholder}"
    
    def test_column_mappings_complete(self):
        """Все маппинги колонок заполнены."""
        required_keys = [
            'date_col', 'client_col', 'product_col', 'type_col', 
            'category_col', 'qty_col', 'price_col', 'discount_col',
            'final_col', 'cash_col', 'transfer_col', 'terminal_col',
            'debt_col', 'comment_col'
        ]
        
        for key in required_keys:
            assert key in SALES_COLS, f"Missing {key} in SALES_COLS"


class TestSQLGeneration:
    """Тесты генерации SQL."""
    
    def test_generated_sql_is_valid_syntax(self):
        """Сгенерированный SQL не содержит незаменённых плейсхолдеров."""
        sql = SALES_SQL_TEMPLATE.format(source_table='sales_cur', **SALES_COLS)
        
        # Проверяем отсутствие незаменённых плейсхолдеров вида {name}
        # Regex {2} допустимы — это PostgreSQL квантификаторы
        import re
        placeholder_pattern = r'\{[a-z_]+\}'  # {word} но не {2}
        
        assert not re.search(placeholder_pattern, sql), "Unreplaced placeholder in SQL"
    
    def test_generated_sql_references_correct_table(self):
        """SQL ссылается на правильную исходную таблицу."""
        sql_cur = TRANSFORMATIONS['sales_cur']['sql']
        sql_hst = TRANSFORMATIONS['sales_hst']['sql']
        
        assert 'sales_cur' in sql_cur
        assert 'sales_hst' in sql_hst
    
    def test_sql_contains_conflict_handling(self):
        """SQL содержит ON CONFLICT для upsert."""
        for source, config in TRANSFORMATIONS.items():
            sql = config['sql']
            assert 'ON CONFLICT' in sql, f"Missing ON CONFLICT in {source}"
            assert 'DO UPDATE SET' in sql, f"Missing DO UPDATE in {source}"
    
    def test_sql_contains_distinct_on(self):
        """SQL использует DISTINCT ON для дедупликации."""
        for source, config in TRANSFORMATIONS.items():
            sql = config['sql']
            assert 'DISTINCT ON' in sql, f"Missing DISTINCT ON in {source}"
    
    def test_sql_generates_legacy_id_with_md5(self):
        """SQL генерирует legacy_id через MD5."""
        for source, config in TRANSFORMATIONS.items():
            sql = config['sql']
            assert 'md5(' in sql.lower(), f"Missing MD5 hash in {source}"


class TestTransformationOrder:
    """Тесты порядка трансформаций."""
    
    def test_clients_before_sales(self):
        """Клиенты должны обрабатываться до продаж (для FK)."""
        keys = list(TRANSFORMATIONS.keys())
        clients_idx = keys.index('clients_cur')
        sales_cur_idx = keys.index('sales_cur')
        
        assert clients_idx < sales_cur_idx, "clients_cur must be processed before sales_cur"
    
    def test_clients_before_trainings(self):
        """Клиенты должны обрабатываться до тренировок (для FK)."""
        keys = list(TRANSFORMATIONS.keys())
        clients_idx = keys.index('clients_cur')
        trainings_idx = keys.index('trainings_cur')
        
        assert clients_idx < trainings_idx, "clients_cur must be processed before trainings_cur"


class TestDateParsing:
    """Тесты парсинга дат в SQL."""
    
    def test_sales_sql_handles_multiple_date_formats(self):
        """SQL обрабатывает несколько форматов дат."""
        sql = TRANSFORMATIONS['sales_cur']['sql']
        
        # Формат DD.MM.YY
        assert 'DD.MM.YY' in sql
        # Формат DD.MM.YYYY
        assert 'DD.MM.YYYY' in sql


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
