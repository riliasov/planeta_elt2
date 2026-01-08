import pytest
import asyncio
from pathlib import Path
import sys
from unittest.mock import AsyncMock, patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.etl.transformer import Transformer, SQL_DIR

# Helper for async tests without pytest-asyncio
def run_async(coro):
    return asyncio.get_event_loop().run_until_complete(coro)

class TestTransformerFiles:
    """Тесты наличия и валидности SQL файлов."""
    
    def test_sql_files_exist(self):
        """Проверяем, что все необходимые SQL файлы существуют."""
        files = [
            'transform_clients.sql',
            'transform_schedule.sql',
            'transform_sales.sql',
            'cleanup.sql'
        ]
        for f in files:
            path = SQL_DIR / f
            assert path.exists(), f"Missing SQL file: {f}"

    def test_sql_content_validity(self):
        """Проверяем базовую валидность содержимого SQL."""
        files = [
            'transform_clients.sql',
            'transform_schedule.sql',
            'transform_sales.sql'
        ]
        
        for f in files:
            path = SQL_DIR / f
            with open(path, 'r', encoding='utf-8') as file:
                sql = file.read()
                
            # Проверяем ключевые элементы
            assert 'INSERT INTO' in sql, f"{f} missing INSERT"
            assert 'SELECT' in sql, f"{f} missing SELECT"
            assert 'ON CONFLICT' in sql, f"{f} missing ON CONFLICT"
            assert 'md5(' in sql.lower(), f"{f} missing md5 hash generation"
            
            # Проверяем отсутствие старых плейсхолдеров
            assert '{source_table}' not in sql, f"{f} has unreplaced placeholder"


class TestTransformerExecution:
    """Тесты выполнения трансформации."""

    def test_run_executes_sqls(self):
        """Проверяем, что метод run выполняет SQL файлы."""
        transformer = Transformer()
        
        async def _test():
            with patch('src.db.connection.DBConnection.execute', new_callable=AsyncMock) as mock_execute:
                success, errors = await transformer.run()
                
                assert success > 0
                assert errors == 0
                
                # Проверяем, что execute вызывался (как минимум 4 раза: 3 транфс + 1 cleanup)
                assert mock_execute.call_count >= 4
                
                # Проверяем аргументы (SQL запросы)
                calls = mock_execute.call_args_list
                sqls = [c[0][0] for c in calls]
                
                # Проверяем что запускались запросы для разных таблиц
                assert any('INSERT INTO clients' in sql for sql in sqls)
                assert any('INSERT INTO sales' in sql for sql in sqls)
                assert any('INSERT INTO schedule' in sql for sql in sqls)
                assert any('UPDATE sales' in sql for sql in sqls) # Cleanup

        run_async(_test())
