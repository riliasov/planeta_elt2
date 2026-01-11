import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from src.etl.loader import DataLoader

class TestLoaderSchema(unittest.IsolatedAsyncioTestCase):
    
    async def test_loader_uses_staging_schema(self):
        # 1. Setup - Force settings to use staging
        with patch('src.config.settings.settings.use_staging_schema', True):
            loader = DataLoader()
            self.assertEqual(loader.schema_prefix, 'staging.')
            
            # Mock DB Connection
            mock_conn = MagicMock()
            mock_conn.execute = AsyncMock()
            mock_conn.copy_records_to_table = AsyncMock()
            
            # Mock transaction context manager
            mock_transaction = AsyncMock()
            mock_transaction.__aenter__.return_value = None
            mock_transaction.__aexit__.return_value = None
            mock_conn.transaction.return_value = mock_transaction
            
            # Mock connection context manager
            mock_pool_acquire = MagicMock()
            mock_pool_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_pool_acquire.__aexit__ = AsyncMock(return_value=None)
            
            with patch('src.db.connection.DBConnection.get_connection', return_value=mock_pool_acquire):
                # 2. Execute
                table = 'test_table'
                cols = ['col1']
                rows = [['val1']]
                
                await loader.load_full_refresh(table, cols, rows)
                
                # 3. Verify TRUNCATE
                mock_conn.execute.assert_called_with('TRUNCATE TABLE staging."test_table"')
                
                # 4. Verify COPY
                # copy_records_to_table(table, schema='staging', ...)
                args, kwargs = mock_conn.copy_records_to_table.call_args
                self.assertEqual(args[0], table)
                self.assertEqual(kwargs['schema_name'], 'staging')
                self.assertEqual(kwargs['columns'], ['col1', '_row_index', '__row_hash'])

    async def test_loader_schema_prefix_logic(self):
        # Test when staging is disabled
        with patch('src.config.settings.settings.use_staging_schema', False):
            loader = DataLoader()
            self.assertEqual(loader.schema_prefix, '')
            
            mock_conn = MagicMock()
            mock_conn.execute = AsyncMock()
            mock_conn.copy_records_to_table = AsyncMock()
            
            mock_transaction = AsyncMock()
            mock_transaction.__aenter__.return_value = None
            mock_transaction.__aexit__.return_value = None
            mock_conn.transaction.return_value = mock_transaction
            
            mock_pool_acquire = MagicMock()
            mock_pool_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_pool_acquire.__aexit__ = AsyncMock(return_value=None)
            
            with patch('src.db.connection.DBConnection.get_connection', return_value=mock_pool_acquire):
                await loader.load_full_refresh('test_table', ['col1'], [['val1']])
                
                # Verify TRUNCATE uses public (quoted)
                mock_conn.execute.assert_called_with('TRUNCATE TABLE "test_table"')
                
                # Verify COPY uses no schema (None)
                args, kwargs = mock_conn.copy_records_to_table.call_args
                self.assertIsNone(kwargs['schema_name'])

if __name__ == '__main__':
    unittest.main()
