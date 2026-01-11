import unittest
import uuid
import json
from unittest.mock import MagicMock, AsyncMock, patch
from src.etl.pipeline import ELTPipeline

class TestPipelineE2E(unittest.IsolatedAsyncioTestCase):
    """E2E тесты для пайплайна."""

    async def test_pipeline_e2e_flow(self):
        """Проверка полного цикла Pipeline -> TableProcessor -> DB."""
        
        # 1. Подготовка
        pipeline = ELTPipeline()
        
        # Мокаем настройки
        mock_sources = {
            "spreadsheets": {
                "spreadsheet_123": {
                    "sheets": [
                        {
                            "target_table": "stg_gsheets.clients",
                            "gid": 0,
                            "pk": "id"
                        }
                    ]
                }
            }
        }
        
        # 2. Патчим все зависимости через instance patching
        with patch("src.etl.pipeline.settings") as mock_settings:
            mock_settings.sources = mock_sources
            mock_settings.use_staging_schema = True
            
            # Подменяем экстактор на мок
            # Важно: В реальности GSheetsExtractor возвращает уже СЛАГИФИЦИРОВАННЫЕ заголовки
            pipeline.extractor.extract_sheet_data = AsyncMock(return_value=(
                ["id", "klient", "mobilnyy", "tip"], 
                [["1", "Иван Иванов", "79991234567", "Зал"]]
            ))
            
            # Подменяем лоадер (его внутренние методы работы с базой)
            pipeline.loader._fetch_existing_hashes = AsyncMock(return_value={})
            
            # Патчим DBConnection глобально для всех компонентов внутри этого теста
            mock_exec = AsyncMock()
            mock_fetch = AsyncMock(return_value=[])
            
            # Чтобы DBConnection.execute и fetch работали во всех модулях
            with patch("src.db.connection.DBConnection.execute", side_effect=mock_exec), \
                 patch("src.db.connection.DBConnection.fetch", side_effect=mock_fetch), \
                 patch("src.db.connection.DBConnection.get_connection", new_callable=AsyncMock) as mock_get_conn:
                
                # Настройка контекстного менеджера соединения
                mock_conn = MagicMock()
                mock_conn.executemany = AsyncMock()
                mock_conn.execute = mock_exec # переиспользуем тот же мок
                
                mock_pool_acquire = MagicMock()
                mock_pool_acquire.__aenter__ = AsyncMock(return_value=mock_conn)
                mock_pool_acquire.__aexit__ = AsyncMock(return_value=None)
                mock_get_conn.return_value = mock_pool_acquire

                # 3. Запуск
                await pipeline.run(scope='current', skip_transform=True, run_exports=False)
            
            # 4. Проверки
            print(f"DEBUG: Processor stats: {pipeline._run_stats}")
            # Проверим детали для таблицы stg_gsheets.clients
            details = next((d for d in pipeline._table_run_details if d['table'] == 'stg_gsheets.clients'), None)
            if details and 'errors' in details:
                 print(f"DEBUG: Validation errors detail: {details['errors']}")

            pipeline.extractor.extract_sheet_data.assert_called_once()
            self.assertTrue(mock_conn.executemany.called or mock_exec.called)
            
            # Статистика
            self.assertEqual(pipeline._run_stats['tables_processed'], 1)
            self.assertEqual(pipeline._run_stats['total_rows_synced'], 1)
            
            # _start_run был?
            self.assertTrue(mock_exec.called)

if __name__ == '__main__':
    unittest.main()
