import logging
import asyncio
import pandas as pd
import gspread
from typing import List, Dict, Any, Optional
from src.db.connection import DBConnection
from src.config.settings import settings
from src.etl.extractor import GSheetsExtractor

log = logging.getLogger('exporter')

class DataMartExporter:
    def __init__(self):
        self.extractor = GSheetsExtractor() # Reusing for authentication

    async def get_client(self):
        """Возвращает авторизованный клиент gspread."""
        # extractor._get_service('sheets', 'v4') возвращает discovery object, 
        # но нам нужен gspread. Reusing credentials logic.
        from google.oauth2.service_account import Credentials
        
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = Credentials.from_service_account_file(
            settings.google_service_account_json, scopes=scopes
        )
        return gspread.authorize(creds)

    async def export_view_to_sheet(self, view_name: str, spreadsheet_id: str, gid: str):
        """Экспортирует результат SQL View в Google Sheet."""
        log.info(f"Экспорт витрины {view_name} в {spreadsheet_id} (gid={gid})...")
        
        # 1. Fetch data from DB
        try:
            query = f'SELECT * FROM {view_name}'
            rows = await DBConnection.fetch(query)
            if not rows:
                log.warning(f"Витрина {view_name} пуста, экспорт пропущен.")
                return
            
            df = pd.DataFrame([dict(r) for r in rows])
        except Exception as e:
            log.error(f"Ошибка при получении данных витрины {view_name}: {e}")
            return

        # 2. Prepare data for Sheets
        # Convert datetimes to strings
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or pd.api.types.is_timezone_aware_dtype(df[col]):
                df[col] = df[col].dt.strftime('%d.%m.%Y %H:%M')
            elif df[col].dtype == 'object':
                 df[col] = df[col].fillna('')

        values = [df.columns.tolist()] + df.values.tolist()

        # 3. Write to Sheets
        try:
            client = await self.get_client()
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._sync_write, client, spreadsheet_id, gid, values)
            log.info(f"Витрина {view_name} успешно экспортирована ({len(df)} строк).")
        except Exception as e:
            log.error(f"Ошибка при записи в Google Sheets: {e}")

    def _sync_write(self, client, spreadsheet_id, gid, values):
        """Синхронная часть записи gspread."""
        ss = client.open_by_key(spreadsheet_id)
        # Find worksheet by gid
        worksheet = None
        for ws in ss.worksheets():
            if str(ws.id) == str(gid):
                worksheet = ws
                break
        
        if not worksheet:
            raise ValueError(f"Лист с gid={gid} не найден.")

        # Очищаем и записываем
        worksheet.clear()
        worksheet.update('A1', values, value_input_option='USER_ENTERED')
