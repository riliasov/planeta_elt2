import gspread
import json
import logging
from typing import List, Dict, Any, Tuple
from google.oauth2.service_account import Credentials
from src.config.settings import settings
from src.utils.helpers import slugify

log = logging.getLogger('extractor')

class GSheetsExtractor:
    def __init__(self):
        self.gc = None
        self._authenticate()

    def _authenticate(self):
        """Аутентификация в Google Services."""
        try:
            creds_path = settings.google_service_account_json
            with open(creds_path, 'r') as f:
                creds_info = json.load(f)
            
            scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
            creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
            self.gc = gspread.authorize(creds)
            log.info("Google Service Account authenticated successfully.")
        except Exception as e:
            log.error(f"Failed to authenticate with Google: {e}")
            raise

    async def extract_sheet_data(self, spreadsheet_id: str, gid: str, range_name: str, target_table: str) -> Tuple[List[str], List[List[Any]]]:
        """
        Извлекает данные из конкретного листа с повторными попытками.
        """
        log.info(f"Extracting {target_table} from {spreadsheet_id} (gid={gid})")
        
        for attempt in range(3):
            try:
                # Re-auth if needed? gspread handles token refresh usually.
                sh = self.gc.open_by_key(spreadsheet_id)
                ws = sh.get_worksheet_by_id(int(gid))
                
                if not ws:
                    raise ValueError(f"Worksheet with GID {gid} not found in {spreadsheet_id}")

                data = ws.get(range_name)
                
                if not data:
                    log.warning(f"No data found for {target_table}")
                    return [], []

                headers = data[0]
                rows = data[1:]
                
                col_names = self._normalize_headers(headers, target_table)
                return col_names, rows
                
            except Exception as e:
                if '429' in str(e):
                    sleep_time = (attempt + 1) * 5
                    log.warning(f"Quota exceeded for {target_table}, retrying in {sleep_time}s...")
                    import time
                    time.sleep(sleep_time)
                else:
                    log.error(f"Error extracting data for {target_table}: {e}")
                    raise
        raise Exception(f"Failed to extract {target_table} after retries")

    def _normalize_headers(self, headers: List[str], table_name: str) -> List[str]:
        """Превращает заголовки Sheet в валидные имена колонок Postgres."""
        # Особый случай для rates (там даты в заголовках) - сохраняем логику из fast_loader.py
        if table_name == 'rates':
            return [f"col_{i}" for i, _ in enumerate(headers)]

        seen = set()
        col_names = []
        for h in headers:
            col_name = slugify(h) or "unknown_col"
            original = col_name
            counter = 1
            while col_name in seen:
                col_name = f"{original}_{counter}"
                counter += 1
            seen.add(col_name)
            col_names.append(col_name)
        return col_names
