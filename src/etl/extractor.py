import gspread
import json
import logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from src.config.settings import settings
from src.utils.helpers import slugify

log = logging.getLogger('extractor')

# Кэш последних модификаций (spreadsheet_id -> modified_time)
_modification_cache: Dict[str, datetime] = {}


class GSheetsExtractor:
    def __init__(self):
        self.gc = None
        self.drive_service = None
        self._authenticate()

    def _authenticate(self):
        """Аутентификация в Google Services (Sheets + Drive)."""
        try:
            creds_path = settings.google_service_account_json
            with open(creds_path, 'r') as f:
                creds_info = json.load(f)
            
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/drive.metadata.readonly'
            ]
            creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
            
            # Sheets API
            self.gc = gspread.authorize(creds)
            
            # Drive API для получения modifiedTime
            self.drive_service = build('drive', 'v3', credentials=creds, cache_discovery=False)
            
            log.info("Google Service Account authenticated successfully (Sheets + Drive).")
        except Exception as e:
            log.error(f"Failed to authenticate with Google: {e}")
            raise

    def get_modified_time(self, spreadsheet_id: str) -> Optional[datetime]:
        """Получает время последней модификации spreadsheet через Drive API."""
        try:
            file_metadata = self.drive_service.files().get(
                fileId=spreadsheet_id,
                fields='modifiedTime'
            ).execute()
            
            modified_str = file_metadata.get('modifiedTime')
            if modified_str:
                # Parse ISO format: 2024-01-15T12:30:00.000Z
                return datetime.fromisoformat(modified_str.replace('Z', '+00:00'))
            return None
        except Exception as e:
            log.warning(f"Could not get modifiedTime for {spreadsheet_id}: {e}")
            return None

    def is_spreadsheet_modified(self, spreadsheet_id: str) -> bool:
        """Проверяет, изменился ли spreadsheet с последнего запроса."""
        current_time = self.get_modified_time(spreadsheet_id)
        if not current_time:
            return True  # Если не смогли получить — считаем что надо загрузить
        
        cached_time = _modification_cache.get(spreadsheet_id)
        if not cached_time:
            # Первый запрос — сохраняем и загружаем
            _modification_cache[spreadsheet_id] = current_time
            return True
        
        if current_time > cached_time:
            # Изменился — обновляем кэш
            _modification_cache[spreadsheet_id] = current_time
            log.info(f"Spreadsheet {spreadsheet_id[:8]}... modified since last sync")
            return True
        else:
            log.info(f"Spreadsheet {spreadsheet_id[:8]}... not modified, skipping")
            return False

    async def extract_sheet_data(self, spreadsheet_id: str, gid: str, range_name: str, target_table: str, check_modified: bool = False) -> Tuple[List[str], List[List[Any]]]:
        """Извлекает данные из конкретного листа с повторными попытками."""
        
        # Опциональная проверка modifiedTime для инкрементальной оптимизации
        if check_modified and not self.is_spreadsheet_modified(spreadsheet_id):
            log.info(f"Skipping {target_table} — spreadsheet not modified")
            return [], []
        
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
