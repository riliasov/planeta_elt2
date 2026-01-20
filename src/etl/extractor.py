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
            
            log.info("Успешная авторизация в Google Services (Sheets + Drive).")
        except Exception as e:
            log.error(f"Ошибка авторизации в Google: {e}")
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
                return datetime.fromisoformat(modified_str.replace('Z', '+00:00'))
            return None
        except Exception as e:
            log.warning(f"Не удалось получить modifiedTime для {spreadsheet_id}: {e}")
            return None

    def is_spreadsheet_modified(self, spreadsheet_id: str) -> bool:
        """Проверяет, изменился ли spreadsheet с последнего запроса."""
        current_time = self.get_modified_time(spreadsheet_id)
        if not current_time:
            return True
        
        cached_time = _modification_cache.get(spreadsheet_id)
        if not cached_time:
            _modification_cache[spreadsheet_id] = current_time
            return True
        
        if current_time > cached_time:
            _modification_cache[spreadsheet_id] = current_time
            log.info(f"Таблица {spreadsheet_id[:8]}... была изменена.")
            return True
        else:
            log.info(f"Таблица {spreadsheet_id[:8]}... не изменялась, пропуск.")
            return False

    # CDC метаданные для smart header detection
    CDC_METADATA_COLS = {'record_id', 'content_hash', 'created_at', 'updated_at', 'updated_by'}

    async def extract_sheet_data(self, spreadsheet_id: str, gid: str, range_name: str, target_table: str, 
                                 check_modified: bool = False, mapping: Optional[Dict[str, str]] = None) -> Tuple[List[str], List[List[Any]]]:
        """Извлекает данные из конкретного листа с повторными попытками.
        
        Если range_name='auto', автоматически находит строку с CDC метаданными.
        """
        
        if check_modified and not self.is_spreadsheet_modified(spreadsheet_id):
            log.info(f"Пропуск {target_table} — изменений в таблице не обнаружено.")
            return [], []
        
        log.info(f"Извлечение {target_table} из {spreadsheet_id[:8]}... (gid={gid})")
        
        for attempt in range(3):
            try:
                sh = self.gc.open_by_key(spreadsheet_id)
                ws = sh.get_worksheet_by_id(int(gid))
                
                if not ws:
                    raise ValueError(f"Лист с GID {gid} не найден в таблице {spreadsheet_id}")

                # Smart header detection
                if range_name.lower() == 'auto':
                    header_info = self._find_cdc_header_row(ws)
                    if header_info is None:
                        raise ValueError(f"CDC header row не найден в {target_table}")
                    header_row = header_info['header_row']
                    data_start_row = header_info['data_start_row']
                    log.info(f"Auto-detected: header row {header_row}, data starts at row {data_start_row}")
                    
                    # Читаем заголовки и данные отдельно
                    headers = ws.row_values(header_row)
                    data = ws.get(f"A{data_start_row}:ZZ")
                    rows = data if data else []
                else:
                    data = ws.get(range_name)
                    if not data:
                        log.warning(f"Данные не найдены для {target_table}")
                        return [], []
                    headers = data[0]
                    rows = data[1:]
                
                col_names = self._normalize_headers(headers, target_table, mapping)
                
                # Robust Mapping: выравниваем каждую строку под длину заголовков (padding)
                aligned_rows = []
                expected_len = len(headers)
                for r in rows:
                    if len(r) < expected_len:
                        r.extend([None] * (expected_len - len(r)))
                    aligned_rows.append(r[:expected_len])
                
                # Фильтрация полностью пустых строк
                aligned_rows = [r for r in aligned_rows if any(cell is not None and str(cell).strip() for cell in r)]

                return col_names, aligned_rows
                
            except Exception as e:
                if '429' in str(e):
                    sleep_time = (attempt + 1) * 5
                    log.warning(f"Лимит квот исчерпан для {target_table}, повтор через {sleep_time}с...")
                    import time
                    time.sleep(sleep_time)
                else:
                    log.error(f"Ошибка при извлечении данных для {target_table}: {e}")
                    raise
        raise Exception(f"Не удалось извлечь {target_table} после всех попыток.")

    def _find_cdc_header_row(self, worksheet, scan_limit: int = 20) -> Optional[Dict[str, int]]:
        """Находит строку с CDC метаданными (самую нижнюю если несколько)."""
        data = worksheet.get(f"A1:ZZ{scan_limit}")
        if not data:
            return None
        
        last_match = None
        
        for row_idx, row in enumerate(data):
            normalized = {str(cell).strip().lower() for cell in row if cell}
            found_cols = self.CDC_METADATA_COLS.intersection(normalized)
            
            if len(found_cols) == len(self.CDC_METADATA_COLS):
                last_match = {
                    "header_row": row_idx + 1,
                    "data_start_row": row_idx + 2
                }
        
        return last_match


    def _normalize_headers(self, headers: List[str], table_name: str, mapping: Optional[Dict[str, str]] = None) -> List[str]:
        """Превращает заголовки Sheet в валидные имена колонок Postgres."""
        if table_name == 'rates':
            return [f"col_{i}" for i, _ in enumerate(headers)]

        seen = set()
        col_names = []
        for h in headers:
            # 1. Сначала проверяем явный маппинг (по исходному имени)
            if mapping and h in mapping:
                col_name = mapping[h]
            else:
                # 2. Иначе slugify
                col_name = slugify(h) or "unknown_col"
            
            original = col_name
            counter = 1
            while col_name in seen:
                col_name = f"{original}_{counter}"
                counter += 1
            seen.add(col_name)
            col_names.append(col_name)
        return col_names
