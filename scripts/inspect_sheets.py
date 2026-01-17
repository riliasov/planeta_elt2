"""Инспектор Google Sheets: извлечение заголовков из всех настроенных листов."""

import gspread
import yaml
import json
from pathlib import Path
from google.oauth2.service_account import Credentials
from src.config.settings import settings


def get_headers_from_sheets():
    """Извлекает заголовки из всех листов, настроенных в sources.yml."""
    
    # Загрузка конфигурации
    config_path = Path('sources.yml')
    if not config_path.exists():
        print("❌ Файл sources.yml не найден")
        return
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Аутентификация Google
    creds_path = settings.google_credentials_path
    if not Path(creds_path).exists():
        print(f"❌ Ключ Google не найден: {creds_path}")
        return
    
    with open(creds_path, 'r') as f:
        creds_info = json.load(f)
    
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)
    
    results = {}
    
    print("=" * 60)
    print("ИНСПЕКТОР GOOGLE SHEETS: Извлечение заголовков")
    print("=" * 60)
    
    # Перебор всех таблиц и листов
    for ssid, sdata in config.get('spreadsheets', {}).items():
        print(f"\n📊 Таблица: {ssid}")
        try:
            sh = gc.open_by_key(ssid)
        except Exception as e:
            print(f"  ❌ Ошибка открытия: {e}")
            continue
        
        for sheet_cfg in sdata.get('sheets', []):
            target_table = sheet_cfg['target_table']
            gid = sheet_cfg.get('gid', 0)
            range_name = sheet_cfg.get('range', 'A:Z')
            
            try:
                worksheet = sh.get_worksheet_by_id(gid)
                
                # Извлекаем только первую строку заголовков
                # Парсим диапазон (например, "B4:W" -> "B4:W4")
                import re
                parts = range_name.split(':')
                start_cell = parts[0]
                match = re.search(r'\d+', start_cell)
                row_num = match.group() if match else "1"
                col_letter = re.sub(r'\d+', '', start_cell)
                end_col = re.sub(r'\d+', '', parts[1]) if len(parts) > 1 else col_letter
                header_range = f"{start_cell}:{end_col}{row_num}"
                
                headers = worksheet.get(header_range)
                if headers and headers[0]:
                    results[target_table] = headers[0]
                    print(f"  ✅ {target_table}: {len(headers[0])} колонок")
                    print(f"      {', '.join(headers[0][:5])}{'...' if len(headers[0]) > 5 else ''}")
                else:
                    print(f"  ⚠️  {target_table}: заголовки не найдены")
                    
            except Exception as e:
                print(f"  ❌ {target_table}: {e}")
    
    # Сохранение результата
    output_path = Path('headers.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print("\n" + "=" * 60)
    print(f"✅ Заголовки сохранены в: {output_path.absolute()}")
    print(f"   Всего таблиц: {len(results)}")
    print("=" * 60)


if __name__ == "__main__":
    get_headers_from_sheets()
