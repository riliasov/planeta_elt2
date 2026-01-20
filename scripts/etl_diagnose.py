#!/usr/bin/env python3
"""ETL Diagnostic Tool: Проверка доступа к Google Sheets и анализ структуры листов.

Использование:
    python scripts/etl_diagnose.py <spreadsheet_id> [--gid <gid>] [--all]

Примеры:
    # Проверить доступ и показать все листы
    python scripts/etl_diagnose.py 1CHYvprkr6hDCujoqc8JE3j5cNIFktywTWwYWx9lqTHE --all
    
    # Проверить конкретный лист
    python scripts/etl_diagnose.py 1CHYvprkr6hDCujoqc8JE3j5cNIFktywTWwYWx9lqTHE --gid 294381083
"""
import argparse
import gspread
import json
import sys
from google.oauth2.service_account import Credentials
from pathlib import Path

# CDC метаданные (все должны присутствовать для валидного листа)
CDC_METADATA_COLS = {'record_id', 'content_hash', 'created_at', 'updated_at', 'updated_by'}


def load_credentials(creds_path: str = "secrets/google-service-account.json"):
    """Загружает credentials для Google API."""
    with open(creds_path, 'r') as f:
        creds_info = json.load(f)
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    return Credentials.from_service_account_info(creds_info, scopes=scopes)


def check_access(spreadsheet_id: str, creds: Credentials) -> tuple:
    """Проверяет доступ к таблице. Возвращает (success, spreadsheet/error)."""
    gc = gspread.authorize(creds)
    try:
        sh = gc.open_by_key(spreadsheet_id)
        return True, sh
    except gspread.exceptions.APIError as e:
        return False, f"API Error: {e}"
    except Exception as e:
        return False, f"Error: {type(e).__name__}: {e}"


def find_cdc_header_row(worksheet, scan_limit: int = 20) -> dict:
    """Находит строку с CDC метаданными (самую нижнюю если несколько).
    
    Returns:
        dict с ключами: header_row, data_start_row, headers, missing_cols
    """
    data = worksheet.get(f"A1:ZZ{scan_limit}")
    if not data:
        return {"error": "No data found"}
    
    last_match = None
    
    for row_idx, row in enumerate(data):
        # Нормализуем заголовки (lowercase, strip)
        normalized = {str(cell).strip().lower() for cell in row if cell}
        
        # Проверяем наличие всех CDC колонок
        found_cols = CDC_METADATA_COLS.intersection(normalized)
        missing_cols = CDC_METADATA_COLS - found_cols
        
        if len(found_cols) == len(CDC_METADATA_COLS):
            # Все колонки найдены — запоминаем (ищем самую нижнюю)
            last_match = {
                "header_row": row_idx + 1,
                "data_start_row": row_idx + 2,
                "headers": row,
                "missing_cols": []
            }
    
    if last_match:
        return last_match
    
    # Если полного совпадения нет, возвращаем частичное
    return {
        "error": "CDC metadata row not found",
        "scan_limit": scan_limit,
        "hint": f"Expected columns: {', '.join(sorted(CDC_METADATA_COLS))}"
    }


def estimate_size_mb(row_count: int, col_count: int, bytes_per_cell: int = 50) -> float:
    """Оценивает размер данных в MB (примерно 50 байт на ячейку)."""
    return (row_count * col_count * bytes_per_cell) / (1024 * 1024)


def analyze_sheet(worksheet, verbose: bool = True) -> dict:
    """Анализирует структуру листа."""
    result = {
        "title": worksheet.title,
        "gid": worksheet.id,
        "row_count": worksheet.row_count,
        "col_count": worksheet.col_count
    }
    
    # Оценка размера
    size_mb = estimate_size_mb(worksheet.row_count, worksheet.col_count)
    result["size_mb"] = size_mb
    
    # Находим строку с CDC метаданными
    cdc_info = find_cdc_header_row(worksheet)
    result.update(cdc_info)
    
    if verbose:
        print(f"\n{'='*60}")
        print(f"Sheet: {worksheet.title} (GID: {worksheet.id})")
        print(f"Size: {worksheet.row_count} rows x {worksheet.col_count} cols (~{size_mb:.2f} MB)")
        
        if "error" in cdc_info:
            print(f"⚠️  {cdc_info['error']}")
            if "hint" in cdc_info:
                print(f"   Hint: {cdc_info['hint']}")
        else:
            print(f"✅ CDC Header Row: {cdc_info['header_row']}")
            print(f"   Data starts at row: {cdc_info['data_start_row']}")
            print(f"   Headers: {cdc_info['headers'][:10]}...")
    
    return result


def main():
    parser = argparse.ArgumentParser(description="ETL Diagnostic Tool")
    parser.add_argument("spreadsheet_id", help="Google Spreadsheet ID")
    parser.add_argument("--gid", type=int, help="Specific sheet GID to analyze")
    parser.add_argument("--all", action="store_true", help="Analyze all sheets")
    parser.add_argument("--creds", default="secrets/google-service-account.json", 
                        help="Path to service account JSON")
    
    args = parser.parse_args()
    
    # Проверяем credentials
    if not Path(args.creds).exists():
        print(f"❌ Credentials file not found: {args.creds}")
        sys.exit(1)
    
    creds = load_credentials(args.creds)
    print(f"🔑 Using service account from: {args.creds}")
    
    # Проверяем доступ
    print(f"\n📊 Checking access to: {args.spreadsheet_id[:20]}...")
    success, result = check_access(args.spreadsheet_id, creds)
    
    if not success:
        print(f"❌ Access denied: {result}")
        print(f"\n💡 Grant access to service account email in Google Sheets sharing settings.")
        sys.exit(1)
    
    sh = result
    print(f"✅ Access granted! Title: \"{sh.title}\"")
    
    # Список всех листов
    worksheets = sh.worksheets()
    print(f"\n📋 Found {len(worksheets)} sheets:")
    for ws in worksheets:
        print(f"   - {ws.title} (GID: {ws.id})")
    
    # Анализ
    if args.gid:
        ws = sh.get_worksheet_by_id(args.gid)
        if ws:
            analyze_sheet(ws)
        else:
            print(f"❌ Sheet with GID {args.gid} not found")
            sys.exit(1)
    elif args.all:
        for ws in worksheets:
            analyze_sheet(ws)
    else:
        # По умолчанию — первый лист
        analyze_sheet(worksheets[0])
        print(f"\n💡 Use --all to analyze all sheets, or --gid <id> for specific sheet")


if __name__ == "__main__":
    main()
