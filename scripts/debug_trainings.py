"""Скрипт для отладки заголовков trainings_hst."""
import gspread
import json
from google.oauth2.service_account import Credentials

def debug_headers():
    creds_path = "secrets/google-service-account.json"
    spreadsheet_id = "1CHYvprkr6hDCujoqc8JE3j5cNIFktywTWwYWx9lqTHE"
    gid = 1318679629
    
    with open(creds_path, 'r') as f:
        creds_info = json.load(f)
    
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)
    
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.get_worksheet_by_id(gid)
    
    # Получаем первые 5 строк
    data = ws.get("A1:ZZ5")
    
    print(f"Sheet: {ws.title}")
    print(f"Total rows (approx): {ws.row_count}")
    print(f"\nFirst 5 rows:")
    for i, row in enumerate(data):
        print(f"Row {i+1}: {row[:15]}...")

if __name__ == "__main__":
    debug_headers()
