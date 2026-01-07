
import os
import json
import asyncio
import asyncpg
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

async def debug_all():
    print("=== Диагностика системы ===")
    
    # 1. Проверка .env
    env_path = '.env'
    if os.path.exists(env_path):
        print("[OK] Файл .env найден")
    else:
        print("[FAIL] Файл .env отсутствует")
    
    # 2. Проверка Google Credentials
    creds_path = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON', 'secrets/google-service-account.json')
    if os.path.exists(creds_path):
        try:
            with open(creds_path, 'r') as f:
                creds_info = json.load(f)
            scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
            Credentials.from_service_account_info(creds_info, scopes=scopes)
            print(f"[OK] Google Service Account ключ валиден ({creds_path})")
        except Exception as e:
            print(f"[FAIL] Ошибка в ключе Google: {e}")
    else:
        print(f"[FAIL] Ключ Google не найден по пути: {creds_path}")

    # 3. Проверка подключения к Supabase
    dsn = os.getenv('SUPABASE_DB_URL')
    if dsn:
        try:
            conn = await asyncpg.connect(dsn, statement_cache_size=0, timeout=10)
            version = await conn.fetchval("SELECT version()")
            print(f"[OK] Подключение к Supabase успешно: {version[:40]}...")
            await conn.close()
        except Exception as e:
            print(f"[FAIL] Ошибка подключения к Supabase: {e}")
    else:
        print("[FAIL] SUPABASE_DB_URL не найден в .env")

    print("\nИнструкции по исправлению см. в HANDOVER.md и README.md")

if __name__ == "__main__":
    asyncio.run(debug_all())
