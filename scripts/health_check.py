"""Комплексная диагностика системы: проверка окружения, подключений, схем БД."""

import asyncio
import os
import json
import asyncpg
from pathlib import Path
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from src.config.settings import settings
from src.db.connection import DBConnection

load_dotenv()


async def check_environment():
    """Проверка переменных окружения и файлов."""
    print("\n=== 1. ОКРУЖЕНИЕ ===")
    
    # .env файл
    env_path = Path('.env')
    if env_path.exists():
        print("✅ Файл .env найден")
    else:
        print("❌ Файл .env отсутствует")
    
    # Переменные
    required_vars = ['SUPABASE_DB_URL', 'GOOGLE_SERVICE_ACCOUNT_JSON']
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var} = {'*' * 10} (скрыто)")
        else:
            print(f"❌ {var} отсутствует")


async def check_google_auth():
    """Проверка аутентификации Google Sheets."""
    print("\n=== 2. GOOGLE SHEETS ===")
    
    creds_path = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON', 'secrets/google-service-account.json')
    if not Path(creds_path).exists():
        print(f"❌ Ключ Google не найден: {creds_path}")
        return
    
    try:
        with open(creds_path, 'r') as f:
            creds_info = json.load(f)
        scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        Credentials.from_service_account_info(creds_info, scopes=scopes)
        print(f"✅ Google Service Account валиден ({creds_path})")
    except Exception as e:
        print(f"❌ Ошибка валидации ключа Google: {e}")


async def check_database():
    """Проверка подключения к БД и структуры схем."""
    print("\n=== 3. БАЗА ДАННЫХ ===")
    
    dsn = os.getenv('SUPABASE_DB_URL')
    if not dsn:
        print("❌ SUPABASE_DB_URL не найден")
        return
    
    try:
        conn = await asyncpg.connect(dsn, statement_cache_size=0, timeout=10)
        version = await conn.fetchval("SELECT version()")
        print(f"✅ Подключение успешно: {version[:50]}...")
        
        # Проверка текущего пользователя
        current_user = await conn.fetchval("SELECT current_user")
        print(f"✅ Подключен как: {current_user}")
        
        # Проверка основных схем
        print("\n--- Схемы БД ---")
        schemas = await conn.fetch("""
            SELECT schema_name FROM information_schema.schemata 
            WHERE schema_name IN ('raw', 'stg_gsheets', 'core', 'ops', 'analytics', 'lookups', 'webapp', 'telegram')
            ORDER BY schema_name
        """)
        found_schemas = [r['schema_name'] for r in schemas]
        expected_schemas = ['raw', 'stg_gsheets', 'core', 'ops', 'analytics', 'lookups']
        
        for schema in expected_schemas:
            if schema in found_schemas:
                # Count tables
                tables = await conn.fetch(f"""
                    SELECT count(*) as cnt 
                    FROM information_schema.tables 
                    WHERE table_schema = '{schema}'
                """)
                table_count = tables[0]['cnt']
                print(f"✅ {schema} ({table_count} таблиц)")
            else:
                print(f"❌ {schema} отсутствует")
        
        # Дополнительные схемы
        optional = set(found_schemas) - set(expected_schemas)
        if optional:
            print(f"ℹ️  Дополнительные схемы: {', '.join(optional)}")
        
        # Проверка алембик
        print("\n--- Alembic Миграции ---")
        try:
            version_check = await conn.fetch("SELECT version_num FROM alembic_version_core LIMIT 1")
            if version_check:
                print(f"✅ Текущая ревизия: {version_check[0]['version_num']}")
        except Exception:
            print("⚠️  Таблица alembic_version_core не найдена")
        
        await conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")


async def check_data_samples():
    """Проверка данных в ключевых таблицах."""
    print("\n=== 4. ДАННЫЕ (примеры) ===")
    
    try:
        # Core tables counts
        for table in ['core.clients', 'core.sales', 'core.schedule']:
            try:
                count = await DBConnection.fetchval(f"SELECT COUNT(*) FROM {table}")
                print(f"✅ {table}: {count} строк")
            except Exception as e:
                print(f"❌ {table}: {e}")
        
        await DBConnection.close()
    except Exception as e:
        print(f"❌ Ошибка проверки данных: {e}")


async def main():
    print("=" * 60)
    print("ДИАГНОСТИКА СИСТЕМЫ PL-ETL-CORE")
    print("=" * 60)
    
    await check_environment()
    await check_google_auth()
    await check_database()
    await check_data_samples()
    
    print("\n" + "=" * 60)
    print("Диагностика завершена.")
    print("Инструкции: см. README.md и docs/HANDOVER.md")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
