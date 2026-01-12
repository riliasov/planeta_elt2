import asyncio
import argparse
from src.db.connection import DBConnection
from src.config.settings import settings

async def audit_permissions(target_role: str = None):
    print(f"=== Database Permission Audit ===")
    
    # 1. Определяем список схем
    target_schemas = ['core', 'stg_gsheets', 'lookups', 'ops', 'public', 'raw', 'analytics']
    
    # 2. Определяем список ролей для аудита
    if target_role:
        roles = [target_role]
    else:
        roles = ['anon', 'authenticated', 'service_role', 'postgres']

    try:
        async with await DBConnection.get_connection() as conn:
            # Получаем все таблицы в целевых схемах
            table_rows = await conn.fetch("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema = ANY($1)
                  AND table_type = 'BASE TABLE'
            """, target_schemas)
            
            if not table_rows:
                print("No tables found in target schemas.")
                return

            for role in roles:
                print(f"\nAudit for Role: [{role}]")
                print(f"{'Schema':<15} | {'Table':<30} | {'Access Level'}")
                print("-" * 65)
                
                for r in table_rows:
                    schema = r['table_schema']
                    table = r['table_name']
                    full_name = f"{schema}.{table}"
                    
                    # Проверяем привилегии
                    try:
                        # Уровни:
                        # GOD: Truncate + Trigger
                        # FULL: Select + Insert + Update + Delete
                        # VIEW: Select only
                        
                        can_select = await conn.fetchval(f"SELECT has_table_privilege('{role}', '{full_name}', 'SELECT')")
                        can_insert = await conn.fetchval(f"SELECT has_table_privilege('{role}', '{full_name}', 'INSERT')")
                        can_update = await conn.fetchval(f"SELECT has_table_privilege('{role}', '{full_name}', 'UPDATE')")
                        can_delete = await conn.fetchval(f"SELECT has_table_privilege('{role}', '{full_name}', 'DELETE')")
                        can_truncate = await conn.fetchval(f"SELECT has_table_privilege('{role}', '{full_name}', 'TRUNCATE')")
                        can_trigger = await conn.fetchval(f"SELECT has_table_privilege('{role}', '{full_name}', 'TRIGGER')")
                        
                        if can_truncate and can_trigger:
                            level = "🔥 god"
                        elif can_select and can_insert and can_update and can_delete:
                            level = "✅ full"
                        elif can_select:
                            level = "👁️ view"
                        else:
                            level = "❌ none"
                            
                        print(f"{schema:<15} | {table:<30} | {level}")
                    except Exception as e:
                        # Вероятно роль не существует
                        print(f"{schema:<15} | {table:<30} | ⚠️ ERROR (Role likely doesn't exist)")
                        break

    except Exception as e:
        print(f"Audit failed: {e}")
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Database Permission Audit')
    parser.add_argument('--role', help='Specific role to check (e.g. authenticated)')
    args = parser.parse_args()
    
    asyncio.run(audit_permissions(args.role))
