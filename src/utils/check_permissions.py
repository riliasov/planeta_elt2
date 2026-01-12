import asyncio
from src.db.connection import DBConnection
from src.config.settings import settings

async def main():
    print("=== Database Permission Check ===")
    try:
        # 1. Current User
        rows = await DBConnection.fetch("SELECT current_user, current_database();")
        user = rows[0]
        print(f"User: {user['current_user']} | DB: {user['current_database']}")
        
        # 2. Schema Privileges
        schemas = ['core', 'stg_gsheets', 'ops', 'public']
        print("\n--- Schema Usage Privileges ---")
        for s in schemas:
            res = await DBConnection.fetch(
                "SELECT has_schema_privilege($1, $2, 'USAGE')", user['current_user'], s
            )
            has_usage = res[0]['has_schema_privilege']
            print(f"Schema '{s}': {'✅ USAGE' if has_usage else '❌ DENIED'}")

        # 3. Table Privileges (Create)
        print("\n--- Create Privileges ---")
        for s in schemas:
            res = await DBConnection.fetch(
                "SELECT has_schema_privilege($1, $2, 'CREATE')", user['current_user'], s
            )
            has_create = res[0]['has_schema_privilege']
            print(f"Schema '{s}': {'✅ CREATE' if has_create else '❌ DENIED'}")

    except Exception as e:
        print(f"❌ Check failed: {e}")
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
