import asyncio
from pathlib import Path
from src.db.connection import DBConnection

async def main():
    sql_path = Path('src/db/sql/init_layered_architecture.sql')
    if not sql_path.exists():
        print(f"Error: {sql_path} not found.")
        return
        
    sql = sql_path.read_text()
    print(f"Executing DDL from {sql_path}...")
    
    try:
        async with await DBConnection.get_connection() as conn:
            async with conn.transaction():
                await conn.execute(sql)
        print("✅ Core layer initialized successfully.")
    except Exception as e:
        print(f"❌ Error initializing core layer: {e}")
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
