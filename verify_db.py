import asyncio
import os
from src.db.connection import DBConnection
from src.config.settings import settings

async def main():
    print("Checking database schemas and raw counts...")
    try:
        # Schema check
        schemas = await DBConnection.fetch("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('raw', 'staging', 'references', 'analytics')")
        print(f"Schemas found: {[r['schema_name'] for r in schemas]}")
        
        # Raw dump check
        raw_counts = await DBConnection.fetch("SELECT sheet_name, count(*) FROM raw.sheets_dump GROUP BY 1")
        print("\nRaw Dump Counts:")
        for r in raw_counts:
            print(f"  {r['sheet_name']}: {r['count']}")
            
        # Staging check
        staging_tables = await DBConnection.fetch("SELECT table_name FROM information_schema.tables WHERE table_schema = 'staging'")
        print(f"\nStaging Tables: {[r['table_name'] for r in staging_tables]}")
        
        db_url = settings.database_dsn
        print(f"\nConnected to: {db_url.split('@')[1] if '@' in db_url else db_url}")

    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
