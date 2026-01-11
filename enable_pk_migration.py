import asyncio
from src.db.connection import DBConnection
from src.config.settings import settings

TABLES = [
    "staging.sales_hst",
    "staging.clients_hst",
    "staging.expenses_hst",
    "staging.rates",
    "staging.trainings_hst",
    "staging.clients_cur",
    "staging.sales_cur",
    "staging.trainings_cur",
    "staging.expenses_cur",
    "staging.price_reference"
]

DDL_STATEMENTS = [
    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS record_id TEXT;",
    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS sheet_content_hash TEXT;",
    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS sheet_created_at TIMESTAMPTZ;",
    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS sheet_updated_at TIMESTAMPTZ;",
    "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS sheet_updated_by TEXT;",
    "TRUNCATE TABLE {table};",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_clean}_record_id ON {table} (record_id);"
]

async def main():
    print("Starting Schema Migration for PK CDC...")
    try:
        for table in TABLES:
            print(f"Processing table: {table}")
            table_clean = table.replace(".", "_")
            for statement in DDL_STATEMENTS:
                sql = statement.format(table=table, table_clean=table_clean)
                print(f"  Executing: {sql}")
                await DBConnection.execute(sql)
            print("  Done.")
        print("\nAll tables migrated and truncated successfully.")
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
