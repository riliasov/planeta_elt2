
import asyncio
import os
import asyncpg
from src.config.settings import settings

async def check_data():
    conn = await asyncpg.connect(settings.database_dsn, statement_cache_size=0)
    try:
        # 1. Count Core Tables
        print("\n--- Core Tables Row Counts ---")
        for table in ['core.clients', 'core.sales', 'core.schedule']:
            try:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                print(f"{table}: {count}")
            except Exception as e:
                print(f"{table}: ERROR {e}")

        # 2. Check Client Balances View
        print("\n--- Analytics: Client Balances (Top 5) ---")
        try:
            rows = await conn.fetch("SELECT * FROM analytics.v_client_balances LIMIT 5")
            if rows:
                headers = rows[0].keys()
                print(" | ".join(headers))
                print("-" * 80)
                for r in rows:
                    print(" | ".join([str(r[h]) for h in headers]))
            else:
                print("View is empty.")
        except Exception as e:
            print(f"FAILED to query view: {e}")
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_data())
