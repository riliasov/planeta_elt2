import asyncio
import asyncpg
from src.config.settings import settings

async def main():
    try:
        conn = await asyncpg.connect(settings.database_dsn)
        rows = await conn.fetch("""
            SELECT pid, pg_blocking_pids(pid) as blocked_by, query 
            FROM pg_stat_activity 
            WHERE cardinality(pg_blocking_pids(pid)) > 0
        """)
        if rows:
            print("BLOCKING PIDS FOUND:")
            for r in rows:
                print(f"PID {r['pid']} blocked by {r['blocked_by']} | Query: {r['query'][:100]}")
        else:
            print("No locks found.")
        await conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
