import asyncio
from src.db.connection import DBConnection

async def main():
    try:
        # 1. Поиск блокировок
        locks = await DBConnection.fetch("""
            SELECT pid, usename, pg_blocking_pids(pid) as blocked_by, query, state
            FROM pg_stat_activity
            WHERE cardinality(pg_blocking_pids(pid)) > 0;
        """)
        
        if locks:
            print("🚨 Found blocking locks:")
            for l in locks:
                print(f"PID {l['pid']} is blocked by {l['blocked_by']}. Query: {l['query']}")
        else:
            print("✅ No blocking locks found.")

        # 2. Поиск всех активных сессий (кроме нашей)
        sessions = await DBConnection.fetch("""
            SELECT pid, usename, query, state, backend_start 
            FROM pg_stat_activity 
            WHERE pid <> pg_backend_pid() AND datname = current_database();
        """)
        
        print(f"\n👥 Active sessions ({len(sessions)}):")
        for s in sessions:
            print(f"PID: {s['pid']} | User: {s['usename']} | State: {s['state']} | Query: {s['query'][:50]}...")

    except Exception as e:
        print(f"Error checking locks: {e}")
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
