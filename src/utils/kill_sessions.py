import asyncio
from src.db.connection import DBConnection

async def main():
    try:
        # Завершаем все сессии, которые не являются текущей и принадлежат нашей БД
        results = await DBConnection.fetch("""
            SELECT pid, pg_terminate_backend(pid) 
            FROM pg_stat_activity 
            WHERE pid <> pg_backend_pid() 
              AND datname = current_database();
        """)
        print(f"✅ Terminated {len(results)} sessions.")
    except Exception as e:
        print(f"❌ Error terminating sessions: {e}")
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
