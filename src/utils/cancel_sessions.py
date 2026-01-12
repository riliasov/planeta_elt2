import asyncio
from src.db.connection import DBConnection

async def main():
    try:
        # Отменяем текущие запросы блокирующих сессий
        pids = [2145451, 2145460, 2145462]
        for pid in pids:
            try:
                print(f"Cancelling session {pid}...")
                await DBConnection.execute(f"SELECT pg_cancel_backend({pid})")
            except Exception as e:
                print(f"Failed to cancel {pid}: {e}")
        print("✅ Cancel requests sent.")
    except Exception as e:
        print(f"❌ Error cancelling sessions: {e}")
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
