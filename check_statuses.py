import asyncio
from src.db.connection import DBConnection

async def main():
    rows = await DBConnection.fetch('SELECT status, count(*) FROM schedule GROUP BY 1')
    for r in rows:
        print(f"{r['status']}: {r['count']}")
    await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
