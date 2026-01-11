import asyncio
from src.db.connection import DBConnection

async def main():
    rows = await DBConnection.fetch('SELECT * FROM staging.price_reference LIMIT 10')
    for r in rows:
        print(dict(r))
    await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
