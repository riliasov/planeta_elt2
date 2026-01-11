import asyncio
from src.db.connection import DBConnection
from src.config.settings import settings

async def main():
    try:
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'staging'"
        tables = await DBConnection.fetch(query)
        print("Tables in staging:")
        for t in tables:
            print(f"- {t['table_name']}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
