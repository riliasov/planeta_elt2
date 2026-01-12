import asyncio
from src.db.connection import DBConnection

async def main():
    try:
        rows = await DBConnection.fetch("SELECT current_user")
        print(f"CURRENT ROLE: {rows[0][0]}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
