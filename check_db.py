
import asyncio
import asyncpg
from dotenv import load_dotenv
import os

load_dotenv()

async def check():
    dsn = os.getenv('SUPABASE_DB_URL')
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch("""
            SELECT table_name, column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'sales_hst'
        """)
        for r in rows:
            print(f"{r['table_name']}.{r['column_name']}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check())
