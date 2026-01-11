import asyncio
from src.db.connection import DBConnection
from src.config.settings import settings

async def check():
    query = """
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'auth', 'storage', 'graphql', 'graphql_public', 'realtime', 'vault')
        ORDER BY table_schema, table_name;
    """
    rows = await DBConnection.fetch(query)
    print("CURRENT DB STRUCTURE:")
    current_schema = None
    for r in rows:
        if r['table_schema'] != current_schema:
            print(f"\n[{r['table_schema']}]")
            current_schema = r['table_schema']
        print(f"  - {r['table_name']}")

if __name__ == '__main__':
    asyncio.run(check())
