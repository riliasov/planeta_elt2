
import json
import asyncio
import asyncpg
import re
from dotenv import load_dotenv
import os

load_dotenv()

def slugify(text):
    text = text.lower()
    # Replace spaces and newlines with underscores
    text = re.sub(r'[\s\n]+', '_', text)
    # Remove non-alphanumeric except underscores
    # But since it's Russian, we might want to keep it or transliterate.
    # The user rule says "Docstrings и логи — в одну строку на русском языке", 
    # but for DB columns it's better to be safe.
    # Let's keep Russian letters but remove other trash.
    text = re.sub(r'[^\w_]+', '', text)
    return text.strip('_')

async def deploy():
    with open('headers.json', 'r') as f:
        headers_map = json.load(f)
    
    dsn = os.getenv('SUPABASE_DB_URL')
    if not dsn:
        print("Error: SUPABASE_DB_URL not found in .env")
        return
        
    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    
    try:
        for table_name, headers in headers_map.items():
            if not headers: continue
            
            # Special case for 'rates' which seems to have dates as headers in the sample
            if table_name == 'rates':
                # Simplified for rates: just text columns for now
                cols = [f"col_{i} text" for i, _ in enumerate(headers)]
            else:
                cols = []
                seen = set()
                for h in headers:
                    col_name = slugify(h)
                    if not col_name: col_name = "unknown_col"
                    original_name = col_name
                    counter = 1
                    while col_name in seen:
                        col_name = f"{original_name}_{counter}"
                        counter += 1
                    seen.add(col_name)
                    cols.append(f'"{col_name}" text')
            
            # Add a meta column for the original row index or hash if needed later
            cols.append('"_row_index" integer')
            cols.append('"_loaded_at" timestamp with time zone default now()')
            
            sql = f'DROP TABLE IF EXISTS "{table_name}"; CREATE TABLE "{table_name}" ({", ".join(cols)});'
            print(f"Deploying table {table_name}...")
            await conn.execute(sql)
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(deploy())
