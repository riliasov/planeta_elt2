
import gspread
import yaml
import json
import asyncio
import asyncpg
import re
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import os

load_dotenv()

def slugify(text):
    text = text.lower()
    text = re.sub(r'[\s\n]+', '_', text)
    text = re.sub(r'[^\w_]+', '', text)
    return text.strip('_')

async def load_data():
    with open('sources.yml', 'r') as f:
        config = yaml.safe_load(f)
    
    with open('secrets/google-service-account.json', 'r') as f:
        creds_info = json.load(f)
    
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)
    
    dsn = os.getenv('SUPABASE_DB_URL')
    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    
    try:
        for ssid, sdata in config.get('spreadsheets', {}).items():
            sh = gc.open_by_key(ssid)
            for sheet_cfg in sdata.get('sheets', []):
                target_table = sheet_cfg['target_table']
                range_name = sheet_cfg['range']
                gid = sheet_cfg['gid']
                
                print(f"Loading {target_table} from sheet {gid}...")
                worksheet = sh.get_worksheet_by_id(gid)
                
                # Fetch all data in the range
                data = worksheet.get(range_name)
                if not data:
                    print(f"No data for {target_table}")
                    continue
                
                headers = data[0]
                rows = data[1:]
                
                # Normalize headers to match table columns
                if target_table == 'rates':
                    col_names = [f"col_{i}" for i, _ in enumerate(headers)]
                else:
                    seen = set()
                    col_names = []
                    for h in headers:
                        col_name = slugify(h)
                        if not col_name: col_name = "unknown_col"
                        original_name = col_name
                        counter = 1
                        while col_name in seen:
                            col_name = f"{original_name}_{counter}"
                            counter += 1
                        seen.add(col_name)
                        col_names.append(col_name)
                
                # Truncate table
                await conn.execute(f'TRUNCATE TABLE "{target_table}"')
                
                # Prepare records for asyncpg copy_records_to_table
                # Add row index and timestamp
                prepared_rows = []
                for idx, r in enumerate(rows):
                    # Pad row with None if it's shorter than headers
                    full_row = list(r) + [None] * (len(col_names) - len(r))
                    # Or truncate if it's longer
                    full_row = full_row[:len(col_names)]
                    
                    # Convert everything to string for 'text' columns
                    full_row = [str(val) if val is not None else None for val in full_row]
                    
                    # Add _row_index (1-based from sheet)
                    # Headers are in data[0], so first row of data is index row_num in sheet
                    # But since range might start at B4, let's just use idx + 1 for now
                    prepared_rows.append(tuple(full_row + [idx + 1]))
                
                # Use copy_records_to_table for fast insert
                # Need to specify column names including _row_index
                target_cols = col_names + ["_row_index"]
                
                await conn.copy_records_to_table(
                    target_table,
                    records=prepared_rows,
                    columns=target_cols
                )
                print(f"Loaded {len(prepared_rows)} rows into {target_table}")
                
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(load_data())
