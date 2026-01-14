import asyncio
from src.db.connection import DBConnection
import logging
import os

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('apply_fix')

async def main():
    try:
        sql_path = 'scripts/fix_handover_schema.sql'
        if not os.path.exists(sql_path):
            log.error(f"SQL file not found: {sql_path}")
            return
            
        with open(sql_path, 'r') as f:
            sql = f.read()
            
        log.info(f"Executing SQL from {sql_path}...")
        await DBConnection.execute(sql)
        log.info("Migration applied successfully.")
        
    except Exception as e:
        log.error(f"Error applying fix: {e}")
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
