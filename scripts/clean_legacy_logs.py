
import asyncio
import asyncpg
from src.config.settings import settings

async def clean_logs():
    print(f"Connecting to {settings.database_dsn}...")
    # Disable statement cache for Supabase/PgBouncer
    conn = await asyncpg.connect(settings.database_dsn, statement_cache_size=0)
    
    try:
        print(f"Cleaning logs in schema '{settings.schema_ops}'...")
        
        # 1. Clean elt_table_stats
        # Delete entries that don't look like "schema.table" (i.e. no dot)
        # Note: Valid entries are like 'stg_gsheets.sales_cur'
        query_stats = f"""
            DELETE FROM {settings.schema_ops}.elt_table_stats 
            WHERE table_name NOT LIKE '%.%'
        """
        result_stats = await conn.execute(query_stats)
        print(f"Deleted from elt_table_stats: {result_stats}")

        # 2. Clean validation_logs
        query_val = f"""
            DELETE FROM {settings.schema_ops}.validation_logs 
            WHERE table_name NOT LIKE '%.%'
        """
        result_val = await conn.execute(query_val)
        print(f"Deleted from validation_logs: {result_val}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await conn.close()
        print("Done.")

if __name__ == "__main__":
    asyncio.run(clean_logs())
