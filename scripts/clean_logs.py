import asyncio
from src.db.connection import DBConnection
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('cleaner')

async def main():
    log.info("Starting cleanup of validation logs...")
    
    # Delete logs created today (since midnight)
    query = """
    DELETE FROM validation_logs 
    WHERE created_at >= CURRENT_DATE;
    """
    
    try:
        # Initialize connection pool
        await DBConnection.get_connection()
        
        # Execute delete
        result = await DBConnection.execute(query)
        # result is usually a string like "DELETE 123"
        log.info(f"Cleanup result: {result}")
        
    except Exception as e:
        log.error(f"Cleanup failed: {e}")
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
