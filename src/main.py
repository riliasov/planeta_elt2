import asyncio
import argparse
import sys
from src.utils.logger import setup_logger
from src.etl.pipeline import ELTPipeline
from src.db.connection import DBConnection

log = setup_logger()

async def main():
    parser = argparse.ArgumentParser(description='Planeta ELT Pipeline')
    parser.add_argument('--skip-load', action='store_true', help='Skip extraction and loading phase')
    parser.add_argument('--skip-transform', action='store_true', help='Skip transformation phase')
    parser.add_argument('--transform-only', action='store_true', help='Alias for --skip-load')
    parser.add_argument('--full-refresh', action='store_true', help='Force full refresh (truncate+insert)')
    parser.add_argument('--deploy-schema', action='store_true', help='Recreate staging tables schema from Sheets headers')
    
    args = parser.parse_args()
    
    # Logic normalization
    skip_load = args.skip_load or args.transform_only
    
    try:
        if args.deploy_schema:
            from src.etl.schema import SchemaManager
            manager = SchemaManager()
            await manager.deploy_staging_tables()
            # If we deployed schema, we likely want to load data too, unless skipped
            if not skip_load:
                 # Implicit full refresh advised after schema deploy
                 args.full_refresh = True
        
        pipeline = ELTPipeline()
        await pipeline.run(
            skip_load=skip_load,
            skip_transform=args.skip_transform,
            full_refresh=args.full_refresh
        )
    except Exception as e:
        log.critical(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
