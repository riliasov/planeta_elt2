
import asyncio
import sys
import argparse
import unittest.mock as mock
import logging
from src.main import main
from src.etl.pipeline import ELTPipeline
from src.etl.processor import TableProcessor

# Configure basic logging for the runner
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("chaos")

def crash_pipeline_run(*args, **kwargs):
    """Simulates a critical system failure."""
    log.critical("💥 CHAOS: Simulating Critical Pipeline Failure!")
    raise SystemError("CHAOS: Out of memory / Database connectivity lost")

async def crash_table_process(self, *args, **kwargs):
    """Simulates a failure for a specific table."""
    # args[3] is target_table in process_table(spreadsheet_id, sheet_cfg, ..., target_table?, mapping?)
    # Wait, process_table signature is: process_table(self, spreadsheet_id, sheet_cfg, full_refresh, dry_run)
    # sheet_cfg contains 'target_table'
    sheet_cfg = args[1] 
    target_table = sheet_cfg.get('target_table', 'unknown')
    
    if "sales" in target_table:
        log.error(f"💥 CHAOS: Simulating failure for table {target_table}")
        raise RuntimeError(f"CHAOS: Connection reset during {target_table}")
    
    # Call the original method if not the target
    # We need to access the UNPATCHED method. 
    # Since we are monkey patching the class, we can't easily call 'super' or 'original'.
    # Strategy: capture original before patching.
    return await ORIGINAL_PROCESS_TABLE(self, *args, **kwargs)

# Capture original methods
ORIGINAL_PROCESS_TABLE = TableProcessor.process_table

async def run_chaos(scenario: str):
    log.info(f"😈 Starting Chaos Runner with scenario: {scenario}")
    
    patchers = []
    
    try:
        if scenario == 'crash_pipeline':
            # Patch ELTPipeline.run to fail immediately
            p = mock.patch.object(ELTPipeline, 'run', side_effect=crash_pipeline_run)
            patchers.append(p)
            
        elif scenario == 'crash_table':
            # Patch TableProcessor.process_table to fail for 'sales'
            p = mock.patch.object(TableProcessor, 'process_table', side_effect=crash_table_process)
            patchers.append(p)
            
        # Start patches
        for p in patchers:
            p.start()
            
        # Run main pipeline
        # We need to simulate CLI args too? main() uses sys.argv
        sys.argv = ["main.py", "--scope", "current"]
        await main()
        
    except SystemExit as e:
        log.info(f"Pipeline finished with exit code: {e}")
    except Exception as e:
        log.error(f"Caught unexpected exception in runner: {e}")
    finally:
        # Stop patches
        for p in patchers:
            p.stop()
        log.info("😇 Chaos Mischief Managed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Chaos Testing for ELT Pipeline')
    parser.add_argument('--scenario', choices=['crash_pipeline', 'crash_table'], required=True,
                      help='Scenario to simulate')
    args = parser.parse_args()
    
    asyncio.run(run_chaos(args.scenario))
