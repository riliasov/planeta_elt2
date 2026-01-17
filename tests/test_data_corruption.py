import sys
from pathlib import Path
import asyncio

# Add project root to path
root_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(root_dir))

from src.etl.loader import DataLoader
from src.etl.processor import TableProcessor
from src.etl.validator import ContractValidator, ValidationResult
from src.etl.extractor import GSheetsExtractor

class MockExtractor(GSheetsExtractor):
    def __init__(self):
        # Bypass real init which calls _authenticate
        pass

    async def extract_sheet_data(self, *args, **kwargs):
        # Имитируем данные: [Name, Junk, Date]
        col_names = ['name', 'junk_col', 'date_val']
        rows = [
            ['Alice', 'trash1', '2024-01-01'],
            ['Bob', 'trash2', '2024-02-02']
        ]
        return col_names, rows

class MockLoader(DataLoader):
    def __init__(self):
        super().__init__()
        self.last_load_call = None
        
    async def load_cdc(self, table, col_names, rows, pk_field):
        self.last_load_call = {
            'col_names': col_names,
            'rows': rows,
            'pk_field': pk_field
        }
        return {'inserted': len(rows)}
        
    async def calculate_changes(self, table, col_names, rows, pk_field): # Mock dry run
         return await self.load_cdc(table, col_names, rows, pk_field)

class MockValidator(ContractValidator):
    def load_contract(self, name):
        # Контракт знает только про name и date_val (junk_col пропущен)
        return {
            'columns': [
                {'name': 'name', 'type': 'string'},
                {'name': 'date_val', 'type': 'date'}
            ]
        }
    
    def validate_dataset(self, rows, name):
        return ValidationResult(is_valid=True, errors=[], total_rows=len(rows), valid_rows=len(rows))

async def test_data_corruption_fix():
    print("=== Testing Data Corruption Fix ===")
    
    extractor = MockExtractor()
    loader = MockLoader()
    validator = MockValidator()
    
    processor = TableProcessor(extractor, loader, validator, "test_run")
    
    # Config: PK is 'name'
    sheet_cfg = {
        'target_table': 'stg.test',
        'pk': 'name'
    }
    
    result = await processor.process_table("scope_id", sheet_cfg, full_refresh=False, dry_run=False)
    
    # Проверка
    call = loader.last_load_call
    output_cols = call['col_names']
    output_rows = call['rows']
    
    print(f"Output Cols: {output_cols}")
    print(f"Output Row 0: {output_rows[0]}")
    
    # Ожидаем: ['name', 'date_val'] (без junk_col)
    assert 'junk_col' not in output_cols, "FAIL: Junk column should be filtered out"
    assert 'date_val' in output_cols, "FAIL: Date column should be present"
    
    # Ожидаем row[0]: ['Alice', '2024-01-01']
    # Если баг есть: ['Alice', 'trash1'] (смещение)
    
    name_idx = output_cols.index('name')
    date_idx = output_cols.index('date_val')
    
    val_name = output_rows[0][name_idx]
    val_date = output_rows[0][date_idx]
    
    if val_date == 'trash1':
        print("❌ CRITICAL FAIL: Data Corruption Detected! Value shifted.")
        sys.exit(1)
        
    if val_date != '2024-01-01':
        print(f"❌ FAIL: Unexpected value '{val_date}' (expected '2024-01-01')")
        sys.exit(1)
        
    print("✅ SUCCESS: Data is correctly aligned.")
    
    # Проверка, что PK не исчез
    assert 'name' in output_cols, "FAIL: PK 'name' was lost!"

if __name__ == "__main__":
    asyncio.run(test_data_corruption_fix())
