import pytest
import yaml
from pathlib import Path
from src.db.connection import DBConnection
from src.etl.validator import ContractValidator

@pytest.mark.asyncio
async def test_schema_vs_contracts_integrity():
    """Проверяет соответствие колонок в БД JSON-контрактам."""
    root_dir = Path(__file__).resolve().parent.parent
    sources_path = root_dir / 'sources.yml'
    contracts_dir = root_dir / 'src' / 'contracts'
    
    with open(sources_path, 'r', encoding='utf-8') as f:
        sources = yaml.safe_load(f)

    validator = ContractValidator(contracts_dir)
    
    # CDC колонки + технические
    TECH_COLS = {'record_id', 'content_hash', 'created_at', 'updated_at', 'updated_by', '_row_index', '__row_hash'}

    for spreadsheet_id, config in sources.items():
        if not isinstance(config, dict) or 'sheets' not in config:
            continue
            
        for sheet in config['sheets']:
            target_table = sheet['target_table']
            if '.' not in target_table:
                schema, table = 'public', target_table
            else:
                schema, table = target_table.split('.', 1)
            
            # Определяем имя контракта
            table_base = table.replace('_cur', '').replace('_hst', '')
            contract_name = 'schedule' if table_base == 'trainings' else table_base
            
            try:
                contract = validator.load_contract(contract_name)
                expected_cols = {c['name'].lower() for c in contract.get('columns', [])}
                expected_cols.update(TECH_COLS)
            except FileNotFoundError:
                continue # Скипаем если нет контракта

            # Получаем колонки из БД
            query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = $1 AND table_name = $2
            """
            async with await DBConnection.get_connection() as conn:
                db_rows = await conn.fetch(query, schema, table)
            
            assert db_rows, f"Таблица {target_table} не найдена в БД"
            
            db_cols = {r['column_name'].lower() for r in db_rows}
            
            # Проверяем что все из контракта есть в БД
            missing = expected_cols - db_cols
            assert not missing, f"В таблице {target_table} отсутствуют колонки: {missing}"
