import asyncio
import logging
from typing import Dict, List, Set
from src.db.connection import DBConnection
from src.config.settings import settings

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
log = logging.getLogger('schema-doctor')

class SchemaDoctor:
    """Инструмент для диагностики здоровья архитектуры базы данных."""
    
    TARGET_SCHEMAS = {
        'raw', 
        'stg_gsheets', 
        'core', 
        'ops',
        'lookups',
        'analytics'
    }
    
    EXPECTED_TABLES = {
        'ops': {'elt_runs', 'elt_table_stats', 'validation_logs'},
        'raw': {'sheets_dump'},
        'core': {'clients', 'sales', 'schedule', 'expenses'},
        'lookups': {'employees', 'products', 'expense_categories'}
    }

    async def diagnose(self):
        log.info("🩺 Запуск диагностики Schema Doctor...")
        log.info("-" * 50)
        
        # 1. Проверка схем
        actual_schemas = await self._get_schemas()
        missing_schemas = self.TARGET_SCHEMAS - actual_schemas
        
        if missing_schemas:
            log.error(f"❌ Отсутствуют схемы: {missing_schemas}")
        else:
            log.info("✅ Все целевые схемы (raw, stg, core, ops) присутствуют.")

        # 2. Проверка таблиц по схемам
        for schema, expected in self.EXPECTED_TABLES.items():
            if schema not in actual_schemas:
                continue
            
            actual_tables = await self._get_tables(schema)
            missing_tables = expected - actual_tables
            
            if missing_tables:
                log.warning(f"⚠️ Схема '{schema}': отсутствуют таблицы {missing_tables}")
            else:
                log.info(f"✅ Схема '{schema}': все ожидаемые таблицы на месте.")

        # 3. Проверка "мусора" в public
        public_tables = await self._get_tables('public')
        sensitive_in_public = public_tables.intersection(self.EXPECTED_TABLES['core'])
        
        if sensitive_in_public:
            log.error(f"🚨 КРИТИЧНО: Бизнес-таблицы обнаружены в схеме PUBLIC: {sensitive_in_public}")
            log.info("👉 Рекомендация: Перенесите их в схему 'core' и удалите из 'public'.")
        
        log.info("-" * 50)
        log.info("Диагностика завершена.")

    async def _get_schemas(self) -> Set[str]:
        rows = await DBConnection.fetch("""
            SELECT schema_name FROM information_schema.schemata
        """)
        return {r['schema_name'] for r in rows}

    async def _get_tables(self, schema: str) -> Set[str]:
        rows = await DBConnection.fetch("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = $1
        """, schema)
        return {r['table_name'] for r in rows}

async def main():
    doctor = SchemaDoctor()
    try:
        await doctor.diagnose()
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
