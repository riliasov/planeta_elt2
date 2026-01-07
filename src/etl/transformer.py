"""Трансформация данных из staging (*_cur, *_hst) в public таблицы.

Стратегия: Unified Target Table.
Обе таблицы (current и history) сливаются в одну целевую.
Дедупликация по legacy_id (MD5 хеш ключевых полей).
"""
import logging
import asyncio
from src.db.connection import DBConnection

log = logging.getLogger('transformer')


# === SQL ШАБЛОНЫ ДЛЯ ТРАНСФОРМАЦИИ ===

# Общий шаблон для продаж (sales_cur и sales_hst)
SALES_SQL_TEMPLATE = '''
    INSERT INTO sales (
        legacy_id, date, product_name, type, category,
        quantity, full_price, discount, final_price,
        cash, transfer, terminal, debt, comment, client_id
    )
    SELECT DISTINCT ON (legacy_id)
        md5(COALESCE("{date_col}", '') || COALESCE("{client_col}", '') || COALESCE("{product_col}", '') || COALESCE("{price_col}", '')) as legacy_id,
        CASE 
            WHEN "{date_col}" ~ '\\d{{2}}\\.\\d{{2}}\\.\\d{{2}}' 
            THEN TO_DATE(substring("{date_col}" from '\\d{{2}}\\.\\d{{2}}\\.\\d{{2}}'), 'DD.MM.YY')::timestamptz
            WHEN "{date_col}" ~ '\\d{{2}}\\.\\d{{2}}\\.\\d{{4}}'
            THEN TO_DATE("{date_col}", 'DD.MM.YYYY')::timestamptz
            ELSE NOW() 
        END as date,
        COALESCE(NULLIF(TRIM("{product_col}"), ''), 'Неизвестно') as product_name,
        NULLIF(TRIM("{type_col}"), '') as type,
        NULLIF(TRIM("{category_col}"), '') as category,
        COALESCE(NULLIF(TRIM("{qty_col}"), '')::integer, 1) as quantity,
        COALESCE(NULLIF(regexp_replace("{price_col}", '[^0-9,.-]', '', 'g'), '')::numeric, 0) as full_price,
        COALESCE(NULLIF(regexp_replace("{discount_col}", '[^0-9,.-]', '', 'g'), '')::numeric, 0) as discount,
        COALESCE(NULLIF(regexp_replace("{final_col}", '[^0-9,.-]', '', 'g'), '')::numeric, 0) as final_price,
        COALESCE(NULLIF(regexp_replace("{cash_col}", '[^0-9,.-]', '', 'g'), '')::numeric, 0) as cash,
        COALESCE(NULLIF(regexp_replace("{transfer_col}", '[^0-9,.-]', '', 'g'), '')::numeric, 0) as transfer,
        COALESCE(NULLIF(regexp_replace("{terminal_col}", '[^0-9,.-]', '', 'g'), '')::numeric, 0) as terminal,
        COALESCE(NULLIF(regexp_replace("{debt_col}", '[^0-9,.-]', '', 'g'), '')::numeric, 0) as debt,
        NULLIF(TRIM("{comment_col}"), '') as comment,
        (SELECT id FROM clients c WHERE c.name = {source_table}."{client_col}" LIMIT 1) as client_id
    FROM {source_table}
    WHERE NULLIF(TRIM("{product_col}"), '') IS NOT NULL
      AND (SELECT id FROM clients c WHERE c.name = {source_table}."{client_col}" LIMIT 1) IS NOT NULL
    ORDER BY legacy_id
    ON CONFLICT (legacy_id) DO UPDATE SET
        date = EXCLUDED.date,
        product_name = EXCLUDED.product_name,
        type = EXCLUDED.type,
        category = EXCLUDED.category,
        quantity = EXCLUDED.quantity,
        full_price = EXCLUDED.full_price,
        discount = EXCLUDED.discount,
        final_price = EXCLUDED.final_price,
        cash = EXCLUDED.cash,
        transfer = EXCLUDED.transfer,
        terminal = EXCLUDED.terminal,
        debt = EXCLUDED.debt,
        comment = EXCLUDED.comment,
        client_id = EXCLUDED.client_id,
        updated_at = NOW()
'''

# Маппинг колонок для разных таблиц
SALES_CUR_COLUMNS = {
    'date_col': 'дата',
    'client_col': 'клиент',
    'product_col': 'продукт',
    'type_col': 'тип',
    'category_col': 'категория',
    'qty_col': 'количество',
    'price_col': 'полная_стоимость',
    'discount_col': 'скидка',
    'final_col': 'окончательная_стоимость',
    'cash_col': 'наличные',
    'transfer_col': 'перевод',
    'terminal_col': 'терминал',
    'debt_col': 'вдолг',
    'comment_col': 'комментарий',
    'source_table': 'sales_cur'
}

# U sales_hst колонки могут отличаться - проверяем и адаптируем
SALES_HST_COLUMNS = {
    'date_col': 'дата',
    'client_col': 'клиент',
    'product_col': 'продукт',
    'type_col': 'тип',
    'category_col': 'категория',
    'qty_col': 'количество',
    'price_col': 'полная_стоимость',
    'discount_col': 'скидка',
    'final_col': 'окончательная_стоимость',
    'cash_col': 'наличные',
    'transfer_col': 'перевод',
    'terminal_col': 'терминал',
    'debt_col': 'вдолг',
    'comment_col': 'комментарий',
    'source_table': 'sales_hst'
}


# === ПОЛНЫЙ МАППИНГ ТРАНСФОРМАЦИЙ ===

TRANSFORMATIONS = {
    # === CLIENTS ===
    'clients_cur': {
        'target': 'clients',
        'sql': '''
            INSERT INTO clients (
                legacy_id, name, phone, child_name, child_dob, 
                age, spent, balance, debt, status
            )
            SELECT DISTINCT ON (legacy_id)
                md5(COALESCE("клиент", '') || COALESCE("мобильный", '')) as legacy_id,
                COALESCE(NULLIF(TRIM("клиент"), ''), 'Без имени') as name,
                COALESCE(NULLIF(TRIM("мобильный"), ''), '+70000000000') as phone,
                NULLIF(TRIM("имя_ребенка"), '') as child_name,
                CASE 
                    WHEN "дата_рождения_ребенка" ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$' 
                    THEN TO_DATE("дата_рождения_ребенка", 'DD.MM.YYYY')
                    ELSE NULL 
                END as child_dob,
                NULL as age,
                0 as spent,
                0 as balance,
                0 as debt,
                NULLIF(TRIM("тип"), '') as status
            FROM clients_cur
            WHERE NULLIF(TRIM("клиент"), '') IS NOT NULL
            ORDER BY legacy_id
            ON CONFLICT (legacy_id) DO UPDATE SET
                name = EXCLUDED.name,
                phone = EXCLUDED.phone,
                child_name = EXCLUDED.child_name,
                child_dob = EXCLUDED.child_dob,
                status = EXCLUDED.status,
                updated_at = NOW()
        '''
    },
    
    # === SCHEDULE ===
    'trainings_cur': {
        'target': 'schedule',
        'sql': '''
            INSERT INTO schedule (
                legacy_id, date, start_time, end_time,
                status, type, category, comment, client_id
            )
            SELECT DISTINCT ON (legacy_id)
                md5(COALESCE("дата", '') || COALESCE("начало", '') || COALESCE("клиент", '')) as legacy_id,
                CASE 
                    WHEN "дата" ~ '^\\d{2}\\.\\d{2}\\.'
                        THEN TO_DATE("дата" || '2025', 'DD.MM.YYYY')
                    WHEN "дата" ~ '\\d{2}\\.\\d{2}\\.\\d{2}'
                        THEN TO_DATE(substring("дата" from '\\d{2}\\.\\d{2}\\.\\d{2}'), 'DD.MM.YY')
                    ELSE CURRENT_DATE
                END as date,
                COALESCE(NULLIF(TRIM("начало"), ''), '09:00')::time as start_time,
                COALESCE(NULLIF(TRIM("конец"), ''), '09:30')::time as end_time,
                COALESCE(NULLIF(TRIM("статус"), ''), 'Свободно') as status,
                NULLIF(TRIM("тип"), '') as type,
                NULLIF(TRIM("категория"), '') as category,
                NULLIF(TRIM("комментарий"), '') as comment,
                (SELECT id FROM clients c WHERE c.name = trainings_cur.клиент LIMIT 1) as client_id
            FROM trainings_cur
            WHERE NULLIF(TRIM("дата"), '') IS NOT NULL
              AND (SELECT id FROM clients c WHERE c.name = trainings_cur.клиент LIMIT 1) IS NOT NULL
            ORDER BY legacy_id
            ON CONFLICT (legacy_id) DO UPDATE SET
                date = EXCLUDED.date,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                status = EXCLUDED.status,
                type = EXCLUDED.type,
                category = EXCLUDED.category,
                comment = EXCLUDED.comment,
                client_id = EXCLUDED.client_id,
                updated_at = NOW()
        '''
    },
    
    # === SALES (Current) ===
    'sales_cur': {
        'target': 'sales',
        'sql': SALES_SQL_TEMPLATE.format(**SALES_CUR_COLUMNS)
    },
    
    # === SALES (History) - сливается в ту же таблицу sales ===
    'sales_hst': {
        'target': 'sales',
        'sql': SALES_SQL_TEMPLATE.format(**SALES_HST_COLUMNS)
    },
}


class Transformer:
    """Выполняет SQL-трансформации из staging в public таблицы."""
    
    async def run(self, tables: list[str] = None):
        """
        Запускает трансформации.
        
        :param tables: Список таблиц для обработки. Если None — все.
        """
        log.info("Starting transformations...")
        success_count = 0
        error_count = 0
        
        target_tables = tables if tables else list(TRANSFORMATIONS.keys())
        
        for source_table in target_tables:
            if source_table not in TRANSFORMATIONS:
                log.warning(f"Unknown table: {source_table}")
                continue
                
            config = TRANSFORMATIONS[source_table]
            
            try:
                # Проверяем существование исходной таблицы
                exists = await self._table_exists(source_table)
                if not exists:
                    log.warning(f"Skipping {source_table}: table does not exist")
                    continue
                
                log.info(f"Transforming {source_table} -> {config['target']}")
                await DBConnection.execute(config['sql'])
                log.info(f"✓ Successfully transformed {source_table}")
                success_count += 1
                
            except Exception as e:
                log.error(f"✗ Failed to transform {source_table}: {e}")
                error_count += 1
        
        log.info(f"Transformation finished. Success: {success_count}, Errors: {error_count}")
        return success_count, error_count
    
    async def _table_exists(self, table: str) -> bool:
        """Проверяет существование таблицы."""
        result = await DBConnection.fetch(
            "SELECT to_regclass($1::text) IS NOT NULL as exists", 
            table
        )
        return result[0]['exists'] if result else False


async def run_all_transformations():
    """Утилита для запуска всех трансформаций."""
    transformer = Transformer()
    return await transformer.run()
