"""Трансформация данных из staging (*_cur) в public таблицы."""

import asyncio
import asyncpg
import logging
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s | %(name)s | %(message)s'
)
log = logging.getLogger('transform')


# Маппинг: staging таблица → (public таблица, SQL трансформации)
TRANSFORMATIONS = {
    'clients_cur': {
        'target': 'clients',
        'sql': '''
            INSERT INTO clients (
                legacy_id, name, phone, child_name, child_dob, 
                age, spent, balance, debt, status
            )
            SELECT 
                NULLIF(TRIM("pk"), '') as legacy_id,
                COALESCE(NULLIF(TRIM("фио"), ''), 'Без имени') as name,
                COALESCE(NULLIF(TRIM("телефон"), ''), '+70000000000') as phone,
                NULLIF(TRIM("ребенок"), '') as child_name,
                CASE 
                    WHEN "дата_рождения" ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$' 
                    THEN TO_DATE("дата_рождения", 'DD.MM.YYYY')
                    ELSE NULL 
                END as child_dob,
                NULLIF(TRIM("возраст"), '') as age,
                COALESCE(NULLIF(TRIM("всего_потрачено"), '')::numeric, 0) as spent,
                COALESCE(NULLIF(TRIM("остаток"), '')::numeric, 0) as balance,
                COALESCE(NULLIF(TRIM("долг"), '')::numeric, 0) as debt,
                NULLIF(TRIM("статус"), '') as status
            FROM clients_cur
            WHERE NULLIF(TRIM("фио"), '') IS NOT NULL
            ON CONFLICT (legacy_id) DO UPDATE SET
                name = EXCLUDED.name,
                phone = EXCLUDED.phone,
                child_name = EXCLUDED.child_name,
                child_dob = EXCLUDED.child_dob,
                age = EXCLUDED.age,
                spent = EXCLUDED.spent,
                balance = EXCLUDED.balance,
                debt = EXCLUDED.debt,
                status = EXCLUDED.status,
                updated_at = NOW()
        '''
    },
    
    'trainings_cur': {
        'target': 'schedule',
        'sql': '''
            INSERT INTO schedule (
                legacy_id, date, start_time, end_time,
                status, type, category, comment
            )
            SELECT 
                NULLIF(TRIM("pk"), '') as legacy_id,
                CASE 
                    WHEN "дата" ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$' 
                    THEN TO_DATE("дата", 'DD.MM.YYYY')
                    WHEN "дата" ~ '^\\d{4}-\\d{2}-\\d{2}$'
                    THEN "дата"::date
                    ELSE CURRENT_DATE 
                END as date,
                COALESCE(NULLIF(TRIM("начало"), ''), '09:00')::time as start_time,
                COALESCE(NULLIF(TRIM("конец"), ''), '09:30')::time as end_time,
                COALESCE(NULLIF(TRIM("статус"), ''), 'Свободно') as status,
                NULLIF(TRIM("тип"), '') as type,
                NULLIF(TRIM("категория"), '') as category,
                NULLIF(TRIM("комментарий"), '') as comment
            FROM trainings_cur
            WHERE NULLIF(TRIM("дата"), '') IS NOT NULL
            ON CONFLICT (legacy_id) DO UPDATE SET
                date = EXCLUDED.date,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                status = EXCLUDED.status,
                type = EXCLUDED.type,
                category = EXCLUDED.category,
                comment = EXCLUDED.comment,
                updated_at = NOW()
        '''
    },
    
    'sales_cur': {
        'target': 'sales',
        'sql': '''
            INSERT INTO sales (
                legacy_id, date, product_name, type, category,
                quantity, full_price, discount, final_price,
                cash, transfer, terminal, debt, comment
            )
            SELECT 
                NULLIF(TRIM("pk"), '') as legacy_id,
                CASE 
                    WHEN "дата" ~ '^\\d{2}\\.\\d{2}\\.\\d{4}$' 
                    THEN TO_DATE("дата", 'DD.MM.YYYY')::timestamptz
                    ELSE NOW() 
                END as date,
                COALESCE(NULLIF(TRIM("товар"), ''), 'Неизвестно') as product_name,
                NULLIF(TRIM("тип"), '') as type,
                NULLIF(TRIM("категория"), '') as category,
                COALESCE(NULLIF(TRIM("количество"), '')::integer, 1) as quantity,
                COALESCE(NULLIF(TRIM("цена"), '')::numeric, 0) as full_price,
                COALESCE(NULLIF(TRIM("скидка"), '')::numeric, 0) as discount,
                COALESCE(NULLIF(TRIM("итого"), '')::numeric, 0) as final_price,
                COALESCE(NULLIF(TRIM("наличные"), '')::numeric, 0) as cash,
                COALESCE(NULLIF(TRIM("перевод"), '')::numeric, 0) as transfer,
                COALESCE(NULLIF(TRIM("терминал"), '')::numeric, 0) as terminal,
                COALESCE(NULLIF(TRIM("долг"), '')::numeric, 0) as debt,
                NULLIF(TRIM("комментарий"), '') as comment
            FROM sales_cur
            WHERE NULLIF(TRIM("товар"), '') IS NOT NULL
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
                updated_at = NOW()
        '''
    }
}


async def check_table_exists(conn, table: str) -> bool:
    """Проверяет существование таблицы."""
    result = await conn.fetchval('''
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = $1
        )
    ''', table)
    return result


async def get_column_names(conn, table: str) -> list[str]:
    """Получает список колонок таблицы."""
    rows = await conn.fetch('''
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = $1
        ORDER BY ordinal_position
    ''', table)
    return [r['column_name'] for r in rows]


async def transform_table(conn, source: str, config: dict) -> dict:
    """Выполняет трансформацию одной таблицы."""
    target = config['target']
    sql = config['sql']
    
    # Проверяем существование таблиц
    if not await check_table_exists(conn, source):
        log.warning(f"Исходная таблица {source} не существует — пропускаем")
        return {'source': source, 'status': 'skipped', 'reason': 'source not found'}
    
    if not await check_table_exists(conn, target):
        log.warning(f"Целевая таблица {target} не существует — пропускаем")
        return {'source': source, 'status': 'skipped', 'reason': 'target not found'}
    
    # Получаем колонки для анализа
    source_cols = await get_column_names(conn, source)
    log.info(f"Колонки {source}: {source_cols[:5]}...")
    
    try:
        # Выполняем трансформацию
        result = await conn.execute(sql)
        log.info(f"{source} → {target}: {result}")
        return {'source': source, 'target': target, 'status': 'success', 'result': result}
    except Exception as e:
        log.error(f"Ошибка трансформации {source}: {e}")
        return {'source': source, 'status': 'error', 'error': str(e)}


async def run_transforms():
    """Запускает все трансформации."""
    dsn = os.getenv('SUPABASE_DB_URL')
    if not dsn:
        log.error("SUPABASE_DB_URL не найден в .env")
        return
    
    conn = await asyncpg.connect(dsn, statement_cache_size=0)
    results = []
    
    try:
        log.info("=" * 50)
        log.info("ЗАПУСК ТРАНСФОРМАЦИЙ staging → public")
        log.info("=" * 50)
        
        for source, config in TRANSFORMATIONS.items():
            result = await transform_table(conn, source, config)
            results.append(result)
        
        # Итоги
        log.info("=" * 50)
        log.info("ИТОГИ ТРАНСФОРМАЦИЙ")
        success = sum(1 for r in results if r.get('status') == 'success')
        skipped = sum(1 for r in results if r.get('status') == 'skipped')
        errors = sum(1 for r in results if r.get('status') == 'error')
        log.info(f"Успешно: {success}, Пропущено: {skipped}, Ошибок: {errors}")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_transforms())
