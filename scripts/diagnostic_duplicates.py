import asyncio
import argparse
from src.db.connection import DBConnection
from src.utils.logger import setup_logger

log = setup_logger()

async def investigate_duplicates(column_name: str = 'klient'):
    """Поиск дубликатов в staging таблицах по хешу или имени."""
    log.info(f"Исследование дубликатов в колонке: {column_name}")
    
    query = f"""
    WITH combined AS (
        SELECT 'sales_cur' as source, "{column_name}" as val, __row_hash FROM staging.sales_cur
        UNION ALL
        SELECT 'sales_hst' as source, "{column_name}" as val, __row_hash FROM staging.sales_hst
    )
    SELECT val, __row_hash, COUNT(*), ARRAY_AGG(source) as sources
    FROM combined
    GROUP BY val, __row_hash
    HAVING COUNT(*) > 1
    ORDER BY count DESC
    LIMIT 20;
    """
    
    try:
        rows = await DBConnection.fetch(query)
        if not rows:
            print("дубликаты по комбинации (значение + хеш) не найдены.")
            return

        print("\nНайденные дубликаты (одинаковый хеш в разных источниках или строках):")
        print("-" * 80)
        print(f"{'Значение':<30} | {'Хеш':<32} | {'Кол-во':<6} | {'Источники'}")
        print("-" * 80)
        for r in rows:
            print(f"{str(r['val'])[:30]:<30} | {r['__row_hash']:<32} | {r['count']:<6} | {r['sources']}")
            
    except Exception as e:
        log.error(f"Ошибка при выполнении диагностики: {e}")
    finally:
        await DBConnection.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--col', default='klient', help='Колонка для проверки')
    args = parser.parse_args()
    
    asyncio.run(investigate_duplicates(args.col))
