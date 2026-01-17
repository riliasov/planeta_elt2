
import asyncio
from src.db.connection import DBConnection
import logging

logging.basicConfig(level=logging.ERROR)

async def verify():
    pool = await DBConnection.get_pool()
    
    print("=== Verification Report ===")
    
    # 1. Check core.sales columns
    print("\n--- core.sales (admin, trainer) ---")
    rows = await DBConnection.fetch("""
        SELECT id, admin, trainer 
        FROM core.sales 
        WHERE admin IS NOT NULL OR trainer IS NOT NULL 
        LIMIT 5
    """)
    if rows:
        for r in rows:
            print(f"ID: {r['id']}, Admin: {r['admin']}, Trainer: {r['trainer']}")
    else:
        print("FAIL: No data found in admin/trainer columns!")

    # 2. Check core.schedule employee_id
    print("\n--- core.schedule (employee_id) ---")
    rows = await DBConnection.fetch("""
        SELECT s.id, s.employee_id, e.full_name
        FROM core.schedule s
        LEFT JOIN lookups.employees e ON s.employee_id = e.id
        WHERE s.employee_id IS NOT NULL
        LIMIT 5
    """)
    if rows:
        for r in rows:
            print(f"ID: {r['id']}, EmpID: {r['employee_id']}, Name: {r['full_name']}")
    else:
        print("FAIL: No data found in employee_id column!")

    # 3. Check public.api_sales
    print("\n--- public.api_sales (access check) ---")
    try:
        rows = await DBConnection.fetch("SELECT * FROM public.api_sales LIMIT 1")
        print("SUCCESS: public.api_sales is accessible")
        print(f"Columns: {rows[0].keys() if rows else 'Empty'}")
    except Exception as e:
        print(f"FAIL: public.api_sales error: {e}")

    await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(verify())
