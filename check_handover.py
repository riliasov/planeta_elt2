import asyncio
from src.db.connection import DBConnection
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('check_handover')

async def check_table_exists(schema, table):
    query = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)"
    result = await DBConnection.fetch(query, schema, table)
    return result[0]['exists']

async def check_routine_exists(schema, routine):
    query = "SELECT EXISTS (SELECT FROM information_schema.routines WHERE routine_schema = $1 AND routine_name = $2)"
    result = await DBConnection.fetch(query, schema, routine)
    return result[0]['exists']

async def get_table_columns(schema, table):
    query = "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2"
    result = await DBConnection.fetch(query, schema, table)
    return [row['column_name'] for row in result]

async def check_telegram_leads():
    try:
        query = "SELECT count(*), count(lead_date) as with_date FROM telegram.telegram_users WHERE is_lead = true"
        result = await DBConnection.fetch(query)
        return result[0]
    except Exception as e:
        log.error(f"Error checking telegram leads: {e}")
        return None

async def main():
    print("--- Handover Check Report ---")
    
    # 1. Check RPC Functions
    print("\n[RPC Functions]")
    for func in ['get_top_users', 'get_telegram_leads_period']:
        exists = await check_routine_exists('public', func)
        print(f"  {func}: {'✅ Exists' if exists else '❌ Missing'}")

    # 2. Check Tables for missing entities
    print("\n[Missing Entities Check]")
    # Tasks
    core_tasks = await check_table_exists('core', 'tasks')
    webapp_tasks = await check_table_exists('webapp', 'tasks')
    print(f"  core.tasks: {'✅' if core_tasks else '❌'}")
    print(f"  webapp.tasks: {'✅' if webapp_tasks else '❌'}")
    
    if webapp_tasks:
        cols = await get_table_columns('webapp', 'tasks')
        print(f"    webapp.tasks Columns: {', '.join(cols)}")
    
    # Booking
    print("\n[Booking Access Check]")
    api_schedule = await check_table_exists('public', 'api_schedule')
    api_sales = await check_table_exists('public', 'api_sales')
    print(f"  public.api_schedule: {'✅' if api_schedule else '❌'}")
    print(f"  public.api_sales: {'✅' if api_sales else '❌'}")
    
    if api_schedule:
        cols = await get_table_columns('public', 'api_schedule')
        print(f"    Columns: {', '.join(cols)}")
    if api_sales:
        cols = await get_table_columns('public', 'api_sales')
        print(f"    Columns: {', '.join(cols)}")

    # 3. Check Telegram Data
    print("\n[Telegram Data Integrity]")
    tg_users_cols = await get_table_columns('telegram', 'telegram_users')
    if 'lead_date' in tg_users_cols:
        print("  lead_date column: ✅ Exists")
    else:
        print("  lead_date column: ❌ Missing")
        
    leads_stats = await check_telegram_leads()
    if leads_stats:
        print(f"  Total Leads: {leads_stats['count']}")
        print(f"  Leads with date: {leads_stats['with_date']}")
        if leads_stats['count'] > 0 and leads_stats['count'] != leads_stats['with_date']:
             print("  ⚠️ WARNING: Some leads are missing lead_date!")
    
    await DBConnection.close()

if __name__ == "__main__":
    asyncio.run(main())
