-- MIGRATION PART 5: Public Wrappers (Secure Proxy)
-- Run this in Supabase SQL Editor

BEGIN;

-- 1. API Sales Wrapper
CREATE OR REPLACE VIEW public.api_sales AS
SELECT 
    id, date, product_name, type, category, 
    quantity, full_price, discount, final_price, 
    cash, transfer, terminal, debt, comment, 
    client_id, admin, trainer -- Included new columns
FROM core.sales
WHERE is_deleted = false
  AND date >= (NOW() - INTERVAL '90 days');

-- 2. API Schedule Wrapper
CREATE OR REPLACE VIEW public.api_schedule AS
SELECT 
    id, date, start_time, end_time, 
    status, type, category, comment, 
    client_id, employee_id -- Included new column
FROM core.schedule
WHERE is_deleted = false
  AND date >= (CURRENT_DATE - INTERVAL '90 days');

-- 3. API ETL Status Wrapper
CREATE OR REPLACE VIEW public.api_elt_status AS
SELECT * 
FROM ops.elt_runs
ORDER BY started_at DESC
LIMIT 10; -- Limit exposure

-- 4. Permissions
GRANT SELECT ON public.api_sales TO authenticated;
GRANT SELECT ON public.api_sales TO anon; -- If needed for public widgets

GRANT SELECT ON public.api_schedule TO authenticated;
GRANT SELECT ON public.api_schedule TO anon;

GRANT SELECT ON public.api_elt_status TO authenticated;
-- No anon access for Ops status usually

COMMIT;
