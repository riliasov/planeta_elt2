-- Migration 012: Comprehensive WebApp Backend Layer
-- Goal: Create Public Views (Wrappers) and Grant Permissions for WebApp

-- 0. Cleanup (Conflicts prevention)
DROP VIEW IF EXISTS public.api_sales CASCADE;
DROP VIEW IF EXISTS public.api_schedule CASCADE;
DROP VIEW IF EXISTS public.api_elt_status CASCADE;
DROP VIEW IF EXISTS public.api_clients CASCADE;
DROP VIEW IF EXISTS public.api_employees CASCADE;
DROP VIEW IF EXISTS public.api_products CASCADE;
DROP VIEW IF EXISTS public.api_auth_events CASCADE;

-- 1. Operational Views (90-day Retention)

-- api_sales
CREATE VIEW public.api_sales AS
SELECT 
    id, 
    date, -- Ensuring date type is propagated
    product_name, 
    type, 
    category, 
    quantity, 
    full_price, 
    discount, 
    final_price, 
    cash, 
    transfer, 
    terminal, 
    debt, 
    comment, 
    client_id, 
    admin, 
    trainer 
FROM core.sales
WHERE is_deleted = false
  AND date >= (CURRENT_DATE - INTERVAL '90 days');

-- api_schedule
CREATE VIEW public.api_schedule AS
SELECT 
    s.id, 
    s.date, 
    s.start_time, 
    s.end_time, 
    s.status, 
    s.type, 
    s.category, 
    s.comment, 
    s.client_id, 
    s.employee_id
FROM core.schedule s
WHERE s.is_deleted = false
  AND s.date >= (CURRENT_DATE - INTERVAL '90 days');

-- 2. Lookup Views (Full Access)

-- api_clients
CREATE VIEW public.api_clients AS
SELECT * 
FROM core.clients
WHERE is_deleted = false;

-- api_employees
CREATE VIEW public.api_employees AS
SELECT * 
FROM lookups.employees
WHERE is_active = true;

-- api_products
CREATE VIEW public.api_products AS
SELECT * 
FROM lookups.products
WHERE is_active = true;

-- 3. System Views

-- api_elt_status
CREATE VIEW public.api_elt_status AS
SELECT *
FROM ops.elt_runs
ORDER BY started_at DESC
LIMIT 10;

-- api_auth_events
CREATE VIEW public.api_auth_events AS
SELECT *
FROM webapp.auth_events;

-- 4. Permissions (Granting access to 'authenticated' role)

-- Allow "traversal" of schemas (necessary for selecting from views owned by superuser/service_role but accessing these tables)
GRANT USAGE ON SCHEMA core, lookups, webapp, ops TO authenticated;

-- Grant CRUD on Operational and Lookup Views
GRANT SELECT, INSERT, UPDATE, DELETE ON public.api_sales TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.api_schedule TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.api_clients TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.api_employees TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.api_products TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.api_auth_events TO authenticated;

-- Grant SELECT only on System Status
GRANT SELECT ON public.api_elt_status TO authenticated;
