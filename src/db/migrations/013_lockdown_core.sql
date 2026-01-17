-- MIGRATION: Lockdown Core Access
-- 013_lockdown_core.sql
-- Goal: Revoke direct table access from 'authenticated' role to enforce usage of Public Views.
-- Source of original grants: 008_data_layer.sql

BEGIN;

-- 1. Revoke Direct Table Access
-- We keep USAGE on schema to allow visibility of types if any, but revoke data access.
REVOKE SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA core FROM authenticated;
REVOKE SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA webapp FROM authenticated;

-- 2. Add Documentation (SQL Comments)
COMMENT ON SCHEMA core IS 
'Protected Data Layer. Direct access for ''authenticated'' role is REVOKED (See Migration 013). Use public.api_* views instead.';

COMMENT ON TABLE core.sales IS 
'Core Sales Data. Access restricted to Service Role. Public access via public.api_sales (90-day retention).';

COMMENT ON TABLE core.clients IS 
'Core Clients Data. Access restricted to Service Role. Public access via public.api_clients.';

-- 3. Verify Public View Access
-- Ensure authenticated users can still USE the public views
-- (These grants should already exist from 010 and 012, but re-affirming is safe)
GRANT SELECT ON public.api_sales TO authenticated;
GRANT SELECT ON public.api_schedule TO authenticated;
GRANT SELECT ON public.api_clients TO authenticated;

-- 4. RLS Policy Cleanup (Optional but good practice)
-- Since they can't SELECT via table, the "Allow authenticated view" policies on core tables 
-- are strictly accessible only if passing through a View with security_invoker (which we don't have)
-- or if the View Owner has rights.
-- We leave RLS enabled as Deep Defense.

COMMIT;
