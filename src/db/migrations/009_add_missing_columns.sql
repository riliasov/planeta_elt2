-- MIGRATION PART 4: Missing Columns & Permissions
-- Run this in Supabase SQL Editor

BEGIN;

-- 1. core.schedule: Add employee_id
ALTER TABLE core.schedule 
ADD COLUMN IF NOT EXISTS employee_id integer REFERENCES lookups.employees(id);

-- 2. core.sales: Add admin, trainer
ALTER TABLE core.sales 
ADD COLUMN IF NOT EXISTS admin text,
ADD COLUMN IF NOT EXISTS trainer text;

-- 3. Grant Permissions on OPS for Authenticated (Just in case, though we prefer wrappers now)
GRANT USAGE ON SCHEMA ops TO authenticated;
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO authenticated;

COMMIT;
