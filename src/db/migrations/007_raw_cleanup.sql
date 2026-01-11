-- MIGRATION: Clean Raw
-- Removing unused legacy tables from raw schema

BEGIN;

-- Drop legacy tables that have 0 rows and are not used in code
DROP TABLE IF EXISTS raw.clients_hst;
DROP TABLE IF EXISTS raw.sales_cur;
DROP TABLE IF EXISTS raw.sales_hst;
DROP TABLE IF EXISTS raw.expenses_cur;
DROP TABLE IF EXISTS raw.expenses_hst;
DROP TABLE IF EXISTS raw.trainings_cur;
DROP TABLE IF EXISTS raw.trainings_hst;

COMMIT;
