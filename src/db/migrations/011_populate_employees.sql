-- Helper: Populate employees from trainings
INSERT INTO lookups.employees (full_name)
SELECT DISTINCT sotrudnik
FROM stg_gsheets.trainings_cur
WHERE sotrudnik IS NOT NULL AND TRIM(sotrudnik) <> ''
ON CONFLICT (full_name) DO NOTHING;

INSERT INTO lookups.employees (full_name)
SELECT DISTINCT sotrudnik
FROM stg_gsheets.trainings_hst
WHERE sotrudnik IS NOT NULL AND TRIM(sotrudnik) <> ''
ON CONFLICT (full_name) DO NOTHING;
