-- Трансформация clients_cur -> clients
-- Источник: clients_cur (Google Sheets текущие клиенты)
-- Целевая таблица: clients (public)

INSERT INTO clients (
    legacy_id, row_hash, source, name, phone, child_name, child_dob, 
    age, spent, balance, debt, status
)
SELECT DISTINCT ON (legacy_id)
    md5(COALESCE("klient"::text, '') || COALESCE("mobilnyy"::text, '')) as legacy_id,
    "__row_hash" as row_hash,
    'clients_cur' as source,
    COALESCE(NULLIF(TRIM("klient"::text), ''), 'Без имени') as name,
    COALESCE(NULLIF(TRIM("mobilnyy"::text), ''), '+70000000000') as phone,
    NULLIF(TRIM("imya_rebenka"::text), '') as child_name,
    CASE 
        WHEN "data_rozhdeniya_rebenka"::text ~ '^\d{2}\.\d{2}\.\d{4}$' 
        THEN TO_DATE("data_rozhdeniya_rebenka"::text, 'DD.MM.YYYY')
        ELSE NULL 
    END as child_dob,
    NULL as age,
    0 as spent,
    0 as balance,
    0 as debt,
    NULLIF(TRIM("tip"::text), '') as status
FROM clients_cur
WHERE NULLIF(TRIM("klient"::text), '') IS NOT NULL
ORDER BY legacy_id
ON CONFLICT (legacy_id) DO UPDATE SET
    row_hash = EXCLUDED.row_hash,
    source = EXCLUDED.source,
    name = EXCLUDED.name,
    phone = EXCLUDED.phone,
    child_name = EXCLUDED.child_name,
    child_dob = EXCLUDED.child_dob,
    status = EXCLUDED.status,
    updated_at = NOW();


-- Источник: clients_hst (если есть)
-- Пока предполагаем что клиенты в основном идут из cur. 
-- Если data_cleaner/schema.py заливает clients_hst, добавим блок.
