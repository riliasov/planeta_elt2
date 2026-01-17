-- Трансформация clients_cur -> clients
-- Источник: clients_cur (Google Sheets текущие клиенты)
-- Целевая таблица: core.clients

MERGE INTO core.clients AS target
USING (
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
        NULLIF(TRIM("tip"::text), '') as status
    FROM stg_gsheets.clients_cur
    WHERE NULLIF(TRIM("klient"::text), '') IS NOT NULL
    ORDER BY legacy_id
) AS source
ON (target.legacy_id = source.legacy_id)
WHEN MATCHED THEN
    UPDATE SET
        row_hash = source.row_hash,
        source = source.source,
        name = source.name,
        phone = source.phone,
        child_name = source.child_name,
        child_dob = source.child_dob,
        status = source.status,
        is_deleted = FALSE,
        deleted_at = NULL,
        updated_at = NOW()
WHEN NOT MATCHED THEN
    INSERT (
        legacy_id, row_hash, source, name, phone, child_name, child_dob, status
    )
    VALUES (
        source.legacy_id, source.row_hash, source.source, source.name, source.phone, 
        source.child_name, source.child_dob, source.status
    );


-- Источник: clients_hst (если есть)
-- Пока предполагаем что клиенты в основном идут из cur. 
-- Если data_cleaner/schema.py заливает clients_hst, добавим блок.
