-- Трансформация trainings_cur/trainings_hst -> schedule
-- Источник: trainings_cur, trainings_hst (Google Sheets тренировки)
-- Целевая таблица: core.schedule

-- === ТЕКУЩИЕ ТРЕНИРОВКИ ===
MERGE INTO core.schedule AS target
USING (
    SELECT DISTINCT ON (legacy_id)
        md5(COALESCE("data"::text, '') || COALESCE("nachalo"::text, '') || COALESCE("klient"::text, '')) as legacy_id,
        "__row_hash" as row_hash,
        'trainings_cur' as source,
        CASE 
            WHEN "data"::text ~ '^\d{2}\.\d{2}\.'
                THEN TO_DATE("data"::text || '2025', 'DD.MM.YYYY')
            WHEN "data"::text ~ '\d{2}\.\d{2}\.\d{2}'
                THEN TO_DATE(substring("data"::text from '\d{2}\.\d{2}\.\d{2}'), 'DD.MM.YY')
            ELSE CURRENT_DATE
        END as date,
        COALESCE(NULLIF(substring(TRIM("nachalo"::text) from '^\d{1,2}:\d{2}'), ''), '09:00')::time as start_time,
        COALESCE(NULLIF(substring(TRIM("konets"::text) from '^\d{1,2}:\d{2}'), ''), '09:30')::time as end_time,
        COALESCE(NULLIF(TRIM(s."status"::text), ''), 'Свободно') as status,
        NULLIF(TRIM("tip"::text), '') as type,
        NULLIF(TRIM("kategoriya"::text), '') as category,
        NULLIF(TRIM("kommentariy"::text), '') as comment,
        c.id as client_id,
        e.id as employee_id
    FROM stg_gsheets.trainings_cur s
    LEFT JOIN core.clients c ON c.name = s.klient
    LEFT JOIN lookups.employees e ON e.full_name = s.sotrudnik
    WHERE NULLIF(TRIM("data"::text), '') IS NOT NULL
      AND c.id IS NOT NULL
    ORDER BY legacy_id
) AS source
ON (target.legacy_id = source.legacy_id)
WHEN MATCHED THEN
    UPDATE SET
        row_hash = source.row_hash,
        source = source.source,
        date = source.date,
        start_time = source.start_time,
        end_time = source.end_time,
        status = source.status,
        type = source.type,
        category = source.category,
        comment = source.comment,
        client_id = source.client_id,
        employee_id = source.employee_id,
        is_deleted = FALSE,
        deleted_at = NULL,
        updated_at = NOW()
WHEN NOT MATCHED THEN
    INSERT (
        legacy_id, row_hash, source, date, start_time, end_time,
        status, type, category, comment, client_id, employee_id
    )
    VALUES (
        source.legacy_id, source.row_hash, source.source, source.date, source.start_time, 
        source.end_time, source.status, source.type, source.category, source.comment, 
        source.client_id, source.employee_id
    );

-- === ИСТОРИЧЕСКИЕ ТРЕНИРОВКИ ===
MERGE INTO core.schedule AS target
USING (
    SELECT DISTINCT ON (legacy_id)
        md5(COALESCE("date"::text, '') || COALESCE("start_time"::text, '') || COALESCE("client_full"::text, '')) as legacy_id,
        "__row_hash" as row_hash,
        'trainings_hst' as source,
        CASE 
            WHEN "date"::text ~ '^\d{2}\.\d{2}\.\d{4}$'
                THEN TO_DATE("date"::text, 'DD.MM.YYYY')
            WHEN "date"::text ~ '\d{2}\.\d{2}\.\d{2}'
                THEN TO_DATE(substring("date"::text from '\d{2}\.\d{2}\.\d{2}'), 'DD.MM.YY')
            ELSE CURRENT_DATE
        END as date,
        COALESCE(NULLIF(substring(TRIM("start_time"::text) from '^\d{1,2}:\d{2}'), ''), '09:00')::time as start_time,
        COALESCE(NULLIF(substring(TRIM("end_time"::text) from '^\d{1,2}:\d{2}'), ''), '09:30')::time as end_time,
        COALESCE(NULLIF(TRIM(s."status"::text), ''), 'Свободно') as status,
        NULLIF(TRIM("product_type"::text), '') as type,
        NULLIF(TRIM("product_category"::text), '') as category,
        NULLIF(TRIM("comment"::text), '') as comment,
        c.id as client_id,
        e.id as employee_id
    FROM stg_gsheets.trainings_hst s
    LEFT JOIN core.clients c ON c.name = s.client_full
    LEFT JOIN lookups.employees e ON e.full_name = s.employee
    WHERE NULLIF(TRIM("date"::text), '') IS NOT NULL
      AND c.id IS NOT NULL
    ORDER BY legacy_id
) AS source
ON (target.legacy_id = source.legacy_id)
WHEN MATCHED THEN
    UPDATE SET
        row_hash = source.row_hash,
        source = source.source,
        date = source.date,
        start_time = source.start_time,
        end_time = source.end_time,
        status = source.status,
        type = source.type,
        category = source.category,
        comment = source.comment,
        client_id = source.client_id,
        employee_id = source.employee_id,
        is_deleted = FALSE,
        deleted_at = NULL,
        updated_at = NOW()
WHEN NOT MATCHED THEN
    INSERT (
        legacy_id, row_hash, source, date, start_time, end_time,
        status, type, category, comment, client_id, employee_id
    )
    VALUES (
        source.legacy_id, source.row_hash, source.source, source.date, source.start_time, 
        source.end_time, source.status, source.type, source.category, source.comment, 
        source.client_id, source.employee_id
    );
