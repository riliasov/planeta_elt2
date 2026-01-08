-- Трансформация trainings_cur/trainings_hst -> schedule
-- Источник: trainings_cur, trainings_hst (Google Sheets тренировки)
-- Целевая таблица: schedule (public)

-- === ТЕКУЩИЕ ТРЕНИРОВКИ ===
INSERT INTO schedule (
    legacy_id, row_hash, source, date, start_time, end_time,
    status, type, category, comment, client_id
)
SELECT DISTINCT ON (legacy_id)
    md5(COALESCE("дата"::text, '') || COALESCE("начало"::text, '') || COALESCE("клиент"::text, '')) as legacy_id,
    "__row_hash" as row_hash,
    'trainings_cur' as source,
    CASE 
        WHEN "дата"::text ~ '^\d{2}\.\d{2}\.'
            THEN TO_DATE("дата"::text || '2025', 'DD.MM.YYYY')
        WHEN "дата"::text ~ '\d{2}\.\d{2}\.\d{2}'
            THEN TO_DATE(substring("дата"::text from '\d{2}\.\d{2}\.\d{2}'), 'DD.MM.YY')
        ELSE CURRENT_DATE
    END as date,
    COALESCE(NULLIF(substring(TRIM("начало"::text) from '^\d{1,2}:\d{2}'), ''), '09:00')::time as start_time,
    COALESCE(NULLIF(substring(TRIM("конец"::text) from '^\d{1,2}:\d{2}'), ''), '09:30')::time as end_time,
    COALESCE(NULLIF(TRIM("статус"::text), ''), 'Свободно') as status,
    NULLIF(TRIM("тип"::text), '') as type,
    NULLIF(TRIM("категория"::text), '') as category,
    NULLIF(TRIM("комментарий"::text), '') as comment,
    (SELECT id FROM clients c WHERE c.name = trainings_cur.клиент LIMIT 1) as client_id
FROM trainings_cur
WHERE NULLIF(TRIM("дата"::text), '') IS NOT NULL
  AND (SELECT id FROM clients c WHERE c.name = trainings_cur.клиент LIMIT 1) IS NOT NULL
ORDER BY legacy_id
ON CONFLICT (legacy_id) DO UPDATE SET
    row_hash = EXCLUDED.row_hash,
    source = EXCLUDED.source,
    date = EXCLUDED.date,
    start_time = EXCLUDED.start_time,
    end_time = EXCLUDED.end_time,
    status = EXCLUDED.status,
    type = EXCLUDED.type,
    category = EXCLUDED.category,
    comment = EXCLUDED.comment,
    client_id = EXCLUDED.client_id,
    updated_at = NOW();

-- === ИСТОРИЧЕСКИЕ ТРЕНИРОВКИ ===
INSERT INTO schedule (
    legacy_id, row_hash, source, date, start_time, end_time,
    status, type, category, comment, client_id
)
SELECT DISTINCT ON (legacy_id)
    md5(COALESCE("дата"::text, '') || COALESCE("начало"::text, '') || COALESCE("клиент"::text, '')) as legacy_id,
    "__row_hash" as row_hash,
    'trainings_hst' as source,
    CASE 
        WHEN "дата"::text ~ '^\d{2}\.\d{2}\.\d{4}$'
            THEN TO_DATE("дата"::text, 'DD.MM.YYYY')
        WHEN "дата"::text ~ '\d{2}\.\d{2}\.\d{2}'
            THEN TO_DATE(substring("дата"::text from '\d{2}\.\d{2}\.\d{2}'), 'DD.MM.YY')
        ELSE CURRENT_DATE
    END as date,
    COALESCE(NULLIF(substring(TRIM("начало"::text) from '^\d{1,2}:\d{2}'), ''), '09:00')::time as start_time,
    COALESCE(NULLIF(substring(TRIM("конец"::text) from '^\d{1,2}:\d{2}'), ''), '09:30')::time as end_time,
    COALESCE(NULLIF(TRIM("статус"::text), ''), 'Свободно') as status,
    NULLIF(TRIM("тип"::text), '') as type,
    NULLIF(TRIM("категория"::text), '') as category,
    NULLIF(TRIM("комментарий"::text), '') as comment,
    (SELECT id FROM clients c WHERE c.name = trainings_hst.клиент LIMIT 1) as client_id
FROM trainings_hst
WHERE NULLIF(TRIM("дата"::text), '') IS NOT NULL
  AND (SELECT id FROM clients c WHERE c.name = trainings_hst.клиент LIMIT 1) IS NOT NULL
ORDER BY legacy_id
ON CONFLICT (legacy_id) DO UPDATE SET
    row_hash = EXCLUDED.row_hash,
    source = EXCLUDED.source,
    date = EXCLUDED.date,
    start_time = EXCLUDED.start_time,
    end_time = EXCLUDED.end_time,
    status = EXCLUDED.status,
    type = EXCLUDED.type,
    category = EXCLUDED.category,
    comment = EXCLUDED.comment,
    client_id = EXCLUDED.client_id,
    updated_at = NOW();
