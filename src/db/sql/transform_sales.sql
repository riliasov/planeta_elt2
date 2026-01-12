-- Трансформация sales_cur/sales_hst -> sales
-- Источник: sales_cur, sales_hst (Google Sheets продажи)
-- Целевая таблица: core.sales 

-- === ТЕКУЩИЕ ПРОДАЖИ ===
INSERT INTO core.sales (
    legacy_id, row_hash, source, date, product_name, type, category,
    quantity, full_price, discount, final_price,
    cash, transfer, terminal, debt, comment, client_id
)
SELECT DISTINCT ON (legacy_id)
    md5(COALESCE("data"::text, '') || COALESCE("klient"::text, '') || COALESCE("produkt"::text, '') || COALESCE("okonchatelnaya_stoimost"::text, '')) as legacy_id,
    "__row_hash" as row_hash,
    'sales_cur' as source,
    CASE 
        WHEN "data"::text ~ '\d{2}\.\d{2}\.\d{2}' 
        THEN TO_DATE(substring("data"::text from '\d{2}\.\d{2}\.\d{2}'), 'DD.MM.YY')::timestamptz
        WHEN "data"::text ~ '\d{2}\.\d{2}\.\d{4}'
        THEN TO_DATE("data"::text, 'DD.MM.YYYY')::timestamptz
        ELSE NOW() 
    END as date,
    COALESCE(NULLIF(TRIM("produkt"::text), ''), 'Неизвестно') as product_name,
    NULLIF(TRIM("tip"::text), '') as type,
    NULLIF(TRIM("kategoriya"::text), '') as category,
    COALESCE(NULLIF(TRIM("kolichestvo"::text), '')::integer, 1) as quantity,
    COALESCE(NULLIF(regexp_replace("polnaya_stoimost"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as full_price,
    COALESCE(NULLIF(regexp_replace("skidka"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as discount,
    COALESCE(NULLIF(regexp_replace("okonchatelnaya_stoimost"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as final_price,
    COALESCE(NULLIF(regexp_replace("nalichnye"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as cash,
    COALESCE(NULLIF(regexp_replace("perevod"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as transfer,
    COALESCE(NULLIF(regexp_replace("terminal"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as terminal,
    COALESCE(NULLIF(regexp_replace("vdolg"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as debt,
    NULLIF(TRIM("kommentariy"::text), '') as comment,
    (SELECT id FROM core.clients c WHERE c.name = s."klient"::text LIMIT 1) as client_id
FROM stg_gsheets.sales_cur s
WHERE NULLIF(TRIM("produkt"::text), '') IS NOT NULL
  AND (SELECT id FROM core.clients c WHERE c.name = s."klient"::text LIMIT 1) IS NOT NULL
ORDER BY legacy_id
ON CONFLICT (legacy_id) DO UPDATE SET
    row_hash = EXCLUDED.row_hash,
    source = EXCLUDED.source,
    date = EXCLUDED.date,
    product_name = EXCLUDED.product_name,
    type = EXCLUDED.type,
    category = EXCLUDED.category,
    quantity = EXCLUDED.quantity,
    full_price = EXCLUDED.full_price,
    discount = EXCLUDED.discount,
    final_price = EXCLUDED.final_price,
    cash = EXCLUDED.cash,
    transfer = EXCLUDED.transfer,
    terminal = EXCLUDED.terminal,
    debt = EXCLUDED.debt,
    comment = EXCLUDED.comment,
    client_id = EXCLUDED.client_id,
    is_deleted = FALSE,
    deleted_at = NULL,
    updated_at = NOW();

-- === ИСТОРИЧЕСКИЕ ПРОДАЖИ ===
INSERT INTO core.sales (
    legacy_id, row_hash, source, date, product_name, type, category,
    quantity, full_price, discount, final_price,
    cash, transfer, terminal, debt, comment, client_id
)
SELECT DISTINCT ON (legacy_id)
    md5(COALESCE("data"::text, '') || COALESCE("klient"::text, '') || COALESCE("produkt"::text, '') || COALESCE("okonchatelnaya_stoimost"::text, '')) as legacy_id,
    "__row_hash" as row_hash,
    'sales_hst' as source,
    CASE 
        WHEN "data"::text ~ '\d{2}\.\d{2}\.\d{2}' 
        THEN TO_DATE(substring("data"::text from '\d{2}\.\d{2}\.\d{2}'), 'DD.MM.YY')::timestamptz
        WHEN "data"::text ~ '\d{2}\.\d{2}\.\d{4}'
        THEN TO_DATE("data"::text, 'DD.MM.YYYY')::timestamptz
        ELSE NOW() 
    END as date,
    COALESCE(NULLIF(TRIM("produkt"::text), ''), 'Неизвестно') as product_name,
    NULLIF(TRIM("tip"::text), '') as type,
    NULLIF(TRIM("kategoriya"::text), '') as category,
    COALESCE(NULLIF(TRIM("kolichestvo"::text), '')::integer, 1) as quantity,
    COALESCE(NULLIF(regexp_replace("polnaya_stoimost"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as full_price,
    COALESCE(NULLIF(regexp_replace("skidka"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as discount,
    COALESCE(NULLIF(regexp_replace("okonchatelnaya_stoimost"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as final_price,
    COALESCE(NULLIF(regexp_replace("nalichnye"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as cash,
    COALESCE(NULLIF(regexp_replace("perevod"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as transfer,
    COALESCE(NULLIF(regexp_replace("terminal"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as terminal,
    COALESCE(NULLIF(regexp_replace("vdolg"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as debt,
    NULLIF(TRIM("kommentariy"::text), '') as comment,
    (SELECT id FROM core.clients c WHERE c.name = s."klient"::text LIMIT 1) as client_id
FROM stg_gsheets.sales_hst s
WHERE NULLIF(TRIM("produkt"::text), '') IS NOT NULL
  AND (SELECT id FROM core.clients c WHERE c.name = s."klient"::text LIMIT 1) IS NOT NULL
ORDER BY legacy_id
ON CONFLICT (legacy_id) DO UPDATE SET
    row_hash = EXCLUDED.row_hash,
    source = EXCLUDED.source,
    date = EXCLUDED.date,
    product_name = EXCLUDED.product_name,
    type = EXCLUDED.type,
    category = EXCLUDED.category,
    quantity = EXCLUDED.quantity,
    full_price = EXCLUDED.full_price,
    discount = EXCLUDED.discount,
    final_price = EXCLUDED.final_price,
    cash = EXCLUDED.cash,
    transfer = EXCLUDED.transfer,
    terminal = EXCLUDED.terminal,
    debt = EXCLUDED.debt,
    comment = EXCLUDED.comment,
    client_id = EXCLUDED.client_id,
    is_deleted = FALSE,
    deleted_at = NULL,
    updated_at = NOW();
