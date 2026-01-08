-- Трансформация sales_cur/sales_hst -> sales
-- Источник: sales_cur, sales_hst (Google Sheets продажи)
-- Целевая таблица: sales (public)

-- === ТЕКУЩИЕ ПРОДАЖИ ===
INSERT INTO sales (
    legacy_id, row_hash, source, date, product_name, type, category,
    quantity, full_price, discount, final_price,
    cash, transfer, terminal, debt, comment, client_id
)
SELECT DISTINCT ON (legacy_id)
    md5(COALESCE("дата"::text, '') || COALESCE("клиент"::text, '') || COALESCE("продукт"::text, '') || COALESCE("окончательная_стоимость"::text, '')) as legacy_id,
    "__row_hash" as row_hash,
    'sales_cur' as source,
    CASE 
        WHEN "дата"::text ~ '\d{2}\.\d{2}\.\d{2}' 
        THEN TO_DATE(substring("дата"::text from '\d{2}\.\d{2}\.\d{2}'), 'DD.MM.YY')::timestamptz
        WHEN "дата"::text ~ '\d{2}\.\d{2}\.\d{4}'
        THEN TO_DATE("дата"::text, 'DD.MM.YYYY')::timestamptz
        ELSE NOW() 
    END as date,
    COALESCE(NULLIF(TRIM("продукт"::text), ''), 'Неизвестно') as product_name,
    NULLIF(TRIM("тип"::text), '') as type,
    NULLIF(TRIM("категория"::text), '') as category,
    COALESCE(NULLIF(TRIM("количество"::text), '')::integer, 1) as quantity,
    COALESCE(NULLIF(regexp_replace("полная_стоимость"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as full_price,
    COALESCE(NULLIF(regexp_replace("скидка"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as discount,
    COALESCE(NULLIF(regexp_replace("окончательная_стоимость"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as final_price,
    COALESCE(NULLIF(regexp_replace("наличные"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as cash,
    COALESCE(NULLIF(regexp_replace("перевод"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as transfer,
    COALESCE(NULLIF(regexp_replace("терминал"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as terminal,
    COALESCE(NULLIF(regexp_replace("вдолг"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as debt,
    NULLIF(TRIM("комментарий"::text), '') as comment,
    (SELECT id FROM clients c WHERE c.name = sales_cur."клиент"::text LIMIT 1) as client_id
FROM sales_cur
WHERE NULLIF(TRIM("продукт"::text), '') IS NOT NULL
  AND (SELECT id FROM clients c WHERE c.name = sales_cur."клиент"::text LIMIT 1) IS NOT NULL
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
    updated_at = NOW();

-- === ИСТОРИЧЕСКИЕ ПРОДАЖИ ===
INSERT INTO sales (
    legacy_id, row_hash, source, date, product_name, type, category,
    quantity, full_price, discount, final_price,
    cash, transfer, terminal, debt, comment, client_id
)
SELECT DISTINCT ON (legacy_id)
    md5(COALESCE("дата"::text, '') || COALESCE("клиент"::text, '') || COALESCE("продукт"::text, '') || COALESCE("окончательная_стоимость"::text, '')) as legacy_id,
    "__row_hash" as row_hash,
    'sales_hst' as source,
    CASE 
        WHEN "дата"::text ~ '\d{2}\.\d{2}\.\d{2}' 
        THEN TO_DATE(substring("дата"::text from '\d{2}\.\d{2}\.\d{2}'), 'DD.MM.YY')::timestamptz
        WHEN "дата"::text ~ '\d{2}\.\d{2}\.\d{4}'
        THEN TO_DATE("дата"::text, 'DD.MM.YYYY')::timestamptz
        ELSE NOW() 
    END as date,
    COALESCE(NULLIF(TRIM("продукт"::text), ''), 'Неизвестно') as product_name,
    NULLIF(TRIM("тип"::text), '') as type,
    NULLIF(TRIM("категория"::text), '') as category,
    COALESCE(NULLIF(TRIM("количество"::text), '')::integer, 1) as quantity,
    COALESCE(NULLIF(regexp_replace("полная_стоимость"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as full_price,
    COALESCE(NULLIF(regexp_replace("скидка"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as discount,
    COALESCE(NULLIF(regexp_replace("окончательная_стоимость"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as final_price,
    COALESCE(NULLIF(regexp_replace("наличные"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as cash,
    COALESCE(NULLIF(regexp_replace("перевод"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as transfer,
    COALESCE(NULLIF(regexp_replace("терминал"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as terminal,
    COALESCE(NULLIF(regexp_replace("вдолг"::text, '[^0-9,.-]', '', 'g'), '')::numeric, 0) as debt,
    NULLIF(TRIM("комментарий"::text), '') as comment,
    (SELECT id FROM clients c WHERE c.name = sales_hst."клиент"::text LIMIT 1) as client_id
FROM sales_hst
WHERE NULLIF(TRIM("продукт"::text), '') IS NOT NULL
  AND (SELECT id FROM clients c WHERE c.name = sales_hst."клиент"::text LIMIT 1) IS NOT NULL
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
    updated_at = NOW();
