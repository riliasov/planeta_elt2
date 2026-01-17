-- Трансформация sales_cur/sales_hst -> sales
-- Источник: sales_cur, sales_hst (Google Sheets продажи)
-- Целевая таблица: core.sales 

-- === ТЕКУЩИЕ ПРОДАЖИ ===
MERGE INTO core.sales AS target
USING (
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
        (SELECT id FROM core.clients c WHERE c.name = s."klient"::text LIMIT 1) as client_id,
        NULLIF(TRIM("admin"::text), '') as admin,
        NULLIF(TRIM("trener"::text), '') as trainer
    FROM stg_gsheets.sales_cur s
    WHERE NULLIF(TRIM("produkt"::text), '') IS NOT NULL
      AND (SELECT id FROM core.clients c WHERE c.name = s."klient"::text LIMIT 1) IS NOT NULL
    ORDER BY legacy_id
) AS source
ON (target.legacy_id = source.legacy_id)
WHEN MATCHED THEN
    UPDATE SET
        row_hash = source.row_hash,
        source = source.source,
        date = source.date,
        product_name = source.product_name,
        type = source.type,
        category = source.category,
        quantity = source.quantity,
        full_price = source.full_price,
        discount = source.discount,
        final_price = source.final_price,
        cash = source.cash,
        transfer = source.transfer,
        terminal = source.terminal,
        debt = source.debt,
        comment = source.comment,
        client_id = source.client_id,
        admin = source.admin,
        trainer = source.trainer,
        is_deleted = FALSE,
        deleted_at = NULL,
        updated_at = NOW()
WHEN NOT MATCHED THEN
    INSERT (
        legacy_id, row_hash, source, date, product_name, type, category,
        quantity, full_price, discount, final_price,
        cash, transfer, terminal, debt, comment, client_id,
        admin, trainer
    )
    VALUES (
        source.legacy_id, source.row_hash, source.source, source.date, source.product_name, 
        source.type, source.category, source.quantity, source.full_price, source.discount, 
        source.final_price, source.cash, source.transfer, source.terminal, source.debt, 
        source.comment, source.client_id, source.admin, source.trainer
    );

-- === ИСТОРИЧЕСКИЕ ПРОДАЖИ ===
MERGE INTO core.sales AS target
USING (
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
        (SELECT id FROM core.clients c WHERE c.name = s."klient"::text LIMIT 1) as client_id,
        NULLIF(TRIM("admin"::text), '') as admin,
        NULLIF(TRIM("trener"::text), '') as trainer
    FROM stg_gsheets.sales_hst s
    WHERE NULLIF(TRIM("produkt"::text), '') IS NOT NULL
      AND (SELECT id FROM core.clients c WHERE c.name = s."klient"::text LIMIT 1) IS NOT NULL
    ORDER BY legacy_id
) AS source
ON (target.legacy_id = source.legacy_id)
WHEN MATCHED THEN
    UPDATE SET
        row_hash = source.row_hash,
        source = source.source,
        date = source.date,
        product_name = source.product_name,
        type = source.type,
        category = source.category,
        quantity = source.quantity,
        full_price = source.full_price,
        discount = source.discount,
        final_price = source.final_price,
        cash = source.cash,
        transfer = source.transfer,
        terminal = source.terminal,
        debt = source.debt,
        comment = source.comment,
        client_id = source.client_id,
        admin = source.admin,
        trainer = source.trainer,
        is_deleted = FALSE,
        deleted_at = NULL,
        updated_at = NOW()
WHEN NOT MATCHED THEN
    INSERT (
        legacy_id, row_hash, source, date, product_name, type, category,
        quantity, full_price, discount, final_price,
        cash, transfer, terminal, debt, comment, client_id,
        admin, trainer
    )
    VALUES (
        source.legacy_id, source.row_hash, source.source, source.date, source.product_name, 
        source.type, source.category, source.quantity, source.full_price, source.discount, 
        source.final_price, source.cash, source.transfer, source.terminal, source.debt, 
        source.comment, source.client_id, source.admin, source.trainer
    );
