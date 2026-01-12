-- SOFT DELETE CLEANUP
-- Помечает записи удалёнными (soft delete) в Core таблицах, если они отсутствуют в Staging.

-- [SALES]
WITH deleted_sales_cur AS (
    UPDATE core.sales s
    SET deleted_at = NOW(), is_deleted = TRUE
    WHERE s.source = 'sales_cur'
      AND s.row_hash NOT IN (SELECT "__row_hash" FROM stg_gsheets.sales_cur WHERE "__row_hash" IS NOT NULL)
      AND s.is_deleted = FALSE
    RETURNING legacy_id
),
deleted_sales_hst AS (
    UPDATE core.sales s
    SET deleted_at = NOW(), is_deleted = TRUE
    WHERE s.source = 'sales_hst'
      AND s.row_hash NOT IN (SELECT "__row_hash" FROM stg_gsheets.sales_hst WHERE "__row_hash" IS NOT NULL)
      AND s.is_deleted = FALSE
    RETURNING legacy_id
)
SELECT 
    (SELECT count(*) FROM deleted_sales_cur) as deleted_sales_cur,
    (SELECT count(*) FROM deleted_sales_hst) as deleted_sales_hst;


-- [SCHEDULE / TRAININGS]
WITH deleted_trainings_cur AS (
    UPDATE core.schedule s
    SET deleted_at = NOW(), is_deleted = TRUE
    WHERE s.source = 'trainings_cur'
      AND s.row_hash NOT IN (SELECT "__row_hash" FROM stg_gsheets.trainings_cur WHERE "__row_hash" IS NOT NULL)
      AND s.is_deleted = FALSE
    RETURNING legacy_id 
), 
deleted_trainings_hst AS (
    UPDATE core.schedule s
    SET deleted_at = NOW(), is_deleted = TRUE
    WHERE s.source = 'trainings_hst'
      AND s.row_hash NOT IN (SELECT "__row_hash" FROM stg_gsheets.trainings_hst WHERE "__row_hash" IS NOT NULL)
      AND s.is_deleted = FALSE
    RETURNING legacy_id
)
SELECT 
    (SELECT count(*) FROM deleted_trainings_cur) as deleted_trainings_cur,
    (SELECT count(*) FROM deleted_trainings_hst) as deleted_trainings_hst;


-- [CLIENTS]
-- Clients usually come from clients_cur or hst.
WITH deleted_clients_cur AS (
    UPDATE core.clients c
    SET deleted_at = NOW(), is_deleted = TRUE, status = 'deleted'
    WHERE c.row_hash NOT IN (
        SELECT "__row_hash" FROM stg_gsheets.clients_cur WHERE "__row_hash" IS NOT NULL
        UNION ALL
        SELECT "__row_hash" FROM stg_gsheets.clients_hst WHERE "__row_hash" IS NOT NULL
    )
      AND c.is_deleted = FALSE
    RETURNING legacy_id
)
SELECT (SELECT count(*) FROM deleted_clients_cur) as deleted_clients;
