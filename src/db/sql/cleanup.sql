-- SOFT DELETE CLEANUP
-- Помечает записи удалёнными (soft delete) в Core таблицах, если они отсутствуют в Staging.

-- [SALES]
WITH deleted_sales_cur AS (
    UPDATE sales s
    SET deleted_at = NOW(), is_deleted = TRUE, validation_status = 'deleted_in_source'
    WHERE s.source = 'sales_cur'
      AND s.row_hash NOT IN (SELECT "__row_hash" FROM staging.sales_cur WHERE "__row_hash" IS NOT NULL)
      AND s.is_deleted = FALSE
    RETURNING legacy_id
),
deleted_sales_hst AS (
    UPDATE sales s
    SET deleted_at = NOW(), is_deleted = TRUE, validation_status = 'deleted_in_source'
    WHERE s.source = 'sales_hst'
      AND s.row_hash NOT IN (SELECT "__row_hash" FROM staging.sales_hst WHERE "__row_hash" IS NOT NULL)
      AND s.is_deleted = FALSE
    RETURNING legacy_id
)
SELECT 
    (SELECT count(*) FROM deleted_sales_cur) as deleted_sales_cur,
    (SELECT count(*) FROM deleted_sales_hst) as deleted_sales_hst;


-- [SCHEDULE / TRAININGS]
WITH deleted_trainings_cur AS (
    UPDATE schedule s
    SET deleted_at = NOW(), is_deleted = TRUE
    WHERE s.row_hash NOT IN (SELECT "__row_hash" FROM staging.trainings_cur WHERE "__row_hash" IS NOT NULL)
      AND s.is_deleted = FALSE
    RETURNING legacy_id -- (Note: schedule might use sales source or custom? We used 'trainings_cur' in old ETL but transform_schedule.sql needs source column too)
    -- WARNING: transform_schedule.sql update needed!
), 
deleted_trainings_hst AS (
    UPDATE schedule s
    SET deleted_at = NOW(), is_deleted = TRUE
    WHERE s.row_hash NOT IN (SELECT "__row_hash" FROM staging.trainings_hst WHERE "__row_hash" IS NOT NULL)
      AND s.is_deleted = FALSE
    RETURNING legacy_id
)
SELECT 
    (SELECT count(*) FROM deleted_trainings_cur) as deleted_trainings_cur,
    (SELECT count(*) FROM deleted_trainings_hst) as deleted_trainings_hst;


-- [CLIENTS]
-- Clients usually come from clients_cur or hst.
WITH deleted_clients_cur AS (
    UPDATE clients c
    SET deleted_at = NOW(), is_deleted = TRUE, status = 'deleted'
    WHERE c.row_hash NOT IN (SELECT "__row_hash" FROM staging.clients_cur WHERE "__row_hash" IS NOT NULL)
      AND c.is_deleted = FALSE
      -- Add source check if we have multiple client sources?
    RETURNING legacy_id
)
SELECT (SELECT count(*) FROM deleted_clients_cur) as deleted_clients;
