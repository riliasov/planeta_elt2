-- Витрина: Баланс клиентов (занятия)
-- Рассчитывается на основе продаж и посещений

CREATE OR REPLACE VIEW analytics.v_client_balances AS
WITH client_sales AS (
    SELECT 
        client_id,
        SUM(
            CASE 
                WHEN product_name ~ '8' THEN 8
                WHEN product_name ~ '4' THEN 4
                WHEN product_name ~ '12' THEN 12
                WHEN product_name ~ '16' THEN 16
                ELSE 1 
            END * quantity
        ) as units_bought,
        SUM(final_price) as total_spent
    FROM core.sales
    WHERE is_deleted = false
    GROUP BY 1
),
client_trainings AS (
    SELECT 
        client_id,
        COUNT(*) as units_used
    FROM core.schedule
    WHERE is_deleted = false
      AND status IN ('Посетили', 'Пропуск')
    GROUP BY 1
)
SELECT 
    c.name as "Клиент",
    c.phone as "Телефон",
    COALESCE(s.units_bought, 0) as "Куплено",
    COALESCE(t.units_used, 0) as "Использовано",
    COALESCE(s.units_bought, 0) - COALESCE(t.units_used, 0) as "Остаток",
    COALESCE(s.total_spent, 0) as "Оплачено",
    c.status as "Статус",
    NOW() as "Дата обновления"
FROM core.clients c
LEFT JOIN client_sales s ON c.id = s.client_id
LEFT JOIN client_trainings t ON c.id = t.client_id
WHERE c.is_deleted = false
  AND (s.units_bought > 0 OR t.units_used > 0)
ORDER BY "Остаток" ASC, c.name ASC;
