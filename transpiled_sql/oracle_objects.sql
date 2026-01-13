WITH active_customers AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.country,
        c.signup_date
    FROM customers c
    WHERE c.status = 'ACTIVE'
),
order_details AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_amount,
        o.order_channel,
        o.order_status,

        -- Correlated subquery
        NVL((
            SELECT SUM(p.paid_amount)
            FROM payments p
            WHERE p.order_id = o.order_id
              AND p.payment_status = 'SUCCESS'
        ),0) AS total_paid,

        -- Window function
        LAG(o.order_amount) OVER (
            PARTITION BY o.customer_id
            ORDER BY o.order_date
        ) AS prev_order_amount
    FROM orders o
    WHERE o.order_date >= ADD_MONTHS(current_date(), -12)
),
customer_aggregation AS (
    SELECT
        ac.customer_id,
        ac.customer_name,
        ac.country,

        COUNT(od.order_id) AS total_orders,

        SUM(
            CASE
                WHEN od.order_status = 'COMPLETED'
                THEN od.order_amount
                ELSE 0
            END
        ) AS completed_sales,

        SUM(od.total_paid) AS total_payments,

        MAX(od.order_date) AS last_order_date,

        ROUND(AVG(od.order_amount),2) AS avg_order_value
    FROM active_customers ac
    LEFT JOIN order_details od
        ON ac.customer_id = od.customer_id
    GROUP BY
        ac.customer_id,
        ac.customer_name,
        ac.country
),
ranking_logic AS (
    SELECT
        ca.*,

        -- Region / country level analytics
        SUM(completed_sales) OVER (PARTITION BY country) AS country_sales,

        RANK() OVER (
            PARTITION BY country
            ORDER BY completed_sales DESC
        ) AS country_rank
    FROM customer_aggregation ca
),
final_report AS (
    SELECT
        rl.*,

        CASE
            WHEN completed_sales >= 4000
                 AND country_rank = 1
                 AND EXISTS (
                     SELECT 1
                     FROM orders o
                     WHERE o.customer_id = rl.customer_id
                       AND o.order_channel = 'ONLINE'
                 )
            THEN 'PLATINUM'

            WHEN completed_sales >= 2000
            THEN 'GOLD'

            WHEN completed_sales > 0
                 AND NOT EXISTS (
                     SELECT 1
                     FROM orders o
                     WHERE o.customer_id = rl.customer_id
                       AND o.order_status = 'CANCELLED'
                 )
            THEN 'SILVER'

            ELSE 'BRONZE'
        END AS customer_segment,

        DECODE(
            SIGN(current_date() - last_order_date),
            1, 'PAST',
            0, 'TODAY',
           -1, 'FUTURE'
        ) AS recency_bucket
    FROM ranking_logic rl
)
SELECT
    customer_id,
    customer_name,
    country,
    total_orders,
    completed_sales,
    total_payments,
    avg_order_value,
    last_order_date,
    country_rank,
    customer_segment,
    recency_bucket
FROM final_report
WHERE completed_sales > 0
ORDER BY country, country_rank;
