-- load dimensions

-- load date dimension
TRUNCATE TABLE
    sdw.dim_date,
    sdw.dim_customer,
    sdw.dim_product,
    sdw.dim_seller,
    sdw.dim_order_status
RESTART IDENTITY;

WITH date_bounds AS (
    SELECT MIN (date_val) AS min_date, MAX(date_val) AS max_date
    FROM (
        SELECT NULLIF(order_purchase_timestamp, '')::timestamp::date AS date_val FROM staging.orders
         UNION ALL
        SELECT NULLIF(shipping_limit_date, '')::timestamp::date AS date_val FROM staging.order_items
        UNION ALL
        SELECT NULLIF(order_delivered_customer_date, '')::timestamp::date AS date_val FROM staging.orders
        UNION ALL
        SELECT NULLIF(order_estimated_delivery_date, '')::timestamp::date AS date_val FROM staging.orders
     ) AS dates
    WHERE date_val IS NOT NULL
)
INSERT INTO sdw.dim_date (
                          date_key,
                          date,
                          year,
                          month,
                          day
)
SELECT
    (EXTRACT(YEAR FROM d)::int * 10000)
        + (EXTRACT(MONTH FROM d)::int * 100)
        + EXTRACT(DAY FROM d)::int AS date_key,
    d::date AS date,
    EXTRACT(YEAR FROM d)::int AS year,
    EXTRACT(MONTH FROM d)::int AS month,
    EXTRACT(DAY FROM d)::int AS day
FROM date_bounds, generate_series(min_date, max_date, '1 day'::interval) AS d;
