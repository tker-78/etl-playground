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


-- load customer dimension
INSERT INTO sdw.dim_customer (
                              customer_id,
                              customer_unique_id,
                              zip_code_prefix,
                              city,
                              state
)
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM staging.customers
WHERE customer_id IS NOT NULL;

-- load seller dimension
INSERT INTO sdw.dim_seller (seller_id, zip_code_prefix, city, state)
SELECT DISTINCT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM staging.sellers
WHERE seller_id IS NOT NULL;

-- load product dimension
INSERT INTO sdw.dim_product ( product_id, category_name, category_name_english, weight, height, length, width)
SELECT
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    NULLIF(p.product_weight_g, '')::INTEGER AS weight,
    NULLIF(p.product_height_cm, '')::INTEGER AS height,
    NULLIF(p.product_length_cm, '') ::INTEGER AS length,
    NULLIF(p.product_width_cm, '')::INTEGER AS width
FROM staging.products AS p
LEFT JOIN staging.product_category_name_translation AS t
    ON t.product_category_name = p.product_category_name
WHERE p.product_id IS NOT NULL;


-- load order_status dimension
INSERT INTO sdw.dim_order_status (order_status)
SELECT DISTINCT order_status
FROM staging.orders
WHERE order_status IS NOT NULL;



