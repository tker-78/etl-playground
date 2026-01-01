-- load facts

TRUNCATE TABLE
    sdw.fact_order_items,
    sdw.fact_orders,
    sdw.fact_payments,
    sdw.fact_reviews
RESTART IDENTITY;


-- load fact_order_items
INSERT INTO sdw.fact_order_items (
                                  order_id,
                                  order_item_id,
                                  customer_sk,
                                  seller_sk,
                                  product_sk,
                                  order_status_sk,
                                  purchase_date_key,
                                  shipping_limit_date_key,
                                  delivered_customer_date_key,
                                  estimated_delivery_date_key,
                                  price,
                                  freight_value
)
SELECT
    oi.order_id,
    NULLIF(oi.order_item_id, ''),
    dc.customer_sk,
    ds.seller_sk,
    dp.product_sk,
    dos.order_status_sk,
    dd_purchase.date_key,
    dd_shipping_limit.date_key,
    dd_delivered.date_key,
    dd_estimated.date_key,
    NULLIF(oi.price, '')::numeric,
    NULLIF(oi.freight_value, '')::numeric
FROM staging.order_items oi
JOIN staging.orders o ON oi.order_id = o.order_id
LEFT JOIN sdw.dim_customer dc ON o.customer_id = dc.customer_id
LEFT JOIN sdw.dim_seller ds ON oi.seller_id = ds.seller_id
LEFT JOIN sdw.dim_product dp ON oi.product_id = dp.product_id
LEFT JOIN sdw.dim_order_status dos ON o.order_status = dos.order_status
LEFT JOIN sdw.dim_date dd_purchase ON dd_purchase.date = NULLIF(o.order_purchase_timestamp, '')::timestamp::date
LEFT JOIN sdw.dim_date dd_shipping_limit ON dd_shipping_limit.date = NULLIF(oi.shipping_limit_date, '')::timestamp::date
LEFT JOIN sdw.dim_date dd_delivered ON dd_delivered.date = NULLIF(o.order_delivered_customer_date, '')::timestamp::date
LEFT JOIN sdw.dim_date dd_estimated ON dd_estimated.date = NULLIF(o.order_estimated_delivery_date, '')::timestamp::date
;


-- load fact_orders
INSERT INTO sdw.fact_orders (
                             order_id,
                             customer_sk,
                             order_status_sk,
                             purchase_date_key,
                             delivered_customer_date_key,
                             estimated_delivery_date_key,
                             items_count,
                             item_price_total,
                             freight_total,
                             payment_total
)
SELECT
    o.order_id,
    dc.customer_sk,
    dos.order_status_sk,
    dd_purchase.date_key,
    dd_delivered.date_key,
    dd_estimated.date_key,
    COUNT(oi.order_item_id),
    SUM(oi.price::numeric),
    SUM(oi.freight_value::numeric),
    SUM(op.payment_value::numeric)
FROM staging.orders o
LEFT JOIN staging.order_items oi ON oi.order_id = o.order_id
LEFT JOIN staging.order_payments op ON op.order_id = o.order_id
LEFT JOIN sdw.dim_customer dc ON dc.customer_id = o.customer_id
LEFT JOIN sdw.dim_order_status dos ON dos.order_status = o.order_status
LEFT JOIN sdw.dim_date dd_purchase ON dd_purchase.date = NULLIF(o.order_purchase_timestamp, '')::timestamp::date
LEFT JOIN sdw.dim_date dd_delivered ON dd_delivered.date = NULLIF(o.order_delivered_customer_date, '')::timestamp::date
LEFT JOIN sdw.dim_date dd_estimated ON dd_estimated.date = NULLIF(o.order_estimated_delivery_date, '')::timestamp::date
GROUP BY
    o.order_id,
    dc.customer_sk,
    dos.order_status_sk,
    dd_purchase.date_key,
    dd_delivered.date_key,
    dd_estimated.date_key
;


-- load fact_payments
INSERT INTO sdw.fact_payments (
                               order_id,
                               payment_sequential,
                               payment_type_sk,
                               payment_installments,
                               payment_value,
                               purchase_date_key,
                               customer_sk
)
SELECT
    o.order_id,
    op.payment_sequential::int,
    dpt.payment_type_sk,
    op.payment_installments::int,
    op.payment_value::numeric,
    dd_purchase.date_key,
    dc.customer_sk
FROM staging.orders o
INNER JOIN staging.order_payments op
ON op.order_id = o.order_id
LEFT JOIN sdw.dim_payment_type dpt
ON dpt.payment_type = op.payment_type
LEFT JOIN sdw.dim_date dd_purchase
ON dd_purchase.date = o.order_purchase_timestamp::timestamp::date
LEFT JOIN sdw.dim_customer dc
ON dc.customer_id = o.customer_id;












