-- sdw means sandbox dw
CREATE SCHEMA IF NOT EXISTS sdw;


-- dimensions
CREATE TABLE IF NOT EXISTS sdw.dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sdw.dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id TEXT UNIQUE,
    customer_unique_id TEXT,
    zip_code_prefix TEXT,
    city TEXT,
    state TEXT
);

CREATE TABLE IF NOT EXISTS sdw.dim_seller (
    seller_sk SERIAL PRIMARY KEY,
    seller_id TEXT UNIQUE,
    zip_code_prefix TEXT,
    city TEXT,
    state TEXT
);

CREATE TABLE IF NOT EXISTS sdw.dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id TEXT UNIQUE,
    category_name TEXT,
    category_name_english TEXT,
    weight INTEGER,
    height INTEGER,
    length INTEGER,
    width INTEGER
);

CREATE TABLE IF NOT EXISTS sdw.dim_order_status (
    order_status_sk SERIAL PRIMARY KEY,
    order_status TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS sdw.dim_payment_type (

);

-- facts

-- grain: order_id x order_item_id
-- i.e. 注文明細毎の分類
-- note: order_item_idはグローバルに一意ではないから、grainはorder_item_idではない。
CREATE TABLE IF NOT EXISTS sdw.fact_order_items (
    order_item_sk SERIAL PRIMARY KEY,
    order_id TEXT,
    order_item_id TEXT,
    customer_sk INTEGER,
    seller_sk INTEGER,
    product_sk INTEGER,
    order_status_sk INTEGER,
    purchase_date_key INTEGER,
    shipping_limit_date_key INTEGER,
    delivered_customer_date_key INTEGER,
    estimated_delivery_date_key INTEGER,
    price NUMERIC(12,2),
    freight_value NUMERIC(12,2)
);

-- grain: order_id
-- i.e. 1注文
-- note: order_idはビジネスとして一意であることを保証されているから、skは不要。
CREATE TABLE IF NOT EXISTS sdw.fact_orders (
    order_id TEXT PRIMARY KEY,
    customer_sk INTEGER,
    order_status_sk INTEGER,
    purchase_date_key INTEGER,
    shipping_limit_date_key INTEGER,
    delivered_customer_date_key INTEGER,
    estimated_delivery_date_key INTEGER,
    items_count INTEGER,
    item_price_total NUMERIC(12,2),
    freight_total NUMERIC(12,2),
    payment_total NUMERIC(12,2)
);

-- grain: order_id

CREATE TABLE IF NOT EXISTS sdw.fact_payments (
    payment_sk SERIAL PRIMARY KEY,
    order_id TEXT,
    payment_sequential INTEGER,
    payment_type_sk INTEGER,
    payment_installments INTEGER,
    payment_value NUMERIC(12,2),
    purchase_date_key INTEGER,
    customer_sk INTEGER
);

-- grain: review_id
-- note: review_idはビジネス内でグローバルに一意

CREATE TABLE IF NOT EXISTS sdw.fact_reviews (
    review_id TEXT,
    order_id TEXT,
    customer_sk INTEGER,
    review_score INTEGER,
    review_creation_date_key INTEGER,
    review_answer_date_key INTEGER
);















