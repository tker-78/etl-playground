from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable

from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


RAW_TABLES_DDL = """
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT
);

CREATE TABLE IF NOT EXISTS staging.geolocations (
    geolocation_zip_code_prefix TEXT,
    geolocation_lat TEXT,
    geolocation_lng TEXT,
    geolocation_city TEXT,
    geolocation_state TEXT
);

CREATE TABLE IF NOT EXISTS staging.order_items (
    order_id TEXT,
    order_item_id TEXT,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TEXT,
    price TEXT,
    freight_value TEXT
);

CREATE TABLE IF NOT EXISTS staging.order_payments (
    order_id TEXT,
    payment_sequential TEXT,
    payment_type TEXT,
    payment_installments TEXT,
    payment_value TEXT
);

CREATE TABLE IF NOT EXISTS staging.order_reviews (
    review_id TEXT,
    order_id TEXT,
    review_score TEXT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TEXT,
    review_answer_timestamp TEXT
);

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_carrier_date TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id TEXT,
    product_category_name TEXT,
    product_name_lenght TEXT,
    product_description_lenght TEXT,
    product_photos_qty TEXT,
    product_weight_g TEXT,
    product_length_cm TEXT,
    product_height_cm TEXT,
    product_width_cm TEXT
);

CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id TEXT,
    seller_zip_code_prefix TEXT,
    seller_city TEXT,
    seller_state TEXT
);

CREATE TABLE IF NOT EXISTS staging.product_category_name_translation (
    product_category_name TEXT,
    product_category_name_english TEXT
);
"""

DW_TABLES_DDL = """
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_sk BIGSERIAL PRIMARY KEY,
    customer_id TEXT UNIQUE,
    customer_unique_id TEXT,
    zip_code_prefix TEXT,
    city TEXT,
    state TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_seller (
    seller_sk BIGSERIAL PRIMARY KEY,
    seller_id TEXT UNIQUE,
    zip_code_prefix TEXT,
    city TEXT,
    state TEXT
);

CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_sk BIGSERIAL PRIMARY KEY,
    product_id TEXT UNIQUE,
    category_name TEXT,
    category_name_english TEXT,
    name_length INTEGER,
    description_length INTEGER,
    photos_qty INTEGER,
    weight_g INTEGER,
    length_cm INTEGER,
    height_cm INTEGER,
    width_cm INTEGER
);

CREATE TABLE IF NOT EXISTS dw.dim_order_status (
    order_status_sk SMALLSERIAL PRIMARY KEY,
    order_status TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dw.dim_payment_type (
    payment_type_sk SMALLSERIAL PRIMARY KEY,
    payment_type TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dw.fact_order_items (
    order_item_sk BIGSERIAL PRIMARY KEY,
    order_id TEXT,
    order_item_id INTEGER,
    customer_sk BIGINT,
    seller_sk BIGINT,
    product_sk BIGINT,
    order_status_sk SMALLINT,
    purchase_date_key INTEGER,
    shipping_limit_date_key INTEGER,
    delivered_customer_date_key INTEGER,
    estimated_delivery_date_key INTEGER,
    price NUMERIC(12, 2),
    freight_value NUMERIC(12, 2)
);

CREATE TABLE IF NOT EXISTS dw.fact_orders (
    order_id TEXT PRIMARY KEY,
    customer_sk BIGINT,
    order_status_sk SMALLINT,
    purchase_date_key INTEGER,
    approved_date_key INTEGER,
    delivered_carrier_date_key INTEGER,
    delivered_customer_date_key INTEGER,
    estimated_delivery_date_key INTEGER,
    items_count INTEGER,
    order_item_total NUMERIC(14, 2),
    freight_total NUMERIC(14, 2),
    payment_total NUMERIC(14, 2)
);

CREATE TABLE IF NOT EXISTS dw.fact_payments (
    order_id TEXT,
    payment_sequential INTEGER,
    payment_type_sk SMALLINT,
    payment_installments INTEGER,
    payment_value NUMERIC(12, 2),
    purchase_date_key INTEGER,
    customer_sk BIGINT
);

CREATE TABLE IF NOT EXISTS dw.fact_reviews (
    review_id TEXT,
    order_id TEXT,
    customer_sk BIGINT,
    review_score INTEGER,
    review_creation_date_key INTEGER,
    review_answer_date_key INTEGER
);
"""


DATASETS = [
    ("olist_customers_dataset.csv", "staging.customers"),
    ("olist_geolocation_dataset.csv", "staging.geolocations"),
    ("olist_order_items_dataset.csv", "staging.order_items"),
    ("olist_order_payments_dataset.csv", "staging.order_payments"),
    ("olist_order_reviews_dataset.csv", "staging.order_reviews"),
    ("olist_orders_dataset.csv", "staging.orders"),
    ("olist_products_dataset.csv", "staging.products"),
    ("olist_sellers_dataset.csv", "staging.sellers"),
    ("product_category_name_translation.csv", "staging.product_category_name_translation"),
]


def _batched(rows: Iterable[dict], batch_size: int) -> Iterable[list[dict]]:
    batch: list[dict] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


@task
def create_staging_tables(block_name: str) -> None:
    with SqlAlchemyConnector.load(block_name) as connector:
        connector.execute(RAW_TABLES_DDL)


@task
def load_csv_to_table(block_name: str, csv_path: Path, table: str, batch_size: int = 5000) -> None:
    with SqlAlchemyConnector.load(block_name) as connector:
        connector.execute(f"TRUNCATE TABLE {table};")
        with csv_path.open("r", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                return
            columns = reader.fieldnames
            column_list = ", ".join(columns)
            values_list = ", ".join([f":{col}" for col in columns])
            insert_sql = f'INSERT INTO {table} ({column_list}) VALUES ({values_list});'
            for batch in _batched(reader, batch_size=batch_size):
                connector.execute_many(insert_sql, batch)


@task
def create_dw_tables(block_name: str) -> None:
    with SqlAlchemyConnector.load(block_name) as connector:
        connector.execute(DW_TABLES_DDL)


@task
def load_dimensions(block_name: str) -> None:
    with SqlAlchemyConnector.load(block_name) as connector:
        connector.execute(
            """
            TRUNCATE TABLE
                dw.dim_date,
                dw.dim_customer,
                dw.dim_seller,
                dw.dim_product,
                dw.dim_order_status,
                dw.dim_payment_type
            RESTART IDENTITY;
            """
        )

        connector.execute(
            """
            WITH date_bounds AS (
                SELECT MIN(date_val) AS min_date, MAX(date_val) AS max_date
                FROM (
                    SELECT NULLIF(order_purchase_timestamp, '')::timestamp::date AS date_val FROM staging.orders
                    UNION ALL
                    SELECT NULLIF(order_approved_at, '')::timestamp::date FROM staging.orders
                    UNION ALL
                    SELECT NULLIF(order_delivered_carrier_date, '')::timestamp::date FROM staging.orders
                    UNION ALL
                    SELECT NULLIF(order_delivered_customer_date, '')::timestamp::date FROM staging.orders
                    UNION ALL
                    SELECT NULLIF(order_estimated_delivery_date, '')::timestamp::date FROM staging.orders
                    UNION ALL
                    SELECT NULLIF(shipping_limit_date, '')::timestamp::date FROM staging.order_items
                    UNION ALL
                    SELECT NULLIF(review_creation_date, '')::timestamp::date FROM staging.order_reviews
                    UNION ALL
                    SELECT NULLIF(review_answer_timestamp, '')::timestamp::date FROM staging.order_reviews
                ) dates
                WHERE date_val IS NOT NULL
            )
            INSERT INTO dw.dim_date (
                date_key,
                date,
                year,
                quarter,
                month,
                month_name,
                day,
                day_of_week,
                day_name,
                is_weekend
            )
            SELECT
                (EXTRACT(YEAR FROM d)::int * 10000)
                + (EXTRACT(MONTH FROM d)::int * 100)
                + EXTRACT(DAY FROM d)::int AS date_key,
                d::date AS date,
                EXTRACT(YEAR FROM d)::int AS year,
                EXTRACT(QUARTER FROM d)::int AS quarter,
                EXTRACT(MONTH FROM d)::int AS month,
                TO_CHAR(d, 'FMMonth') AS month_name,
                EXTRACT(DAY FROM d)::int AS day,
                EXTRACT(ISODOW FROM d)::int AS day_of_week,
                TO_CHAR(d, 'FMDay') AS day_name,
                CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
            FROM date_bounds, generate_series(min_date, max_date, interval '1 day') AS d;
            """
        )

        connector.execute(
            """
            INSERT INTO dw.dim_customer (
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
            """
        )

        connector.execute(
            """
            INSERT INTO dw.dim_seller (
                seller_id,
                zip_code_prefix,
                city,
                state
            )
            SELECT DISTINCT
                seller_id,
                seller_zip_code_prefix,
                seller_city,
                seller_state
            FROM staging.sellers
            WHERE seller_id IS NOT NULL;
            """
        )

        connector.execute(
            """
            INSERT INTO dw.dim_product (
                product_id,
                category_name,
                category_name_english,
                name_length,
                description_length,
                photos_qty,
                weight_g,
                length_cm,
                height_cm,
                width_cm
            )
            SELECT DISTINCT
                p.product_id,
                p.product_category_name,
                t.product_category_name_english,
                NULLIF(p.product_name_lenght, '')::int,
                NULLIF(p.product_description_lenght, '')::int,
                NULLIF(p.product_photos_qty, '')::int,
                NULLIF(p.product_weight_g, '')::int,
                NULLIF(p.product_length_cm, '')::int,
                NULLIF(p.product_height_cm, '')::int,
                NULLIF(p.product_width_cm, '')::int
            FROM staging.products p
            LEFT JOIN staging.product_category_name_translation t
                ON t.product_category_name = p.product_category_name
            WHERE p.product_id IS NOT NULL;
            """
        )

        connector.execute(
            """
            INSERT INTO dw.dim_order_status (order_status)
            SELECT DISTINCT order_status
            FROM staging.orders
            WHERE order_status IS NOT NULL;
            """
        )

        connector.execute(
            """
            INSERT INTO dw.dim_payment_type (payment_type)
            SELECT DISTINCT payment_type
            FROM staging.order_payments
            WHERE payment_type IS NOT NULL;
            """
        )


@task
def load_facts(block_name: str) -> None:
    with SqlAlchemyConnector.load(block_name) as connector:
        connector.execute(
            """
            TRUNCATE TABLE
                dw.fact_order_items,
                dw.fact_orders,
                dw.fact_payments,
                dw.fact_reviews
            RESTART IDENTITY;
            """
        )

        connector.execute(
            """
            INSERT INTO dw.fact_order_items (
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
                NULLIF(oi.order_item_id, '')::int,
                dc.customer_sk,
                ds.seller_sk,
                dp.product_sk,
                dos.order_status_sk,
                dd_purchase.date_key,
                dd_shipping.date_key,
                dd_delivered.date_key,
                dd_estimated.date_key,
                NULLIF(oi.price, '')::numeric,
                NULLIF(oi.freight_value, '')::numeric
            FROM staging.order_items oi
            JOIN staging.orders o
                ON o.order_id = oi.order_id
            LEFT JOIN dw.dim_customer dc
                ON dc.customer_id = o.customer_id
            LEFT JOIN dw.dim_seller ds
                ON ds.seller_id = oi.seller_id
            LEFT JOIN dw.dim_product dp
                ON dp.product_id = oi.product_id
            LEFT JOIN dw.dim_order_status dos
                ON dos.order_status = o.order_status
            LEFT JOIN dw.dim_date dd_purchase
                ON dd_purchase.date = NULLIF(o.order_purchase_timestamp, '')::timestamp::date
            LEFT JOIN dw.dim_date dd_shipping
                ON dd_shipping.date = NULLIF(oi.shipping_limit_date, '')::timestamp::date
            LEFT JOIN dw.dim_date dd_delivered
                ON dd_delivered.date = NULLIF(o.order_delivered_customer_date, '')::timestamp::date
            LEFT JOIN dw.dim_date dd_estimated
                ON dd_estimated.date = NULLIF(o.order_estimated_delivery_date, '')::timestamp::date;
            """
        )

        connector.execute(
            """
            INSERT INTO dw.fact_orders (
                order_id,
                customer_sk,
                order_status_sk,
                purchase_date_key,
                approved_date_key,
                delivered_carrier_date_key,
                delivered_customer_date_key,
                estimated_delivery_date_key,
                items_count,
                order_item_total,
                freight_total,
                payment_total
            )
            SELECT
                o.order_id,
                dc.customer_sk,
                dos.order_status_sk,
                dd_purchase.date_key,
                dd_approved.date_key,
                dd_carrier.date_key,
                dd_delivered.date_key,
                dd_estimated.date_key,
                COUNT(oi.order_item_id),
                COALESCE(SUM(NULLIF(oi.price, '')::numeric), 0),
                COALESCE(SUM(NULLIF(oi.freight_value, '')::numeric), 0),
                COALESCE(SUM(NULLIF(op.payment_value, '')::numeric), 0)
            FROM staging.orders o
            LEFT JOIN staging.order_items oi
                ON oi.order_id = o.order_id
            LEFT JOIN staging.order_payments op
                ON op.order_id = o.order_id
            LEFT JOIN dw.dim_customer dc
                ON dc.customer_id = o.customer_id
            LEFT JOIN dw.dim_order_status dos
                ON dos.order_status = o.order_status
            LEFT JOIN dw.dim_date dd_purchase
                ON dd_purchase.date = NULLIF(o.order_purchase_timestamp, '')::timestamp::date
            LEFT JOIN dw.dim_date dd_approved
                ON dd_approved.date = NULLIF(o.order_approved_at, '')::timestamp::date
            LEFT JOIN dw.dim_date dd_carrier
                ON dd_carrier.date = NULLIF(o.order_delivered_carrier_date, '')::timestamp::date
            LEFT JOIN dw.dim_date dd_delivered
                ON dd_delivered.date = NULLIF(o.order_delivered_customer_date, '')::timestamp::date
            LEFT JOIN dw.dim_date dd_estimated
                ON dd_estimated.date = NULLIF(o.order_estimated_delivery_date, '')::timestamp::date
            GROUP BY
                o.order_id,
                dc.customer_sk,
                dos.order_status_sk,
                dd_purchase.date_key,
                dd_approved.date_key,
                dd_carrier.date_key,
                dd_delivered.date_key,
                dd_estimated.date_key;
            """
        )

        connector.execute(
            """
            INSERT INTO dw.fact_payments (
                order_id,
                payment_sequential,
                payment_type_sk,
                payment_installments,
                payment_value,
                purchase_date_key,
                customer_sk
            )
            SELECT
                op.order_id,
                NULLIF(op.payment_sequential, '')::int,
                dpt.payment_type_sk,
                NULLIF(op.payment_installments, '')::int,
                NULLIF(op.payment_value, '')::numeric,
                dd_purchase.date_key,
                dc.customer_sk
            FROM staging.order_payments op
            LEFT JOIN staging.orders o
                ON o.order_id = op.order_id
            LEFT JOIN dw.dim_payment_type dpt
                ON dpt.payment_type = op.payment_type
            LEFT JOIN dw.dim_customer dc
                ON dc.customer_id = o.customer_id
            LEFT JOIN dw.dim_date dd_purchase
                ON dd_purchase.date = NULLIF(o.order_purchase_timestamp, '')::timestamp::date;
            """
        )

        connector.execute(
            """
            INSERT INTO dw.fact_reviews (
                review_id,
                order_id,
                customer_sk,
                review_score,
                review_creation_date_key,
                review_answer_date_key
            )
            SELECT
                r.review_id,
                r.order_id,
                dc.customer_sk,
                NULLIF(r.review_score, '')::int,
                dd_creation.date_key,
                dd_answer.date_key
            FROM staging.order_reviews r
            LEFT JOIN staging.orders o
                ON o.order_id = r.order_id
            LEFT JOIN dw.dim_customer dc
                ON dc.customer_id = o.customer_id
            LEFT JOIN dw.dim_date dd_creation
                ON dd_creation.date = NULLIF(r.review_creation_date, '')::timestamp::date
            LEFT JOIN dw.dim_date dd_answer
                ON dd_answer.date = NULLIF(r.review_answer_timestamp, '')::timestamp::date;
            """
        )


@flow
def brazilian_ecommerce_dimensional_etl(
    block_name: str = "my-postgres-connection",
    data_dir: str = "data/brazilian-e-commerce",
) -> None:
    # create_staging_tables(block_name)
    create_dw_tables(block_name)

    data_path = Path(data_dir)
    for file_name, table in DATASETS:
        load_csv_to_table(block_name, data_path / file_name, table)

    load_dimensions(block_name)
    load_facts(block_name)


if __name__ == "__main__":
    brazilian_ecommerce_dimensional_etl()
