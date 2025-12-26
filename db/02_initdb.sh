#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="/data/brazilian-e-commerce"

# 接続情報は公式 postgres イメージの env をそのまま使う
: "${POSTGRES_DB:?}"

echo "=== CSV import start ==="


declare -A TABLE_MAP=(
  ["olist_customers_dataset.csv"]="customers"
  ["olist_geolocation_dataset.csv"]="geolocations"
  ["olist_order_items_dataset.csv"]="order_items"
  ["olist_order_payments_dataset.csv"]="order_payments"
  ["olist_order_reviews_dataset.csv"]="order_reviews"
  ["olist_orders_dataset.csv"]="orders"
  ["olist_products_dataset.csv"]="products"
  ["olist_sellers_dataset.csv"]="sellers"
  ["product_category_name_translation.csv"]="product_category_name_translation"

)


for csv in "${DATA_DIR}"/*.csv; do
  [ -e "$csv" ] || continue

  file=$(basename "$csv")
  table="${TABLE_MAP[$file]:-}"

  echo "--- importing ${csv} -> ${table}"

  psql \
    --username "$POSTGRES_USER" \
    <<SQL
\\copy staging.${table} FROM '${csv}' WITH (FORMAT CSV, HEADER);
SQL

done

echo "=== CSV import finished ==="
