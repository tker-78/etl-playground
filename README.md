# ETL Playground

Prefectを用いてData Warehouse構築するための実行環境です。


## usage

```
docker compose up -d
```

```
docker compose exec flow pip install -r requirements.txt
```

Databaseとの接続Blockの作成

```
docker compose exec flow python etl/db_connection.py
```

Dimension tables, Fact tablesの作成(dwスキーマ)

```
docker compose exec flow python etl/flows/prefect_brazilian_ecommerce_dimensional.py
```

```
docker compose -f superset/docker-compose-image-tag.yml up -d
```


supersetから`etl-db`を参照するには、networkのconnectが必要。

```
docker network connect superset_default etl-playground-etl-db-1
```