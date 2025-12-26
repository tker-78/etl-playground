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
docker compose -f superset/docker-compose-image-tag.yaml up -d
```

:memo:

supersetからprefectのdb(etl-db)を参照する場合は、
下記のnetwork関係の箇所をアンコメントする。

```
  etl-db:
    image: postgres:15
    restart: always
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - db-data:/var/lib/postgresql/data
      - ./db:/docker-entrypoint-initdb.d
      - ./data/brazilian-e-commerce:/data/brazilian-e-commerce
#    networks:
#      - etl-net
volumes:
  flow-storage:
  postgres_data:
  db-data:
#networks:
#  etl-net:
#    external: true
```






