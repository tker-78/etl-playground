from prefect import task, flow
from pydantic import SecretStr
from prefect_sqlalchemy import SqlAlchemyConnector, ConnectionComponents, SyncDriver

@task
def db_connection():
    connection_info = ConnectionComponents(
        driver=SyncDriver.POSTGRESQL_PSYCOPG2,
        username="postgres",
        password=SecretStr("postgres"),
        host="etl-db",
        port=5432,
        database="postgres"
    )

    connector = SqlAlchemyConnector(connection_info=connection_info)
    connector.save("my-postgres-connection")


if __name__ == "__main__":
    db_connection()