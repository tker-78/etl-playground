from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector

@task
def create_sandbox_tables(block_name: str):
    with SqlAlchemyConnector.load(block_name) as connector:
        with open("etl/sql/sandbox/create_tables.sql") as f:
            connector.execute(f.read())

@flow
def brazilian_ecommerce_dimensional_etl(
        block_name: str = "my-postgres-connection",
):
    create_sandbox_tables(block_name)

if __name__ == "__main__":
    brazilian_ecommerce_dimensional_etl()
