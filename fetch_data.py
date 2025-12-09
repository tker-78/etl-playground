from prefect import flow
from prefect_sqlalchemy import SqlAlchemyConnector

@flow
def fetch_data(block_name: str) -> list:
    all_rows = []
    with SqlAlchemyConnector.load(block_name) as connector:
        while True:
            new_rows = connector.fetch_many("SELECT * FROM users;", size=2)
            if len(new_rows) == 0:
                break
            all_rows.append(new_rows)
    print(all_rows)
    return all_rows

if __name__ == "__main__":
    fetch_data("my-postgres-connection")