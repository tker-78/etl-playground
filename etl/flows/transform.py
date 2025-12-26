from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector

def update_table_data(connector, table_name: str):
    delete_query = f"DELETE FROM {table_name};"
    insert_query = f"""
    INSERT INTO {table_name} (
    post_id,
    title,
    content,
    user_id,
    username,
    password,
    email
    )
    SELECT posts.id as post_id,title, content, user_id, username, password, email FROM posts
    LEFT JOIN users ON users.id = posts.user_id;
    """
    connector.execute(delete_query)
    connector.execute(insert_query)


def create_table(connector, table_name: str):
        create_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
        post_id SERIAL PRIMARY KEY, 
        title VARCHAR,
        content TEXT,
        user_id INT,
        username VARCHAR,
        password VARCHAR,
        email VARCHAR
        );
        """
        connector.execute(create_query)

@flow
def update_posts_users_table(block_name: str, table_name: str = "posts_users"):
    with SqlAlchemyConnector.load(block_name) as connector:
        create_table(connector, table_name)
        update_table_data(connector, table_name)


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
    # fetch_data("my-postgres-connection")
    update_posts_users_table("my-postgres-connection", "posts_users")