from prefect import flow

@flow(log_prints=True)
def hello(name: str = "Marvin") -> None:
    """Log a friendly greeting."""
    print(f"Hello, {name}!")

if __name__ == "__main__":
    hello()