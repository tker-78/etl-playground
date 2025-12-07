from prefect import flow, task
import random

@task
def get_customer_ids() -> list[str]:
    return [f"customer{n}" for n in random.choices(range(100), k=10)]

@task
def process_customer(customer_id: str) -> str:
    return f"Processed {customer_id}"

@flow
def main() -> list[str]:
    customer_ids = get_customer_ids()
    results = process_customer.map(customer_ids)
    return results

if __name__ == "__main__":
    main.serve(name="Getting Started")

