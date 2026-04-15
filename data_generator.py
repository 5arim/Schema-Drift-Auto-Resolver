import random
from pathlib import Path

import pandas as pd
from faker import Faker


fake = Faker()
DATA_DIR = Path("data")


def _unique_ids(count: int, start: int, end: int) -> list[int]:
    """Generate unique integer IDs in the inclusive range [start, end]."""
    if count > (end - start + 1):
        raise ValueError("Count exceeds the number of unique IDs available in range.")
    return random.sample(range(start, end + 1), count)


def generate_day_1_data(num_records: int = 50) -> pd.DataFrame:
    """
    Generate day 1 orders data and save to data/day_1_orders.csv.

    Schema:
    - order_id: unique integer (1000-9999)
    - user_id: integer (1-500)
    - amount: float (10.0-500.0), rounded to 2 decimals
    - status: one of pending, shipped, delivered
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    statuses = ["pending", "shipped", "delivered"]
    order_ids = _unique_ids(num_records, 1000, 9999)

    records = []
    for order_id in order_ids:
        records.append(
            {
                "order_id": order_id,
                "user_id": fake.random_int(min=1, max=500),
                "amount": round(fake.pyfloat(min_value=10.0, max_value=500.0), 2),
                "status": random.choice(statuses),
            }
        )

    df = pd.DataFrame(records, columns=["order_id", "user_id", "amount", "status"])
    output_path = DATA_DIR / "day_1_orders.csv"
    df.to_csv(output_path, index=False)
    return df


def generate_day_2_data(num_records: int = 50) -> pd.DataFrame:
    """
    Generate day 2 orders data with intentional schema drift and save to data/day_2_orders.csv.

    Drift introduced:
    - user_id changes from int to string (e.g., USR-123)
    - amount renamed to transaction_amount
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    statuses = ["pending", "shipped", "delivered"]
    order_ids = _unique_ids(num_records, 10000, 19999)

    records = []
    for order_id in order_ids:
        records.append(
            {
                "order_id": order_id,
                "user_id": f"USR-{fake.random_int(min=1, max=500)}",
                "transaction_amount": round(fake.pyfloat(min_value=10.0, max_value=500.0), 2),
                "status": random.choice(statuses),
            }
        )

    df = pd.DataFrame(
        records, columns=["order_id", "user_id", "transaction_amount", "status"]
    )
    output_path = DATA_DIR / "day_2_orders.csv"
    df.to_csv(output_path, index=False)
    return df


if __name__ == "__main__":
    day_1_df = generate_day_1_data()
    print(
        f"Success: Created data/day_1_orders.csv with {len(day_1_df)} records "
        "using baseline schema."
    )

    day_2_df = generate_day_2_data()
    print(
        f"Success: Created data/day_2_orders.csv with {len(day_2_df)} records "
        "including intentional schema drift."
    )
