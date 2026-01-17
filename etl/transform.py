import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict

from etl.extract import extract_products

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def generate_price_variation(base_price: float) -> float:
    variation_percent = random.uniform(-0.10, 0.10)
    price = base_price * (1 + variation_percent)
    return round(max(price, 1.0), 2)


def generate_snapshot_dates(days: int = 365) -> List[datetime]:
    today = datetime.utcnow().date()
    return [today - timedelta(days=i) for i in range(days)]


def transform_products(
    products: List[Dict],
    days: int = 365,
    variations_per_day: int = 7
) -> List[Dict]:
    """
    Transform product data into time-series price snapshots.
    """
    logging.info("Starting transformation process")

    snapshots = []
    dates = generate_snapshot_dates(days)

    for product in products:
        for date in dates:
            for _ in range(variations_per_day):
                snapshot = {
                    "product_id": product["product_id"],
                    "title": product["title"],
                    "category": product["category"],
                    "base_price": product["price"],
                    "simulated_price": generate_price_variation(product["price"]),
                    "snapshot_date": date,
                    "ingested_at": datetime.utcnow()
                }
                snapshots.append(snapshot)

    logging.info(f"Generated {len(snapshots)} transformed rows")
    return snapshots


if __name__ == "__main__":
    products = extract_products()
    transformed_data = transform_products(products)
    print(f"Total transformed rows: {len(transformed_data)}")
