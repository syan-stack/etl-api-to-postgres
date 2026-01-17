import requests
import logging
from typing import List, Dict

API_URL = "https://fakestoreapi.com/products"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def extract_products() -> List[Dict]:
    """
    Extract product data from Fake Store API.

    Returns:
        List of product dictionaries
    """
    logging.info("Starting data extraction from Fake Store API")

    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"API request failed: {e}")
        raise

    data = response.json()

    if not isinstance(data, list):
        raise ValueError("Unexpected API response format")

    cleaned_products = []

    for item in data:
        product = {
            "product_id": item.get("id"),
            "title": item.get("title"),
            "price": item.get("price"),
            "category": item.get("category"),
            "description": item.get("description"),
        }

        if None in product.values():
            logging.warning(f"Skipping incomplete product: {product}")
            continue

        cleaned_products.append(product)

    logging.info(f"Successfully extracted {len(cleaned_products)} products")
    return cleaned_products


if __name__ == "__main__":
    products = extract_products()
    print(f"Extracted {len(products)} products")

