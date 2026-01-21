import logging
import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Dict

from etl.extract import extract_products
from etl.transform import transform_products
from config.config import ETL_POSTGRES_CONN_ID

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw_product_prices (
    product_id INTEGER,
    title TEXT,
    category TEXT,
    base_price NUMERIC,
    simulated_price NUMERIC,
    snapshot_date DATE,
    ingested_at TIMESTAMP
);
"""

INSERT_SQL = """
INSERT INTO raw_product_prices (
    product_id,
    title,
    category,
    base_price,
    simulated_price,
    snapshot_date,
    ingested_at
)
VALUES (
    %(product_id)s,
    %(title)s,
    %(category)s,
    %(base_price)s,
    %(simulated_price)s,
    %(snapshot_date)s,
    %(ingested_at)s
);
"""

def get_connection():
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(ETL_POSTGRES_CONN_ID)

    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )

def load_to_postgres(rows: List[Dict]):
    logging.info("Connecting to postgreSQL")
    conn = get_connection()
    cur = conn.cursor()

    try:
        logging.info("Ensuring raw table exists")
        cur.execute(CREATE_TABLE_SQL)

        logging.info(f"Inserting {len(rows)} rows into raw_product_prices")
        execute_batch(cur, INSERT_SQL, rows, page_size=1000)

        conn.commit()
        logging.info("Data successfully loaded into PostgreSQl")

    except Exception as e:
        conn.rollback()
        logging.error(f"load data failed :< {e}")
        raise

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    products = extract_products()
    transformed_data = transform_products(products)
    load_to_postgres(transformed_data)

     

