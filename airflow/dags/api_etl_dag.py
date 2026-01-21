from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_task_callable():
    from etl.extract import extract_products
    extract_products()


def transform_task_callable():
    from etl.extract import extract_products
    from etl.transform import transform_products

    products = extract_products()
    transform_products(products)


def load_task_callable():
    from etl.extract import extract_products
    from etl.transform import transform_products
    from etl.load import load_to_postgres

    products = extract_products()
    transformed = transform_products(products)
    load_to_postgres(transformed)


def build_analytics_callable():
    import psycopg2
    from airflow.hooks.base import BaseHook

    conn_airflow = BaseHook.get_connection("etl_postgres")

    conn = psycopg2.connect(
        host=conn_airflow.host,
        port=conn_airflow.port,
        dbname=conn_airflow.schema,
        user=conn_airflow.login,
        password=conn_airflow.password,
    )

    cur = conn.cursor()
    with open("/opt/airflow/sql/create_analytics_tables.sql", "r") as f:
        cur.execute(f.read())

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="api_etl_pipeline",
    default_args=default_args,
    description="Production-style ETL pipeline orchestrated by Airflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "airflow", "postgres"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_task_callable,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task_callable,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_task_callable,
    )

    analytics_task = PythonOperator(
        task_id="build_analytics_task",
        python_callable=build_analytics_callable,
    )

    extract_task >> transform_task >> load_task >> analytics_task
