from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import csv

from kafka_tutorial.public_producer import public_trades_producer
from kafka_tutorial.consumer import consume_trades_from_kafka, clean_data, merge_trades_csv, load_data

STATIC_STAGING_CSV = "/opt/airflow/temp/static_trades.csv"
STATIC_TRANSFORMED_CSV = "/opt/airflow/temp/static_transformed.csv"
STATIC_FINAL_CSV = "/opt/airflow/temp/static_final.csv"

def produce_static_trades():
    trades = [
        {"id": "1", "amount": 0.01, "rate": 5000000, "pair": "btc_jpy", "order_type": "buy", "created_at": "2024-01-01T00:00:00Z"},
        {"id": "2", "amount": 0.02, "rate": 5001000, "pair": "btc_jpy", "order_type": "sell", "created_at": "2024-01-01T00:00:01Z"},
        {"id": "3", "amount": 0.015, "rate": 4999000, "pair": "btc_jpy", "order_type": "buy", "created_at": "2024-01-01T00:00:02Z"},
        {"id": "4", "amount": 0.03, "rate": 5002000, "pair": "btc_jpy", "order_type": "sell", "created_at": "2024-01-01T00:00:03Z"},
    ]

    os.makedirs(os.path.dirname(STATIC_STAGING_CSV), exist_ok=True)
    with open(STATIC_STAGING_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=trades[0].keys())
        writer.writeheader()
        writer.writerows(trades)

    print(f"Static trades written to {STATIC_STAGING_CSV}")

def extract_static_trades():
    consume_trades_from_kafka(
        topic="market.trades",
        staging_path=STATIC_STAGING_CSV
    )

    import os
    print(
        "FILE SIZE:",
        os.path.getsize(STATIC_STAGING_CSV)
        if os.path.exists(STATIC_STAGING_CSV)
        else "NO FILE"
    )

def clean_static_trades():
    clean_data(
        input_path=STATIC_STAGING_CSV,
        output_path=STATIC_TRANSFORMED_CSV
    )

def merge_static_trades_task():
    merge_trades_csv(cleaned_csv=STATIC_TRANSFORMED_CSV)

def load_static_data_task():
    os.makedirs(os.path.dirname(STATIC_FINAL_CSV), exist_ok=True)
    load_data(
        input_path=STATIC_TRANSFORMED_CSV,
        target_path=STATIC_FINAL_CSV
    )

with DAG(
    dag_id="static_trades_analytics",
    start_date=datetime(2026, 3, 28),
    schedule_interval="@daily",
    catchup=False,
    tags=["static"]
) as dag:

    static_produce = PythonOperator(
        task_id="produce_static_trades",
        python_callable=produce_static_trades
    )

    static_extract = PythonOperator(
        task_id="extract_static_trades",
        python_callable=extract_static_trades
    )

    static_clean = PythonOperator(
        task_id="clean_static_trades",
        python_callable=clean_static_trades
    )

    static_merge = PythonOperator(
        task_id="merge_static_trades",
        python_callable=lambda: merge_trades_csv(STATIC_TRANSFORMED_CSV, STATIC_FINAL_CSV)
    )

    static_load = PythonOperator(
        task_id="load_static_data",
        python_callable=load_static_data_task
    )

    static_produce >> static_extract >> static_clean >> static_merge >> static_load