from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv
import json
import time
from kafka import KafkaProducer, KafkaConsumer

# Import your functions
from kafka_tutorial.public_producer import stream_file as kafka_stream_file, simulate_trades_from_csv, simulate_dashboard,public_trades_producer
from kafka_tutorial.consumer import extract_from_stream as kafka_extract_from_stream, clean_data, merge_trades_csv, consume_trades_from_kafka, load_data, extract_from_stream, generate_dashboard
from kafka_tutorial.public_producer import parse_line

bootstrap_servers = ["broker1:9092", "broker2:9092"]

def consumer_trades_from_kafka():
    consume_trades_from_kafka("/opt/airflow/temp/staging.csv")

def stream_csv_to_staging():
    simulate_trades_from_csv("/app/kafka_tutorial/trades.csv")

def clean_trade_data():
    clean_data(
        input_path="/opt/airflow/temp/staging.csv",
        output_path="/opt/airflow/temp/transformed.csv"
    )

def merge_datasets():
    global market_state
    merge_trades_csv("/opt/airflow/temp/transformed.csv", "/opt/airflow/temp/final.csv")

def update_dashboard():
    generate_dashboard(
            input_path="/opt/airflow/temp/final.csv",
            output_dir="/opt/airflow/temp/dashboard"
        )

# ----------------- DAG Definition -------------------

with DAG(
    dag_id="user_analytics",
    start_date=datetime(2026, 3, 23),
    schedule_interval="@daily",
    catchup=False,
    tags=["live"]
) as dag:

    stream_csv_task = PythonOperator(
        task_id="stream_csv_to_staging",
        python_callable=stream_csv_to_staging
    )

    consume_task = PythonOperator(
        task_id="consume_trades_from_kafka",
        python_callable=consumer_trades_from_kafka
    )

    extract_task = PythonOperator(
        task_id="extract_staging_data",
        python_callable=lambda: extract_from_stream("/app/kafka_tutorial/trades.csv", topic="market.trades")
    )

    clean_task = PythonOperator(
        task_id="clean_trade_data",
        python_callable=clean_trade_data
    )

    merge_task = PythonOperator(
        task_id="merge_datasets",
        python_callable=merge_datasets
    )

    load_task = PythonOperator(
        task_id="load_final_data",
        python_callable=lambda: load_data(
            input_path="/opt/airflow/temp/transformed.csv",
            target_path="/tmp/airflow_data/final.csv"
        )
    )

    dashboard_task = PythonOperator(
        task_id="update_dashboard",
        python_callable=update_dashboard
    )

    # ----------------- DAG Dependencies ----------------
stream_csv_task >> consume_task >> extract_task >> clean_task >> merge_task >> load_task >> dashboard_task

