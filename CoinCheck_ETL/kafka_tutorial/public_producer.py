from kafka import KafkaProducer
import time
from kafka_tutorial.Errors import producer_get_error
from kafka_tutorial.Errors import producer_send_error
from kafka_tutorial.Errors import import_error
from kafka_tutorial.kafka_functions import public_get
import csv
import json
import requests

producer = None

#//Producer function for Order Ticker//
def ticker_producer(): 
    url = "https://coincheck.com/api/ticker"
    params = {
    "pair": "btc_jpy"
    }
    data_fetched = producer_get_error(url, params)
    producer_send_error(producer,'market.ticker', data_fetched)

def public_trades_producer(live_only=False, topic="market.trades"):
    from kafka import KafkaProducer
import requests, json
from datetime import datetime

last_sent_timestamp = None  # global, persists across calls

def public_trades_producer(live_only=False, topic="market.trades"):
    global last_sent_timestamp

    url = "https://coincheck.com/api/trades"
    params = {"pair": "btc_jpy"}

    producer = KafkaProducer(
        bootstrap_servers=['broker1:9092', 'broker2:9092'],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data_fetched = resp.json()
    except Exception as e:
        print("Error fetching API data:", e)
        return

    trades = data_fetched.get("data", [])
    if not trades:
        print("No trades fetched")
        return

    # Convert created_at to datetime
    # Convert created_at to datetime for comparison
    for t in trades:
        t['created_at'] = datetime.fromisoformat(t['created_at'].replace('Z', '+00:00'))

    # Filter only new trades
    if live_only and last_sent_timestamp:
        trades = [t for t in trades if t['created_at'] > last_sent_timestamp]

    if trades:
        # Update last_sent_timestamp
        last_sent_timestamp = max(t['created_at'] for t in trades)

        # Send trades to Kafka as JSON-serializable dicts
        for t in trades:
            t_to_send = t.copy()
            t_to_send['created_at'] = t_to_send['created_at'].isoformat()  # convert back to string
            producer.send(topic, t_to_send)

        producer.flush()
        print(f"{len(trades)} trades sent to {topic}")
    else:
        print("No new live trades to send")

def order_books_producer():
    url = "https://coincheck.com/api/order_books"
    params = {
    "pair": "btc_jpy"
    }
    data_fetched = producer_get_error(url, params)
    producer_send_error(producer, 'market.orderbooks', data_fetched)

def standard_rate_producer():
    url = "https://coincheck.com/api/rate/btc_jpy"
    data_fetched = producer_get_error(url)
    producer_send_error(producer,'market.rate', data_fetched)

def parse_line(line):
    fields = line.strip().split(",")
    try:
        return {
            "timestamp": fields[0],
            "price": float(fields[1]),
            "volume": float(fields[2]),
            "source":"file"
        }   
    except (IndexError, ValueError) as e:
        print(f"Error parsing line: {e}")
        return None

def stream_API():
    while True:
        standard_rate_producer()
        ticker_producer()
        public_trades_producer()
        order_books_producer()
        time.sleep(5)

def stream_file(filename="trades.csv"):
    producer = None
    bootstrap_servers = ['broker1:9092', 'broker2:9092']
    max_retries = 12  # 1 minute total
    retries = 0
    while producer is None and retries < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['broker1:9092', 'broker2:9092'],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Connected to Kafka!")
            with open(filename) as f:
                try:
                    next(f)
                except StopIteration:
                    print("File is empty, nothing to stream")
                    return
                for line in f:
                    record = parse_line(line)
                    record["source_file"] = filename
                    if record:
                        producer.send("market.trades", record)
                    else:
                        producer.send("market.trades.invalid", {"raw": line})
        except Exception as e:
            retries += 1
            print(f"Kafka not ready, retry {retries}, waiting 5s...", e)
            time.sleep(5)

    if producer is None:
        raise RuntimeError("Failed to connect to Kafka after multiple retries")


def simulate_trades_from_csv(filename="/app/trades.csv", topic="market.trades"):
    """
    Reads a CSV file and streams each row as a separate Kafka message.
    """
    producer = KafkaProducer(
        bootstrap_servers=['broker1:9092', 'broker2:9092'],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Optionally convert numeric fields
            if "price" in row:
                row["price"] = float(row["price"])
            if "quantity" in row:
                row["quantity"] = float(row["quantity"])
            producer.send(topic, row)
            time.sleep(0.5)  # simulate live streaming

    producer.flush()
    print(f"All {reader.line_num-1} trades from CSV sent to {topic}")

def simulate_dashboard(filename, topic="market.trades"):
    producer = KafkaProducer(
        bootstrap_servers=['broker1:9092', 'broker2:9092'],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    with open(filename) as f:
        reader = csv.DictReader(f)

        for row in reader:
            try:
                trade = {
                    "id": int(row["id"]),
                    "amount": float(row["amount"]),
                    "rate": float(row["rate"]),
                    "pair": row["pair"],
                    "order_type": row["order_type"],
                    "created_at": row["created_at"]
                }

                producer.send(topic, trade)
                print("Sent:", trade)

                time.sleep(0.5)

            except Exception as e:
                print("Error:", e)

    producer.flush()