import time
import json
import requests
from kafka import KafkaProducer
from urllib.parse import urlparse
import hmac
import hashlib
import os 
from datetime import datetime

API_KEY = os.getenv("COINCHECK_API_KEY")
API_SECRET = os.getenv("COINCHECK_API_SECRET")
producer = None

if not API_KEY or not API_SECRET:
    raise RuntimeError("Coincheck API credentials missing")

def create_nonce():
    now = datetime.utcnow() 
    nonce = str(int(now.timestamp() * 1000))
    return nonce

def create_path(url):
    urlparsed = urlparse(url)
    path = urlparsed.path
    if urlparsed.query:
        path += "?" + urlparsed.query
    return path

def create_signature(url, nonce, body=""):
    path = create_path(url)
    message = nonce + path + body
    signature = hmac.new(
        API_SECRET.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return signature

def request_sample_get(url, params=None):
    nonce = create_nonce()
    signature = create_signature(url, nonce)
    headers = {
        "ACCESS-KEY": API_KEY,
        "ACCESS-NONCE": str(nonce),
        "ACCESS-SIGNATURE": signature
    }
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        try:

    except Error as e:

    response.raise_for_status()
    return response.json()

def request_sample_post(url, params):
    nonce = create_nonce()
    body = json.dumps(params)
    signature = create_signature(url, nonce, body)
    headers = {
        "ACCESS-KEY": API_KEY,
        "ACCESS-NONCE": str(nonce),
        "ACCESS-SIGNATURE": signature
    }
    response = requests.post(url, headers=headers, data=body, timeout=10)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

def request_sample_delete(url):
    nonce = create_nonce()
    signature = create_signature(url, nonce)
    headers = {
        "ACCESS-KEY": API_KEY,
        "ACCESS-NONCE": str(nonce),
        "ACCESS-SIGNATURE": signature
    }
    response = requests.delete(url, headers=headers, timeout=10)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()


def calc_rate_producer():
    url = "https://coincheck.com/api/exchange/orders/rate"
    params = {
    "order_type": "sell",
    "pair": "btc_jpy",
    "amount": 0.01,
    "rate": 2800
    }
    data_fetched = request_sample_post(url, params)
    producer.send('market.rate', data_fetched)

def market_sell_producer():
    url = "https://coincheck.com/api/exchange/orders"   
    params = {
    "pair": "btc_jpy",
    "order_type": "sell",
    "amount": 0.01,
    "rate":2800
    }
    data_fetched = request_sample_post(url, params)
    producer.send('order.sell', data_fetched)

def market_buy_producer():
    url = "https://coincheck.com/api/exchange/orders"   
    params = {
    "pair": "btc_jpy",
    "order_type": "buy",
    "amount": 0.01,
    "rate":2800
    }
    data_fetched = request_sample_post(url, params)
    producer.send('order.buy', data_fetched)
    
def cancel_order_producer():
    url = "https://coincheck.com/api/exchange/orders/12345678"
    data_fetched = request_sample_delete(url)
    producer.send('order.cancel', data_fetched)

def transaction_history_producer():
    url = "https://coincheck.com/api/exchange/orders/transactions"
    data_fetched = request_sample_get(url)
    producer.send('order.history', data_fetched)


def transaction_history_pagination_producer():
    url = "https://coincheck.com/api/exchange/orders/transactions_pagination"
    data_fetched = request_sample_get(url)
    producer.send('order.historypag',data_fetched)

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
        calc_rate_producer()
        market_sell_producer()
        market_buy_producer()
        cancel_order_producer()
        transaction_history_producer()
        transaction_history_pagination_producer()
        time.sleep(5)

def stream_file(filename="trades.csv"):
    with open(filename) as f:
        for line in f:
            next(f)# Skip header`
            record = parse_line(line)
            record["source_file"] = filename
            if record:
                producer.send("market.trades", record)
            else:
                producer.send("market.trades.invalid", {"raw": line})

while producer is None: #Connecting to Kafka with retry mechanism
    try: 
        producer = KafkaProducer(bootstrap_servers=['broker1:9092','broker2:9092'], value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        break
    except Exception as e: 
        print("Kafka not ready, retrying in 5s...", e) 
        time.sleep(5)
    stream_file("trades.csv")
    stream_API()

