from kafka import KafkaConsumer
import json
import time
import os
import csv
import pandas as pd
import matplotlib.pyplot as plt

market_state = pd.DataFrame(columns=['id', 'amount', 'rate', 'pair', 'order_type', 'created_at'])

def consume_trades_from_kafka(staging_path,topic="market.trades"):
    """Safely consume Kafka messages and write to CSV, only when called."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['broker1:9092','broker2:9092'],
        group_id="live-consumer-group",   # ✅ IMPORTANT
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000
    )

    records = [msg.value for msg in consumer]
    consumer.close()

    if not records:
        print("No messages found in Kafka topic.")
        return

    os.makedirs(os.path.dirname(staging_path), exist_ok=True)
    with open(staging_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(records[0].keys())  # write header
        for record in records:
            writer.writerow(record.values())
    print(f"{len(records)} records written to {staging_path}")

def clean_data(input_path, output_path):
    df = pd.read_csv(input_path)
    new_df = df.dropna()
    expected_columns = ['id', 'amount', 'rate', 'pair', 'order_type', 'created_at']
    for col in expected_columns:
        if col not in new_df.columns:
            new_df[col] = None
    new_df['id'] = pd.to_numeric(new_df['id'], errors='coerce')
    new_df['amount'] = pd.to_numeric(new_df['amount'], errors='coerce')
    new_df['rate'] = pd.to_numeric(new_df['rate'], errors='coerce')

    # Drop rows with invalid pair or order_type
    new_df = new_df[~new_df['pair'].isnull()]  # keep only rows with a pair
    new_df = new_df[new_df['pair'] == 'btc_jpy']  # only BTC/JPY
    new_df = new_df[new_df['order_type'].isin(['buy', 'sell'])]  # only valid order types

    # Convert timestamp to datetime
    new_df['created_at'] = pd.to_datetime(new_df['created_at'], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # Drop rows where conversion failed
    new_df.dropna(subset=['id', 'amount', 'rate', 'created_at'], inplace=True)

    # Save cleaned CSV
    new_df.to_csv(output_path, index=False)
    print(f"Cleaned data saved to {output_path}, {len(new_df)} rows kept.")

def merge_trades_csv(cleaned_csv, output_csv):
    import os, csv, datetime

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    # Read new trades
    with open(cleaned_csv, newline='') as f:
        reader = csv.DictReader(f)
        new_trades = list(reader)

    # Read existing merged data
    if os.path.exists(output_csv):
        with open(output_csv, newline='') as f:
            reader = csv.DictReader(f)
            merged = list(reader)
    else:
        merged = []

    # Combine and remove duplicates by 'id'
    seen_ids = set()
    combined = []
    for row in merged + new_trades:
        if row['id'] not in seen_ids:
            combined.append(row)
            seen_ids.add(row['id'])

    # Sort by 'created_at' as datetime
    combined.sort(key=lambda x: datetime.datetime.fromisoformat(x['created_at'].replace('Z', '+00:00')))

    # Write merged CSV
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=combined[0].keys())
        writer.writeheader()
        writer.writerows(combined)

    print(f"Merged {len(new_trades)} trades. Total records now: {len(combined)}")

def extract_from_stream(staging_path, topic="market.trades"):
    records = consume_trades_from_kafka(topic)
    if not records:
        print("No messages found in Kafka topic.")
        return
    os.makedirs(os.path.dirname(staging_path), exist_ok=True)
    with open(staging_path, 'w', newline='') as f:
        writer = csv.writer(f)
        for record in records:
            writer.writerow(record.values())

    print(f"Success! {len(records)} records written to {staging_path}")
    

def load_data(input_path, target_path):

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"{input_path} does not exist! Make sure upstream tasks ran successfully.")

    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    df = pd.read_csv(input_path)
    df.to_csv(target_path, index=False)
    print(f"Loaded data to {target_path}")

def generate_dashboard(input_path, output_dir):
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Load data
    df = pd.read_csv(input_path)

    if df.empty:
        print("No data for dashboard.")
        return

    # Convert timestamp
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df.sort_values('created_at', inplace=True)

    df['rolling_avg'] = df['rate'].rolling(window=5).mean()
    plt.plot(df['created_at'], df['rolling_avg'])
    # -------------------------
    # 📊 Metrics
    # -------------------------
    avg_price = df['rate'].mean()
    total_volume = df['amount'].sum()
    buy_count = (df['order_type'] == 'buy').sum()
    sell_count = (df['order_type'] == 'sell').sum()

    print(f"Average Price: {avg_price}")
    print(f"Total Volume: {total_volume}")
    print(f"Buy Orders: {buy_count}")
    print(f"Sell Orders: {sell_count}")

    # -------------------------
    # 📈 Price over time
    # -------------------------
    plt.figure()
    plt.plot(df['created_at'], df['rate'])
    plt.title("BTC/JPY Price Over Time")
    plt.xlabel("Time")
    plt.ylabel("Price")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{output_dir}/price_chart.png")
    plt.close()

    # -------------------------
    # 📊 Buy vs Sell
    # -------------------------
    plt.figure()
    plt.bar(['Buy', 'Sell'], [buy_count, sell_count])
    plt.title("Buy vs Sell Orders")
    plt.tight_layout()
    plt.savefig(f"{output_dir}/buy_sell_chart.png")
    plt.close()

    print(f"Dashboard generated in {output_dir}")