#!/usr/bin/env bash
set -e
cd CoinCheck_ETL
docker compose up -d
cd kafka_tutorial
docker compose up -d
echo "Waiting for Kafka to come online..."
docker exec broker1 cub kafka-ready -b broker1:9092,broker2:9093 2 30

docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic  market.ticker --partitions 1 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic  market.ticker.invalid --partitions 1 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic  market.trades --partitions 1 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic  market.trades.invalid --partitions 1 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic market.orderbooks --partitions 1 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic market.orderbooks.invalid --partitions 1 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic market.rate --partitions 1 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic market.rate.invalid --partitions 1 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic order.sell --partitions 3 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic order.sell.invalid --partitions 3 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic order.buy --partitions 3 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic order.buy.invalid --partitions 3 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic order.cancel --partitions 3 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic order.cancel.invalid --partitions 3 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic order.historypag --partitions 3 --replication-factor 2
docker exec broker1 kafka-topics --bootstrap-server broker1:9092,broker2:9093 --create --if-not-exists --topic order.historypag.invalid --partitions 3 --replication-factor 2