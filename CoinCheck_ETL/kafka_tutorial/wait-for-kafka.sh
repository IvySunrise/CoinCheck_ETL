#!/bin/sh
echo "Waiting for broker1..."
until nc -z broker1 9092; do
  echo "Broker1 not ready..."
  sleep 5
done
echo "Waiting for broker2..."
until nc -z broker2 9093; do
  echo "Broker2 not ready..."
  sleep 5
done
echo "Kafka is ready, starting producer..."
exec python -u /app/public_producer.py