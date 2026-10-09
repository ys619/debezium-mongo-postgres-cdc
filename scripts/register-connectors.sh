#!/usr/bin/env bash
set -e

CONNECT_URL=${CONNECT_URL:-"http://localhost:8083"}
CONFIG_FILE=${1:-"$(dirname "$0")/../configs/mongodb-source-connector.json"}

echo "=========================================================="
echo "Registering Debezium MongoDB Source Connector"
echo "Target Kafka Connect: ${CONNECT_URL}"
echo "=========================================================="

echo "Waiting for Kafka Connect REST API to be ready..."
until curl -s -f -o /dev/null "${CONNECT_URL}/connectors"; do
  echo "Kafka Connect is starting up, retrying in 3 seconds..."
  sleep 3
done

echo "Kafka Connect is ready!"

# Pre-create Kafka topics so consumers discover them immediately
echo "Ensuring Kafka CDC topics exist..."
docker exec kafka /kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic cdc.inventory.customers --partitions 1 --replication-factor 1 2>/dev/null || true
docker exec kafka /kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic cdc.inventory.orders --partitions 1 --replication-factor 1 2>/dev/null || true


# Check if connector is already registered
CONNECTOR_NAME=$(grep -o '"name": *"[^"]*"' "$CONFIG_FILE" | head -1 | cut -d'"' -f4)

if curl -s -f "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" > /dev/null; then
  echo "Connector '${CONNECTOR_NAME}' already exists. Updating configuration..."
  curl -s -X PUT \
    -H "Content-Type: application/json" \
    --data "$(cat "$CONFIG_FILE" | grep -v '^{"name":' | sed 's/^{//' | sed 's/}$//' | sed 's/"config": *//')" \
    "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" | jq . || cat
else
  echo "Registering new connector '${CONNECTOR_NAME}'..."
  curl -s -i -X POST \
    -H "Accept:application/json" \
    -H "Content-Type:application/json" \
    "${CONNECT_URL}/connectors/" \
    -d @"$CONFIG_FILE"
fi

echo ""
echo "Current connector status:"
sleep 2
curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | (command -v jq >/dev/null && jq . || cat)
echo ""
echo "Done!"
