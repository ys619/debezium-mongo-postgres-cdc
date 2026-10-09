#!/usr/bin/env python3
"""
Event-Driven CDC Receiver & Replication Service
Consumes Debezium Change Data Capture (CDC) events from Apache Kafka
and replicates/upserts records in real time to PostgreSQL.
"""

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import Json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("CDC-Replication-Service")

# Configuration from Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "debezium-postgres-replicator")
TOPIC_PATTERN = os.getenv("TOPIC_PATTERN", "^cdc\\.inventory\\..*")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "inventory_sink")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_password")


def wait_for_postgres(max_retries=30, delay=2):
    """Wait for PostgreSQL database to be reachable."""
    logger.info("Connecting to PostgreSQL at %s:%d/%s...", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=5
            )
            conn.autocommit = True
            logger.info("Successfully connected to PostgreSQL on attempt %d.", attempt)
            return conn
        except Exception as e:
            logger.warning("Postgres not ready (attempt %d/%d): %s. Retrying in %ds...", attempt, max_retries, e, delay)
            time.sleep(delay)
    raise RuntimeError("Failed to connect to PostgreSQL after multiple attempts.")


def create_kafka_consumer(max_retries=30, delay=3):
    """Wait for Kafka to be reachable and subscribe to CDC topics."""
    logger.info("Connecting to Kafka at %s subscribing to '%s'...", KAFKA_BOOTSTRAP_SERVERS, TOPIC_PATTERN)
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
                key_deserializer=lambda k: json.loads(k.decode("utf-8")) if k else None,
                consumer_timeout_ms=1000,
                metadata_max_age_ms=2000
            )
            # Subscribe to pattern
            consumer.subscribe(pattern=TOPIC_PATTERN)
            logger.info("Successfully subscribed to Kafka topics matching '%s'.", TOPIC_PATTERN)
            return consumer
        except Exception as e:
            logger.warning("Kafka not ready (attempt %d/%d): %s. Retrying in %ds...", attempt, max_retries, e, delay)
            time.sleep(delay)
    raise RuntimeError("Failed to connect to Kafka after multiple attempts.")


def parse_id(data):
    """Extract standard string ID from MongoDB _id (which could be ObjectId dict or string)."""
    if not data:
        return None
    if isinstance(data, dict):
        if "$oid" in data:
            return str(data["$oid"])
        return json.dumps(data)
    return str(data)


def parse_cdc_payload(message_value, message_key):
    """
    Extract operation, document, and metadata from Debezium CDC payload.
    Supports both Debezium with schemas and schemaless JSON.
    """
    if not message_value:
        return None

    # Handle optional schema envelope
    payload = message_value.get("payload", message_value) if isinstance(message_value, dict) else {}

    op = payload.get("op")  # 'c': Create, 'u': Update, 'd': Delete, 'r': Snapshot Read
    ts_ms = payload.get("ts_ms")
    cdc_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc) if ts_ms else datetime.now(timezone.utc)

    # Extract source info
    source = payload.get("source", {})
    collection = source.get("collection", "unknown")

    # Parse 'after' document (for create/update/snapshot)
    after_raw = payload.get("after")
    doc = None
    if after_raw:
        if isinstance(after_raw, str):
            try:
                doc = json.loads(after_raw)
            except Exception:
                doc = {"raw": after_raw}
        elif isinstance(after_raw, dict):
            doc = after_raw

    # Parse 'filter' or key for delete operations
    doc_id = None
    if doc and "_id" in doc:
        doc_id = parse_id(doc["_id"])

    if not doc_id and payload.get("filter"):
        filter_raw = payload.get("filter")
        f_dict = json.loads(filter_raw) if isinstance(filter_raw, str) else filter_raw
        if isinstance(f_dict, dict) and "_id" in f_dict:
            doc_id = parse_id(f_dict["_id"])

    if not doc_id and message_key:
        key_val = message_key.get("payload", message_key) if isinstance(message_key, dict) else {}
        if isinstance(key_val, dict) and "id" in key_val:
            k_id = key_val["id"]
            if isinstance(k_id, str) and k_id.startswith("{"):
                try:
                    k_json = json.loads(k_id)
                    doc_id = parse_id(k_json.get("_id", k_id))
                except Exception:
                    doc_id = str(k_id)
            else:
                doc_id = str(k_id)

    return {
        "op": op,
        "collection": collection,
        "doc_id": doc_id,
        "doc": doc,
        "cdc_time": cdc_time,
        "raw_payload": payload
    }


def replicate_customer(cursor, cdc_data):
    """Replicate customer change into PostgreSQL customers table."""
    op = cdc_data["op"]
    doc_id = cdc_data["doc_id"]
    doc = cdc_data["doc"]
    cdc_time = cdc_data["cdc_time"]

    if op in ("c", "u", "r"):  # Create, Update, or Snapshot Read
        first_name = doc.get("first_name")
        last_name = doc.get("last_name")
        email = doc.get("email")
        phone = doc.get("phone")
        address = Json(doc.get("address")) if doc.get("address") else None
        status = doc.get("status", "ACTIVE")

        sql = """
            INSERT INTO customers (
                id, first_name, last_name, email, phone, address, status,
                raw_document, deleted, last_cdc_op, last_cdc_timestamp, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, FALSE, %s, %s, NOW()
            )
            ON CONFLICT (id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                address = EXCLUDED.address,
                status = EXCLUDED.status,
                raw_document = EXCLUDED.raw_document,
                deleted = FALSE,
                last_cdc_op = EXCLUDED.last_cdc_op,
                last_cdc_timestamp = EXCLUDED.last_cdc_timestamp,
                updated_at = NOW();
        """
        cursor.execute(sql, (
            doc_id, first_name, last_name, email, phone, address, status,
            Json(doc), op, cdc_time
        ))
        action_name = "UPSERTED (created/read)" if op in ("c", "r") else "UPDATED"
        logger.info("[REPLICATED] Customer ID=%s %s in Postgres. (Op: %s)", doc_id, action_name, op)

    elif op == "d":  # Delete
        sql = """
            UPDATE customers SET
                deleted = TRUE,
                last_cdc_op = 'd',
                last_cdc_timestamp = %s,
                updated_at = NOW()
            WHERE id = %s;
        """
        cursor.execute(sql, (cdc_time, doc_id))
        logger.info("[REPLICATED] Customer ID=%s marked as DELETED in Postgres. (Op: d)", doc_id)


def replicate_order(cursor, cdc_data):
    """Replicate order change into PostgreSQL orders table."""
    op = cdc_data["op"]
    doc_id = cdc_data["doc_id"]
    doc = cdc_data["doc"]
    cdc_time = cdc_data["cdc_time"]

    if op in ("c", "u", "r"):
        customer_id = parse_id(doc.get("customer_id"))
        order_number = doc.get("order_number")
        total_amount = doc.get("total_amount", 0.0)
        items = Json(doc.get("items")) if doc.get("items") else None
        order_status = doc.get("order_status", "PENDING")

        sql = """
            INSERT INTO orders (
                id, customer_id, order_number, total_amount, items, order_status,
                raw_document, deleted, last_cdc_op, last_cdc_timestamp
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, FALSE, %s, %s
            )
            ON CONFLICT (id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                order_number = EXCLUDED.order_number,
                total_amount = EXCLUDED.total_amount,
                items = EXCLUDED.items,
                order_status = EXCLUDED.order_status,
                raw_document = EXCLUDED.raw_document,
                deleted = FALSE,
                last_cdc_op = EXCLUDED.last_cdc_op,
                last_cdc_timestamp = EXCLUDED.last_cdc_timestamp;
        """
        cursor.execute(sql, (
            doc_id, customer_id, order_number, total_amount, items, order_status,
            Json(doc), op, cdc_time
        ))
        logger.info("[REPLICATED] Order ID=%s UPSERTED in Postgres. (Op: %s)", doc_id, op)

    elif op == "d":
        sql = """
            UPDATE orders SET
                deleted = TRUE,
                last_cdc_op = 'd',
                last_cdc_timestamp = %s
            WHERE id = %s;
        """
        cursor.execute(sql, (cdc_time, doc_id))
        logger.info("[REPLICATED] Order ID=%s marked DELETED in Postgres. (Op: d)", doc_id)


def record_audit_log(cursor, cdc_data):
    """Optionally record event into cdc_audit_log table."""
    try:
        sql = """
            INSERT INTO cdc_audit_log (
                source_collection, document_id, operation, cdc_timestamp, payload
            ) VALUES (%s, %s, %s, %s, %s);
        """
        cursor.execute(sql, (
            cdc_data["collection"],
            cdc_data["doc_id"],
            cdc_data["op"],
            cdc_data["cdc_time"],
            Json(cdc_data["raw_payload"])
        ))
    except Exception as e:
        logger.error("Failed to write to audit log: %s", e)


def main():
    logger.info("======================================================")
    logger.info("Starting Debezium -> Kafka -> PostgreSQL CDC Replicator")
    logger.info("======================================================")

    db_conn = wait_for_postgres()
    consumer = create_kafka_consumer()

    logger.info("CDC Consumer is listening for events on Kafka...")

    while True:
        try:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                continue

            for topic_partition, msgs in records.items():
                topic_name = topic_partition.topic
                logger.info("Received %d message(s) from topic: %s", len(msgs), topic_name)

                with db_conn.cursor() as cursor:
                    for msg in msgs:
                        cdc_data = parse_cdc_payload(msg.value, msg.key)
                        if not cdc_data or not cdc_data["op"]:
                            continue

                        collection = cdc_data["collection"]
                        if collection == "unknown":
                            # deduce from topic name cdc.inventory.customers
                            parts = topic_name.split(".")
                            if len(parts) >= 3:
                                collection = parts[2]
                                cdc_data["collection"] = collection

                        logger.info("Processing CDC Event: op='%s', collection='%s', id='%s'",
                                    cdc_data["op"], collection, cdc_data["doc_id"])

                        if collection == "customers":
                            replicate_customer(cursor, cdc_data)
                        elif collection == "orders":
                            replicate_order(cursor, cdc_data)
                        else:
                            logger.info("No dedicated handler for collection '%s', recording audit log only.", collection)

                        record_audit_log(cursor, cdc_data)

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as db_err:
            logger.error("Database connection lost: %s. Reconnecting...", db_err)
            time.sleep(3)
            db_conn = wait_for_postgres()

        except Exception as e:
            logger.error("Error in consumer loop: %s", e, exc_info=True)
            time.sleep(2)


if __name__ == "__main__":
    main()
