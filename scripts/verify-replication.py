#!/usr/bin/env python3
"""
Automated Verification Script for Debezium CDC Pipeline
Tests INSERT, UPDATE, and DELETE in MongoDB and verifies real-time propagation into PostgreSQL.
"""

import sys
import time
import uuid

try:
    from pymongo import MongoClient
    import psycopg2
except ImportError:
    print("Dependencies missing. Run: pip install pymongo psycopg2-binary")
    sys.exit(1)

MONGO_URI = "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true"
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "inventory_sink"
PG_USER = "postgres"
PG_PASSWORD = "postgres_password"

def main():
    print("=================================================================")
    print("  Debezium Event-Driven CDC Python Verification Suite           ")
    print("=================================================================")

    # 1. Connect to MongoDB
    print("\nConnecting to MongoDB...")
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_db = mongo_client["inventory"]
    customers_col = mongo_db["customers"]
    print("Connected to MongoDB.")

    # 2. Connect to PostgreSQL
    print("Connecting to PostgreSQL...")
    pg_conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    pg_conn.autocommit = True
    pg_cursor = pg_conn.cursor()
    print("Connected to PostgreSQL.")

    test_id = f"test_{int(time.time())}"
    print(f"\nGenerated Unique Test ID: {test_id}")

    # -------------------------------------------------------------
    # TEST 1: INSERT
    # -------------------------------------------------------------
    print(f"\n[STEP 1] Inserting document into MongoDB 'inventory.customers'...")
    doc = {
        "_id": test_id,
        "first_name": "Antigravity",
        "last_name": "Engineer",
        "email": f"test.{test_id}@example.com",
        "phone": "+1-800-555-0199",
        "address": {"city": "San Francisco", "state": "CA", "country": "USA"},
        "status": "ACTIVE"
    }
    customers_col.insert_one(doc)
    print("Document inserted into MongoDB. Waiting for CDC replication...")

    found = False
    for attempt in range(15):
        time.sleep(1)
        pg_cursor.execute("SELECT id, first_name, email, status FROM customers WHERE id = %s", (test_id,))
        row = pg_cursor.fetchone()
        if row:
            print(f" -> Found in PostgreSQL: id={row[0]}, name={row[1]}, email={row[2]}, status={row[3]}")
            assert row[1] == "Antigravity"
            assert row[3] == "ACTIVE"
            found = True
            break
        print(f" -> Waiting... ({attempt + 1}/15)")

    if not found:
        print("[FAIL] Insert was not replicated to PostgreSQL within timeout!")
        sys.exit(1)
    print("[PASS] STEP 1: INSERT successfully replicated!")

    # -------------------------------------------------------------
    # TEST 2: UPDATE
    # -------------------------------------------------------------
    print(f"\n[STEP 2] Updating document in MongoDB (status: PROMOTED)...")
    customers_col.update_one(
        {"_id": test_id},
        {"$set": {"status": "PROMOTED", "email": f"promoted.{test_id}@example.com"}}
    )
    print("Document updated in MongoDB. Waiting for CDC update replication...")

    found = False
    for attempt in range(15):
        time.sleep(1)
        pg_cursor.execute("SELECT status, email, last_cdc_op FROM customers WHERE id = %s", (test_id,))
        row = pg_cursor.fetchone()
        if row and row[0] == "PROMOTED":
            print(f" -> Updated in PostgreSQL: status={row[0]}, email={row[1]}, op={row[2]}")
            found = True
            break
        print(f" -> Waiting... ({attempt + 1}/15)")

    if not found:
        print("[FAIL] Update was not replicated to PostgreSQL within timeout!")
        sys.exit(1)
    print("[PASS] STEP 2: UPDATE successfully replicated!")

    # -------------------------------------------------------------
    # TEST 3: DELETE
    # -------------------------------------------------------------
    print(f"\n[STEP 3] Deleting document in MongoDB...")
    customers_col.delete_one({"_id": test_id})
    print("Document deleted in MongoDB. Waiting for CDC delete replication...")

    found = False
    for attempt in range(15):
        time.sleep(1)
        pg_cursor.execute("SELECT deleted, last_cdc_op FROM customers WHERE id = %s", (test_id,))
        row = pg_cursor.fetchone()
        if row and row[0] is True:
            print(f" -> Delete confirmed in PostgreSQL: deleted={row[0]}, op={row[1]}")
            found = True
            break
        print(f" -> Waiting... ({attempt + 1}/15)")

    if not found:
        print("[FAIL] Delete was not replicated to PostgreSQL within timeout!")
        sys.exit(1)
    print("[PASS] STEP 3: DELETE successfully replicated!")

    print("\n=================================================================")
    print("  ALL VERIFICATION TESTS COMPLETED SUCCESSFULLY!                ")
    print("=================================================================")

if __name__ == "__main__":
    main()
