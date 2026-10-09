#!/usr/bin/env bash
# ==============================================================================
# End-to-End CDC Verification Script (Docker exec based, 0 dependencies needed)
# ==============================================================================
set -e

GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${CYAN}=================================================================${NC}"
echo -e "${CYAN}  Debezium Event-Driven CDC Test: MongoDB -> Kafka -> PostgreSQL ${NC}"
echo -e "${CYAN}=================================================================${NC}"

TEST_ID="test_$(date +%s)"
echo -e "Generated Test ID: ${YELLOW}${TEST_ID}${NC}"

# ------------------------------------------------------------------------------
# STEP 1: INSERT into MongoDB
# ------------------------------------------------------------------------------
echo -e "\n${CYAN}>>> STEP 1: Inserting new customer into MongoDB (inventory.customers)...${NC}"
docker exec mongodb mongosh inventory --quiet --eval "
  db.customers.insertOne({
    _id: '${TEST_ID}',
    first_name: 'Alex',
    last_name: 'Morgan',
    email: 'alex.${TEST_ID}@example.com',
    phone: '+1-555-0100',
    address: { city: 'Bengaluru', country: 'India' },
    status: 'ACTIVE'
  });
"

echo -e "Waiting for CDC event to propagate through Kafka to PostgreSQL..."
SUCCESS=0
for i in {1..15}; do
  RESULT=$(docker exec postgres psql -U postgres -d inventory_sink -t -A -c "SELECT id, email, status FROM customers WHERE id = '${TEST_ID}';")
  if [ -n "$RESULT" ]; then
    echo -e "${GREEN}[SUCCESS] Found in PostgreSQL:${NC} $RESULT"
    SUCCESS=1
    break
  fi
  sleep 1
done

if [ $SUCCESS -ne 1 ]; then
  echo -e "${RED}[FAILED] Insert was not replicated to PostgreSQL within 15 seconds!${NC}"
  exit 1
fi

# ------------------------------------------------------------------------------
# STEP 2: UPDATE in MongoDB
# ------------------------------------------------------------------------------
echo -e "\n${CYAN}>>> STEP 2: Updating customer in MongoDB (status: VIP, updated email)...${NC}"
docker exec mongodb mongosh inventory --quiet --eval "
  db.customers.updateOne(
    { _id: '${TEST_ID}' },
    { \$set: { status: 'VIP', email: 'vip.${TEST_ID}@example.com' } }
  );
"

echo -e "Waiting for CDC UPDATE event to propagate..."
SUCCESS=0
for i in {1..15}; do
  RESULT=$(docker exec postgres psql -U postgres -d inventory_sink -t -A -c "SELECT id, email, status, last_cdc_op FROM customers WHERE id = '${TEST_ID}';")
  if [[ "$RESULT" == *"VIP"* ]]; then
    echo -e "${GREEN}[SUCCESS] Updated in PostgreSQL:${NC} $RESULT"
    SUCCESS=1
    break
  fi
  sleep 1
done

if [ $SUCCESS -ne 1 ]; then
  echo -e "${RED}[FAILED] Update was not replicated to PostgreSQL within 15 seconds!${NC}"
  exit 1
fi

# ------------------------------------------------------------------------------
# STEP 3: DELETE in MongoDB
# ------------------------------------------------------------------------------
echo -e "\n${CYAN}>>> STEP 3: Deleting customer in MongoDB...${NC}"
docker exec mongodb mongosh inventory --quiet --eval "
  db.customers.deleteOne({ _id: '${TEST_ID}' });
"

echo -e "Waiting for CDC DELETE event to propagate..."
SUCCESS=0
for i in {1..15}; do
  RESULT=$(docker exec postgres psql -U postgres -d inventory_sink -t -A -c "SELECT id, deleted, last_cdc_op FROM customers WHERE id = '${TEST_ID}';")
  if [[ "$RESULT" == *"t|d"* ]]; then
    echo -e "${GREEN}[SUCCESS] Deleted record marked in PostgreSQL:${NC} $RESULT"
    SUCCESS=1
    break
  fi
  sleep 1
done

if [ $SUCCESS -ne 1 ]; then
  echo -e "${RED}[FAILED] Delete was not replicated to PostgreSQL within 15 seconds!${NC}"
  exit 1
fi

# ------------------------------------------------------------------------------
# Audit Log Check
# ------------------------------------------------------------------------------
echo -e "\n${CYAN}>>> Checking CDC Audit Log entries in PostgreSQL...${NC}"
docker exec postgres psql -U postgres -d inventory_sink -c "
  SELECT audit_id, source_collection, document_id, operation, cdc_timestamp
  FROM cdc_audit_log
  WHERE document_id = '${TEST_ID}'
  ORDER BY audit_id ASC;
"

echo -e "${GREEN}=================================================================${NC}"
echo -e "${GREEN}  ALL CDC REPLICATION TESTS PASSED!                             ${NC}"
echo -e "${GREEN}  MongoDB -> Debezium Oplog -> Kafka -> PostgreSQL Verified!     ${NC}"
echo -e "${GREEN}=================================================================${NC}"
