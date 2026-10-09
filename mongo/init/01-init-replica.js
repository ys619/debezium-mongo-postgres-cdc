// MongoDB Replica Set & Sample Data Initialization
// This runs on MongoDB startup or can be executed via mongosh

try {
  const status = rs.status();
  print("Replica set status already OK: " + status.set);
} catch (e) {
  print("Initializing replica set rs0...");
  rs.initiate({
    _id: "rs0",
    members: [
      { _id: 0, host: "mongodb:27017" }
    ]
  });
  print("Replica set rs0 initialized.");
}

// Switch to inventory database
const dbName = "inventory";
const targetDb = db.getSiblingDB(dbName);

// Ensure collection exists so Debezium can detect change streams immediately
if (!targetDb.getCollectionNames().includes("customers")) {
  targetDb.createCollection("customers");
  print("Created collection: inventory.customers");
}

if (!targetDb.getCollectionNames().includes("orders")) {
  targetDb.createCollection("orders");
  print("Created collection: inventory.orders");
}
