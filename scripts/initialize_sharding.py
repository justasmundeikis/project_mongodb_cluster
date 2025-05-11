from pymongo import MongoClient, errors

# Connect to the MongoDB sharded cluster via mongos
client = MongoClient("mongodb://localhost:27020")  # Connecting to mongos

try:
    # Add Shard 1 and Shard 2 to the cluster
    print("⏳ Adding Shard 1...")
    client.admin.command("addShard", "shard1RS/shard1-node1:27018,shard1-node2:27018,shard1-node3:27018")

    print("⏳ Adding Shard 2...")
    client.admin.command("addShard", "shard2RS/shard2-node1:27019,shard2-node2:27019,shard2-node3:27019")

    # Enable sharding for the ais_db database
    print("⚙️ Enabling Sharding for the ais_db database...")
    client.admin.command("enableSharding", "ais_db")

    # Create a hashed index on the sharding key before sharding the collection
    print("⚙️ Creating hashed index on 'mmsi' field...")
    vessel_collection = client["ais_db"]["vesseldata"]
    vessel_collection.create_index([("mmsi", "hashed")])

    # Shard the vesseldata collection by the MMSI field
    print("⚙️ Sharding collection vesseldata by MMSI...")
    client.admin.command("shardCollection", "ais_db.vesseldata", key={"mmsi": "hashed"})

    # Start the balancer to redistribute data across shards
    print("🚀 Starting the balancer...")
    client.admin.command("balancerStart")

    print("✅ Shards added, sharding enabled, and balancer started successfully!")

except errors.OperationFailure as e:
    print(f"❌ Operation failed: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")