from pymongo import MongoClient

# Connect to the MongoDB router (mongos)
client = MongoClient("mongodb://localhost:27020")  # Make sure the port is correct for your mongos

# Use consistent database and collection names aligned with your setup
# Assuming consistent naming aligning with previous scripts
db = client["ais_db"]  # Change to "ais_db" if consistent with your previous setup
collection = db["vesseldata"]  # Change to "vesseldata" for consistency

# Fields to index for efficient filtering and retrieval
fields_to_index = [
    "timestamp",
    "date",
    "mmsi"
]

# Create indexes on these fields
for field in fields_to_index:
    print(f"📌 Creating index on '{field}'...")
    collection.create_index(field)

print("✅ All indexes created.")