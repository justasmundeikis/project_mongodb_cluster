import polars as pl
from pymongo import MongoClient
from pathlib import Path
import gc

# MongoDB connection setup
client = MongoClient("mongodb://localhost:27020")  # Connect to mongos router
db = client.ais_db
collection = db.vesseldata

# Configuration
parquet_dir = Path("clean_data")  # Directory containing Parquet files
chunk_size = 1_000_000

# Function to insert a chunk of data into MongoDB
def insert_chunk(data_chunk, chunk_num):
    """Insert a chunk of data into the MongoDB collection."""
    records = data_chunk.to_dicts()
    collection.insert_many(records, ordered=False)
    print(f"✅ Inserted chunk {chunk_num} ({len(records)} rows)")
    del records, data_chunk
    gc.collect()

# Process all Parquet files
print("⏳ Starting MongoDB data insertion...")

for file_number, file_path in enumerate(parquet_dir.glob("*.parquet"), start=1):
    print(f"⏳ Processing file {file_number}: {file_path.name}")
    
    df = pl.read_parquet(file_path)
    total_rows = df.height
    chunk_num = 1

    # Insert data in chunks
    for i in range(0, total_rows, chunk_size):
        chunk = df.slice(i, chunk_size)
        insert_chunk(chunk, chunk_num)
        chunk_num += 1

    print(f"✅ Completed processing file {file_number}: {file_path.name}")

print("✅ All data inserted successfully.")