import pathlib
import polars as pl
import gc

# Define paths for data directories
import_data_path = pathlib.Path.cwd() / "imported_data"
cleaned_data_path = pathlib.Path.cwd() / "clean_data"

# Ensure the cleaned_data directory is created
cleaned_data_path.mkdir(parents=True, exist_ok=True)

# List all CSV files in the imported_data directory
csv_files = list(import_data_path.glob("*.csv"))

for csv_file in csv_files:
    print(f"Processing {csv_file.name}")

    df = pl.read_csv(csv_file)
    df = df.rename({"# Timestamp": "timestamp"})
    new_column_names = {col: col.lower().replace(' ', '_') for col in df.columns}
    df = df.rename(new_column_names)

    # Clean and parse Timestamp
    df = df.with_columns(
        pl.col("timestamp")
        .str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M:%S", strict=False)
    )

    # Add 'date' column and convert to string
    df = df.with_columns(
        pl.col("timestamp").dt.date().cast(pl.Utf8).alias("date")  # Cast date to string
    )

    # Format Timestamp to ISO string (before saving) and cast to Utf8
    df = df.with_columns(
        pl.col("timestamp").dt.strftime("%Y-%m-%dT%H:%M:%S").cast(pl.Utf8)
    )

    df = df.unique(subset=["timestamp", "mmsi"], maintain_order=True)
    cleaned_filename = csv_file.stem + '-cleaned.parquet'
    df.write_parquet(cleaned_data_path / cleaned_filename)
    print(f"Saved cleaned file as {cleaned_filename}")

    # Deleting DataFrame and forcing garbage collection
    del df
    gc.collect()

print("Process complete")