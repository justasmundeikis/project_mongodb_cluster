import pathlib
import urllib.parse
import requests
import zipfile
from concurrent.futures import ThreadPoolExecutor

# Define a local path under your current working directory
imported_data_dir = pathlib.Path.cwd() / "imported_data"

# Create the directory if it doesn't exist
imported_data_dir.mkdir(parents=True, exist_ok=True)

url = "https://web.ais.dk/aisdata/"
files = [
    'aisdk-2025-05-01.zip',
    'aisdk-2025-05-02.zip',
    'aisdk-2025-05-03.zip',
    'aisdk-2025-05-04.zip',
    'aisdk-2025-05-05.zip',
    'aisdk-2025-05-06.zip',
    'aisdk-2025-05-07.zip',
]

def download_file(file_name, base_url, destination_dir):
    """Download a single zip file."""
    full_url = urllib.parse.urljoin(base_url, file_name)
    zip_path = destination_dir / file_name
    try:
        print(f"Downloading: {file_name} from {full_url}")
        response = requests.get(full_url, timeout=10)  # Added timeout for debugging
        response.raise_for_status()
        with open(zip_path, 'wb') as file:
            file.write(response.content)
        print(f"Download complete: {file_name}")
    except requests.RequestException as e:
        print(f"Failed to download {file_name}: {e}")

def extract_file(zip_path):
    """Extract a single zip file."""
    if zip_path.exists():
        print(f"Extracting: {zip_path.name}")
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(zip_path.parent)
            print(f"Extraction complete: {zip_path.name}")
        except zipfile.BadZipFile:
            print(f"Failed to extract {zip_path.name}: Bad zip file")

def delete_file(zip_path):
    """Delete a single zip file."""
    if zip_path.exists():
        print(f"Deleting: {zip_path.name}")
        try:
            zip_path.unlink()
            print(f"Deletion complete: {zip_path.name}")
        except Exception as e:
            print(f"Failed to delete {zip_path.name}: {e}")

# A reasonable number of workers given the I/O-bound nature
max_workers = 16


print("Starting downloads...")
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    executor.map(lambda f: download_file(f, url, imported_data_dir), files)

zip_files = list(imported_data_dir.glob("*.zip"))

print("Starting extraction...")
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    executor.map(extract_file, zip_files)

print("Starting deletion...")
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    executor.map(delete_file, zip_files)

print("Process complete.")