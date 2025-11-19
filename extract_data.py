import os
import requests 
from google.cloud import storage

TRIP_DATA_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

TRIP_DATA_FILES = [
    "yellow_tripdata_2025-01.parquet",
    "yellow_tripdata_2025-02.parquet"
]

LOOKUP_CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
LOOKUP_CSV_FILENAME = "taxi_zone_lookup.csv"

OUTPUT_DIR = "data/raw"
BUCKET_NAME = "cis9940assignment1"

os.makedirs(OUTPUT_DIR, exist_ok=True)

for filename in TRIP_DATA_FILES:
    url = f"{TRIP_DATA_BASE_URL}{filename}"
    local_path = os.path.join(OUTPUT_DIR, filename)

    print(f"Downloading {filename} from {url}")
    response = requests.get(url)
    response.raise_for_status()

    with open(local_path, "wb") as f: 
        f.write(response.content)

    print(f"Saved {filename} to {local_path}")

lookup_local_path = os.path.join(OUTPUT_DIR, LOOKUP_CSV_FILENAME)

print(f"Downloading {LOOKUP_CSV_FILENAME} from {LOOKUP_CSV_URL}")
response = requests.get(LOOKUP_CSV_URL)
response.raise_for_status()

with open(lookup_local_path, "wb") as f:
    f.write(response.content)

print(f"Saved {LOOKUP_CSV_FILENAME} to {lookup_local_path}")

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

for filename in TRIP_DATA_FILES:
    local_path = os.path.join(OUTPUT_DIR, filename)
    blob = bucket.blob(filename)
    print(f"Uploading {filename} to gs://{BUCKET_NAME}/{filename}")
    blob.upload_from_filename(local_path)

blob = bucket.blob(LOOKUP_CSV_FILENAME)
print(f"Uploading {LOOKUP_CSV_FILENAME} to gs://{BUCKET_NAME}/{LOOKUP_CSV_FILENAME}")
blob.upload_from_filename(lookup_local_path)

print("Uploaded all files to Google Cloud Storage.")
