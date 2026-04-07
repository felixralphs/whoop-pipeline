import os
import json
import glob
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import storage, bigquery

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BQ_RAW_DATASET = os.getenv("BQ_RAW_DATASET")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

storage_client = storage.Client(project=GCP_PROJECT_ID)
bq_client = bigquery.Client(project=GCP_PROJECT_ID)


def upload_to_gcs(local_filepath, data_type):
    """Upload raw JSON file to GCS — this becomes our source of truth"""
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    blob_name = f"{data_type}/{date_str}/{os.path.basename(local_filepath)}"
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_filepath)
    gcs_uri = f"gs://{GCS_BUCKET_NAME}/{blob_name}"
    print(f"  Uploaded to {gcs_uri}")
    return gcs_uri


def load_gcs_to_bq(gcs_uri, table_name):
    """
    Load JSON from GCS into BigQuery using a load job.
    - autodetect=True means BigQuery infers the schema automatically
    - WRITE_TRUNCATE replaces the table each run (full refresh pattern)
    - This runs entirely inside Google's network — fast and scalable
    """
    table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )

    print(f"  Load job started: {load_job.job_id}")
    load_job.result()  # Wait for job to complete

    table = bq_client.get_table(table_id)
    print(f"  Loaded {table.num_rows} rows into {table_id}")


def convert_to_newline_delimited_json(local_filepath):
    """
    BigQuery expects newline-delimited JSON (one record per line)
    rather than a JSON array. This converts our raw files.
    """
    with open(local_filepath) as f:
        data = json.load(f)

    ndj_filepath = local_filepath.replace(".json", "_ndj.json")
    with open(ndj_filepath, "w") as f:
        for record in data:
            f.write(json.dumps(record) + "\n")

    return ndj_filepath


def get_latest(pattern):
    """Find the most recent raw file for a given data type"""
    files = glob.glob(f"raw/{pattern}_*.json")
    files = [f for f in files if "_ndj" not in f]  # exclude converted files
    return max(files) if files else None


if __name__ == "__main__":
    print("Starting WHOOP data load to GCP...")
    print("="*40)

    data_types = {
        "recovery": "recovery",
        "sleep": "sleep",
        "cycles": "cycles",
        "workouts": "workouts",
    }

    for data_type, table_name in data_types.items():
        print(f"\nProcessing {data_type}...")

        filepath = get_latest(data_type)
        if not filepath:
            print(f"  No file found for {data_type}, skipping")
            continue

        print(f"  Converting to newline-delimited JSON...")
        ndj_filepath = convert_to_newline_delimited_json(filepath)

        print(f"  Uploading to GCS...")
        gcs_uri = upload_to_gcs(ndj_filepath, data_type)

        print(f"  Loading from GCS into BigQuery...")
        load_gcs_to_bq(gcs_uri, table_name)

    print("\n" + "="*40)
    print("Load complete!")