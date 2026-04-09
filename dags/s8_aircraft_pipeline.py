import gzip
import json
import os
from datetime import datetime

import boto3
import pandas as pd
import requests
import s3fs
from airflow import DAG
from airflow.decorators import task

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL")
SHARED_DATA_DIR = os.environ.get("SHARED_DATA_DIR", "/opt/airflow/shared")

TRACKING_BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"
AIRCRAFT_DB_URL = "https://opensky-network.org/datasets/metadata/aircraftDatabase.csv"
FUEL_RATES_URL = (
    "https://raw.githubusercontent.com/martsec/flight_co2_analysis"
    "/main/data/aircraft_type_fuel_consumption_rates.json"
)
MAX_TRACKING_FILES = 100


with DAG(
    dag_id="s8_aircraft_pipeline",
    start_date=datetime(2023, 11, 1),
    end_date=datetime(2023, 11, 2),
    schedule="@daily",
    catchup=True,
    default_args={"retries": 1},
) as dag:

    @task()
    def download_tracking_data():
        filenames = []
        for i in range(MAX_TRACKING_FILES):
            total_seconds = i * 5
            h = total_seconds // 3600
            m = (total_seconds % 3600) // 60
            s = total_seconds % 60
            filenames.append(f"{h:02d}{m:02d}{s:02d}Z.json.gz")

        all_records = []
        for filename in filenames:
            resp = requests.get(TRACKING_BASE_URL + filename, timeout=30)
            if resp.status_code != 200:
                continue
            try:
                data = json.loads(gzip.decompress(resp.content))
            except Exception:
                try:
                    data = resp.json()
                except Exception:
                    continue
            all_records.extend(data.get("aircraft", []))

        local_path = "/tmp/tracking.json"
        with open(local_path, "w") as f:
            json.dump(all_records, f)

        return local_path

    @task()
    def upload_tracking_to_s3(local_path, ds=None):
        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)
        s3_key = f"tracking/_created_date={ds}/dump.json"
        s3.upload_file(local_path, BRONZE_BUCKET, s3_key)
        return s3_key

    @task()
    def download_aircraft_db():
        response = requests.get(AIRCRAFT_DB_URL, stream=True, timeout=120)
        response.raise_for_status()

        local_path = "/tmp/aircraft_db.csv"
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=65536):
                f.write(chunk)

        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)
        s3.upload_file(local_path, BRONZE_BUCKET, "aircraft_db/aircraft_db.csv")
        return "aircraft_db/aircraft_db.csv"

    @task()
    def download_fuel_rates():
        response = requests.get(FUEL_RATES_URL, timeout=30)
        response.raise_for_status()
        data = response.json()

        local_path = "/tmp/fuel_rates.json"
        with open(local_path, "w") as f:
            json.dump(data, f)

        s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT)
        s3.upload_file(local_path, BRONZE_BUCKET, "fuel_rates/fuel_rates.json")

        # Write to shared folder so the API can read it
        os.makedirs(SHARED_DATA_DIR, exist_ok=True)
        shared_path = os.path.join(SHARED_DATA_DIR, "fuel_rates.json")
        with open(shared_path, "w") as f:
            json.dump(data, f)

        return "fuel_rates/fuel_rates.json"

    @task()
    def tracking_bronze_to_silver(tracking_key, aircraft_db_key, ds=None):
        fs = s3fs.S3FileSystem(
            endpoint_url=S3_ENDPOINT,
            key="minioadmin",
            secret="minioadmin",
        )

        with fs.open(f"{BRONZE_BUCKET}/{tracking_key}") as f:
            df = pd.read_json(f)

        if df.empty:
            return ""

        # Drop any existing 'type' column before renaming 't' to avoid duplicates
        if "type" in df.columns and "t" in df.columns:
            df = df.drop(columns=["type"])
        df = df.rename(columns={"hex": "icao", "r": "registration", "t": "type"})
        df["day"] = ds

        keep = ["icao", "registration", "type", "day", "lat", "lon", "alt_baro", "gs"]
        df = df[[c for c in keep if c in df.columns]]
        df = df[df["icao"].notna() & (df["icao"] != "")]

        # alt_baro can contain 'ground' string — coerce to numeric
        for col in ["alt_baro", "gs", "lat", "lon"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        with fs.open(f"{BRONZE_BUCKET}/{aircraft_db_key}") as f:
            df_db = pd.read_csv(f, dtype=str, low_memory=False)

        col_map = {}
        if "icao24" in df_db.columns:
            col_map["icao24"] = "icao"
        if "manufacturername" in df_db.columns:
            col_map["manufacturername"] = "manufacturer"
        if "model" in df_db.columns:
            col_map["model"] = "model"
        if "operator" in df_db.columns:
            col_map["operator"] = "owner"
        elif "owner" in df_db.columns:
            col_map["owner"] = "owner"

        df_db = df_db[list(col_map.keys())].rename(columns=col_map)
        if "icao" in df_db.columns:
            df_db = df_db.drop_duplicates("icao")

        df_enriched = df.merge(df_db, on="icao", how="left")

        silver_key = f"{SILVER_BUCKET}/tracking/_created_date={ds}/data.snappy.parquet"
        with fs.open(silver_key, "wb") as f:
            df_enriched.to_parquet(f, compression="snappy", index=False)

        # Write to shared folder so the API can read it
        local_prepared = os.path.join(SHARED_DATA_DIR, "prepared")
        os.makedirs(local_prepared, exist_ok=True)
        df_enriched.to_parquet(
            os.path.join(local_prepared, f"tracking_{ds}.snappy.parquet"),
            compression="snappy",
            index=False,
        )

        return f"s3://{silver_key}"

    tracking_local = download_tracking_data()
    tracking_key = upload_tracking_to_s3(tracking_local)
    aircraft_db_key = download_aircraft_db()
    download_fuel_rates()

    tracking_bronze_to_silver(tracking_key, aircraft_db_key)
