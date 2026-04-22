"""
Airflow DAG: load_csvs_to_snowflake
------------------------------------
Runs every 10 minutes. Scans the Spark output directories for new CSV files
and bulk loads them into Snowflake raw_events and raw_alerts tables.

To use:
1. Copy this file to ~/airflow25/dags/
2. Update OUTPUT_DIR to the actual path where Spark writes CSVs
3. Make sure rsa_key.p8 is accessible at the path set in RSA_KEY_PATH
4. Set your Snowflake credentials in the variables at the top
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json

# ---- Config — update these before running ----
OUTPUT_DIR = "/home/compute/d.linus/data-engineering-anomaly-detector/output"  # UPDATE THIS
RSA_KEY_PATH = "/home/compute/d.linus/rsa_key.p8"  # UPDATE THIS

SNOWFLAKE_USER = "FALCON"
SNOWFLAKE_ACCOUNT = "SFEDU02-UNB02139"
SNOWFLAKE_DATABASE = "FALCON_DB"
SNOWFLAKE_SCHEMA = "FALCON_SCHEMA"
SNOWFLAKE_WAREHOUSE = "FALCON_WH"
SNOWFLAKE_ROLE = "TRAINING_ROLE"

# File to track which CSVs have already been loaded so we don't duplicate
LOADED_FILES_TRACKER = "/home/compute/d.linus/airflow_loaded_files.json"  # UPDATE THIS

# Columns expected in each table (event_dt added from folder name)
RAW_EVENTS_COLS = [
    "id", "name", "symbol", "price", "event_ts",
    "volume_24h", "volume_change_24h", "percent_change_1h",
    "percent_change_24h", "processing_time", "event_id"
]

RAW_ALERTS_COLS = [
    "id", "event_id", "event_ts", "curr_price", "past_prices",
    "past_timestamps", "mean", "std", "z_score", "alert_id"
]


def get_loaded_files():
    """Load the set of already-processed file paths from tracker file."""
    if os.path.exists(LOADED_FILES_TRACKER):
        with open(LOADED_FILES_TRACKER, "r") as f:
            return set(json.load(f))
    return set()


def save_loaded_files(loaded):
    """Save the updated set of processed file paths to tracker file."""
    with open(LOADED_FILES_TRACKER, "w") as f:
        json.dump(list(loaded), f)


def get_snowflake_connection():
    """Create and return a Snowflake connection using private key auth."""
    import snowflake.connector
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    with open(RSA_KEY_PATH, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )

    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        account=SNOWFLAKE_ACCOUNT,
        private_key=private_key_bytes,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
        role=SNOWFLAKE_ROLE
    )


def collect_new_csvs(folder_name, loaded_files):
    """
    Walk partitioned output folder and return list of
    (csv_path, event_dt) tuples for files not yet loaded.
    """
    folder_path = os.path.join(OUTPUT_DIR, folder_name)
    results = []

    if not os.path.exists(folder_path):
        print(f"Folder not found: {folder_path}")
        return results

    for partition in sorted(os.listdir(folder_path)):
        if not partition.startswith("event_dt="):
            continue
        event_dt = partition.replace("event_dt=", "")
        partition_path = os.path.join(folder_path, partition)
        for filename in os.listdir(partition_path):
            if not filename.endswith(".csv") or filename.startswith("."):
                continue
            csv_path = os.path.join(partition_path, filename)
            if csv_path not in loaded_files:
                results.append((csv_path, event_dt))

    return results


def load_csvs_to_snowflake(**context):
    """
    Main task: find new CSV files and bulk load them into Snowflake.
    Tracks which files have been loaded to avoid duplicates.
    """
    import pandas as pd

    loaded_files = get_loaded_files()
    newly_loaded = set()

    conn = get_snowflake_connection()
    cursor = conn.cursor()
    print("Connected to Snowflake!")

    def load_folder(folder_name, table_name, col_names):
        new_files = collect_new_csvs(folder_name, loaded_files)

        if not new_files:
            print(f"No new files found for {table_name}")
            return

        print(f"Found {len(new_files)} new file(s) for {table_name}")

        dfs = []
        for csv_path, event_dt in new_files:
            df = pd.read_csv(csv_path)
            if df.empty:
                continue
            if "is_anomaly" in df.columns:
                df = df.drop(columns=["is_anomaly"])
            df = df[[c for c in col_names if c in df.columns]]
            df["event_dt"] = event_dt
            dfs.append((df, csv_path))
            print(f"  Read {len(df)} rows from {csv_path}")

        if not dfs:
            return

        combined = pd.concat([d for d, _ in dfs], ignore_index=True)
        print(f"  Total: {len(combined)} rows to load into {table_name}")

        # Write combined df to temp CSV and bulk load via Snowflake stage
        tmp_path = f"/tmp/{table_name}_airflow_load.csv"
        combined.to_csv(tmp_path, index=False, header=False)

        col_list = ", ".join(combined.columns)

        cursor.execute(f"REMOVE @%{table_name}")
        cursor.execute(f"PUT file://{tmp_path} @%{table_name} OVERWRITE=TRUE AUTO_COMPRESS=TRUE")
        cursor.execute(f"""
            COPY INTO {table_name} ({col_list})
            FROM @%{table_name}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('', 'None', 'nan')
                EMPTY_FIELD_AS_NULL = TRUE
            )
            PURGE = TRUE
        """)

        result = cursor.fetchone()
        print(f"  Loaded! Result: {result}")
        os.remove(tmp_path)

        # Mark these files as loaded
        for _, csv_path in dfs:
            newly_loaded.add(csv_path)

    try:
        load_folder("raw_output", "raw_events", RAW_EVENTS_COLS)
        load_folder("alerts", "raw_alerts", RAW_ALERTS_COLS)
        conn.commit()
        print("All new data loaded successfully!")

    except Exception as e:
        conn.rollback()
        raise e

    finally:
        cursor.close()
        conn.close()

    # Save updated tracker only after successful load
    save_loaded_files(loaded_files | newly_loaded)
    print(f"Tracker updated — {len(newly_loaded)} new files recorded")


# ---- DAG definition ----
default_args = {
    "owner": "d.linus",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="load_csvs_to_snowflake",
    default_args=default_args,
    description="Loads new Spark output CSVs into Snowflake raw_events and raw_alerts tables",
    schedule_interval="*/10 * * * *",  # every 10 minutes
    start_date=datetime(2026, 4, 21),
    catchup=False,
    tags=["crypto", "anomaly-detection", "snowflake"],
) as dag:

    load_task = PythonOperator(
        task_id="load_new_csvs",
        python_callable=load_csvs_to_snowflake,
        provide_context=True,
    )