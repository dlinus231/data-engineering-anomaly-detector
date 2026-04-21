import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

load_dotenv()

# ---- Load private key ----
private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
with open(private_key_path, "rb") as key_file:
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

# ---- Snowflake connection ----
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    private_key=private_key_bytes,
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    role=os.getenv("SNOWFLAKE_ROLE")
)
cursor = conn.cursor()
print("Connected to Snowflake!")

OUTPUT_DIR = "./output"

# Raw events columns in the CSV (event_dt is NOT in the CSV, it's in the folder name)
RAW_EVENTS_COLS = [
    "id", "name", "symbol", "price", "event_ts",
    "volume_24h", "volume_change_24h", "percent_change_1h",
    "percent_change_24h", "processing_time", "event_id"
]

# Raw alerts columns in the CSV
RAW_ALERTS_COLS = [
    "id", "event_id", "event_ts", "curr_price", "past_prices",
    "past_timestamps", "mean", "std", "z_score", "alert_id"
]


def collect_csvs(folder_name):
    """
    Walk through partitioned folder and return list of (csv_path, event_dt) tuples.
    Skips hidden files and non-CSV files.
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
            results.append((os.path.join(partition_path, filename), event_dt))

    return results


def merge_csvs(folder_name, col_names):
    """
    Read all CSVs from a partitioned folder, add event_dt column,
    and return a single combined dataframe.
    """
    files = collect_csvs(folder_name)
    if not files:
        print(f"No files found in {folder_name}")
        return None

    dfs = []
    for csv_path, event_dt in files:
        df = pd.read_csv(csv_path)
        if df.empty:
            continue
        # Drop is_anomaly if present
        if "is_anomaly" in df.columns:
            df = df.drop(columns=["is_anomaly"])
        # Keep only expected columns in the right order
        df = df[[c for c in col_names if c in df.columns]]
        df["event_dt"] = event_dt
        dfs.append(df)
        print(f"  Read {len(df)} rows from {csv_path}")

    if not dfs:
        return None

    combined = pd.concat(dfs, ignore_index=True)
    print(f"  Total: {len(combined)} rows")
    return combined


def load_df_to_snowflake(df, table_name):
    """
    Write dataframe to a temp CSV, upload to Snowflake stage, then COPY INTO table.
    This is much faster than row-by-row inserts.
    """
    tmp_path = f"/tmp/{table_name}_load.csv"
    df.to_csv(tmp_path, index=False, header=False)

    # Clear any existing files from the stage first
    cursor.execute(f"REMOVE @%{table_name}")

    # Upload to Snowflake internal stage
    print(f"  Uploading to Snowflake stage...")
    cursor.execute(f"PUT file://{tmp_path} @%{table_name} OVERWRITE=TRUE AUTO_COMPRESS=TRUE")

    # Build explicit column list from dataframe so Snowflake knows the order
    col_list = ", ".join(df.columns)

    # COPY INTO table from stage
    print(f"  Running COPY INTO {table_name}...")
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
    print(f"  Done! Result: {result}")
    os.remove(tmp_path)


try:
    # ---- Load raw_events ----
    print("\nCollecting raw_events CSVs...")
    raw_df = merge_csvs("raw_output", RAW_EVENTS_COLS)
    if raw_df is not None:
        print(f"Loading {len(raw_df)} rows into raw_events...")
        load_df_to_snowflake(raw_df, "raw_events")

    # ---- Load raw_alerts ----
    print("\nCollecting raw_alerts CSVs...")
    alerts_df = merge_csvs("alerts", RAW_ALERTS_COLS)
    if alerts_df is not None:
        print(f"Loading {len(alerts_df)} rows into raw_alerts...")
        load_df_to_snowflake(alerts_df, "raw_alerts")

    conn.commit()
    print("\nAll data loaded successfully!")

except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
    raise

finally:
    cursor.close()
    conn.close()
    print("Snowflake connection closed.")