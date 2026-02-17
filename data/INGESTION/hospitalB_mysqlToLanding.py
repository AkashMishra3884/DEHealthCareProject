from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
import datetime
import json

# -----------------------------
# INIT CLIENTS
# -----------------------------
storage_client = storage.Client()
bq_client = bigquery.Client()

spark = SparkSession.builder.appName("HospitalA_CSV_To_Landing").getOrCreate()

# -----------------------------
# CONFIG
# -----------------------------
GCS_BUCKET = "healthcare13022025"

HOSPITAL_NAME = "hospital-a"

# SOURCE CSV PATH (already exists)
SOURCE_BASE_PATH = f"gs://{GCS_BUCKET}/data/hosp-a/"

# TARGET LANDING PATH (BigQuery reads from here)
LANDING_BASE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"

# BigQuery Audit
BQ_PROJECT = "lithe-land-345711"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_TEMP_BUCKET = GCS_BUCKET


# -----------------------------
# TABLE LIST
# -----------------------------
TABLES = [
    "departments",
    "providers",
    "patients",
    "encounters",
    "transactions"
]


# -----------------------------
# LOGGING
# -----------------------------
log_entries = []

def log_event(event_type, message, table=None):
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")


# -----------------------------
# ARCHIVE OLD JSON
# -----------------------------
def move_existing_json_to_archive(table):
    """
    Moves old JSON files from landing/<table>/ to landing/archive/<table>/yyyy/mm/dd/
    """
    prefix = f"landing/{HOSPITAL_NAME}/{table}/"
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=prefix))

    json_files = [b.name for b in blobs if b.name.endswith(".json")]

    if not json_files:
        log_event("INFO", f"No existing JSON files to archive for {table}", table=table)
        return

    now = datetime.datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    for file in json_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)
        archive_path = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"

        storage_client.bucket(GCS_BUCKET).copy_blob(
            source_blob,
            storage_client.bucket(GCS_BUCKET),
            archive_path
        )
        source_blob.delete()

    log_event("SUCCESS", f"Archived {len(json_files)} JSON files for {table}", table=table)


# -----------------------------
# WRITE AUDIT LOG
# -----------------------------
def write_audit_log(table, load_type, record_count, status):
    audit_df = spark.createDataFrame([
        ("hospital_a_db", table, load_type, int(record_count), datetime.datetime.now(), status)
    ], ["data_source", "tablename", "load_type", "record_count", "load_timestamp", "status"])

    (audit_df.write.format("bigquery")
        .option("table", BQ_AUDIT_TABLE)
        .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
        .mode("append")
        .save())


# -----------------------------
# MAIN INGESTION
# -----------------------------
def csv_to_landing_json(table):
    try:
        csv_path = f"{SOURCE_BASE_PATH}{table}.csv"
        output_path = f"{LANDING_BASE_PATH}{table}/"

        log_event("INFO", f"Reading CSV from: {csv_path}", table=table)

        df = spark.read.option("header", "true").csv(csv_path)

        record_count = df.count()
        log_event("INFO", f"Record count in CSV for {table}: {record_count}", table=table)

        # If CSV is empty, skip writing
        if record_count == 0:
            log_event("WARNING", f"No data in CSV for {table}. Skipping write.", table=table)
            write_audit_log(table, "Full", 0, "SUCCESS")
            return

        # Archive old JSON
        move_existing_json_to_archive(table)

        # Write JSON using Spark (correct way)
        log_event("INFO", f"Writing JSON to: {output_path}", table=table)

        df.write.mode("overwrite").json(output_path)

        log_event("SUCCESS", f"JSON written successfully for {table}", table=table)

        # Audit update
        write_audit_log(table, "Full", record_count, "SUCCESS")
        log_event("SUCCESS", f"Audit log updated for {table}", table=table)

    except Exception as e:
        log_event("ERROR", f"Failed for {table}: {str(e)}", table=table)
        try:
            write_audit_log(table, "Full", 0, "FAILED")
        except:
            pass


# -----------------------------
# RUN ALL TABLES
# -----------------------------
for t in TABLES:
    csv_to_landing_json(t)

log_event("SUCCESS", "Hospital A CSV → Landing JSON completed for all tables")
