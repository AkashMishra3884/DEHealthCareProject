from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
import datetime
import json

# Initialize GCS & BigQuery Clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Initialize Spark Session
spark = SparkSession.builder.appName("HospitalAMySQLToLanding").getOrCreate()

# Google Cloud Storage (GCS) Configuration
GCS_BUCKET = "healthcare13022025"
HOSPITAL_NAME = "hospital-a"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"

# BigQuery Configuration
BQ_PROJECT = "lithe-land-345711"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_BUCKET = GCS_BUCKET  # temporary bucket for bigquery connector

# MySQL Configuration
MYSQL_CONFIG = {
    "url": "jdbc:mysql://34.42.43.144:3306/hospital_a_db?useSSL=false&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "Aman@2025"
}

# ------------------------------------------------------------------------------------------------------------------#
# Logging Mechanism
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

def save_logs_to_gcs():
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"

    json_data = json.dumps(log_entries, indent=4)

    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)
    blob.upload_from_string(json_data, content_type="application/json")

    print(f"✅ Logs saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")

def save_logs_to_bigquery():
    if log_entries:
        log_df = spark.createDataFrame(log_entries)

        (log_df.write.format("bigquery")
            .option("table", BQ_LOG_TABLE)
            .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
            .mode("append")
            .save())

        print("✅ Logs stored in BigQuery")

# ------------------------------------------------------------------------------------------------------------------#
# Function to Move Existing Files to Archive (safe version)

def move_existing_files_to_archive(table):
    """
    Moves all existing JSON files in landing/table/ to archive/table/yyyy/mm/dd/
    NOTE: Spark writes files like part-00000.json so we cannot extract date from filename.
    """
    prefix = f"landing/{HOSPITAL_NAME}/{table}/"
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=prefix))

    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    if not existing_files:
        log_event("INFO", f"No existing JSON files for table {table}", table=table)
        return

    now = datetime.datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)

        archive_path = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        storage_client.bucket(GCS_BUCKET).copy_blob(
            source_blob,
            storage_client.bucket(GCS_BUCKET),
            destination_blob.name
        )
        source_blob.delete()

        log_event("INFO", f"Moved {file} to {archive_path}", table=table)

# ------------------------------------------------------------------------------------------------------------------#
# Function to Get Latest Watermark from BigQuery Audit Table

def get_latest_watermark(table_name):
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}'
          AND data_source = "hospital_a_db"
          AND status = "SUCCESS"
    """
    result = bq_client.query(query).result()
    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
    return "1900-01-01 00:00:00"

# ------------------------------------------------------------------------------------------------------------------#
# Function to Extract Data from MySQL and Save to GCS (FIXED JSON WRITE)

def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
        log_event("INFO", f"Latest watermark for {table}: {last_watermark}", table=table)

        if load_type.lower() == "full":
            query = f"(SELECT * FROM {table}) AS t"
        else:
            query = f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"

        df = (spark.read.format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", query)
                .load())

        log_event("SUCCESS", f"✅ Successfully extracted data from {table}", table=table)

        record_count = df.count()
        log_event("INFO", f"Record count extracted for {table}: {record_count}", table=table)

        # IMPORTANT FIX: if 0 rows, skip writing files (prevents schema empty issue)
        if record_count == 0:
            log_event("INFO", f"⚠️ No new data for {table}. Skipping JSON write.", table=table)

            audit_df = spark.createDataFrame([
                ("hospital_a_db", table, load_type, 0, datetime.datetime.now(), "SUCCESS")
            ], ["data_source", "tablename", "load_type", "record_count", "load_timestamp", "status"])

            (audit_df.write.format("bigquery")
                .option("table", BQ_AUDIT_TABLE)
                .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
                .mode("append")
                .save())

            log_event("SUCCESS", f"✅ Audit log updated for {table} (0 rows)", table=table)
            return

        # Move old JSON files to archive
        move_existing_files_to_archive(table)

        # Spark writes JSON directly to GCS (NO PANDAS)
        output_path = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/{table}/"
        df.write.mode("overwrite").json(output_path)

        log_event("SUCCESS", f"✅ JSON files written by Spark to {output_path}", table=table)

        # Insert Audit Entry
        audit_df = spark.createDataFrame([
            ("hospital_a_db", table, load_type, record_count, datetime.datetime.now(), "SUCCESS")
        ], ["data_source", "tablename", "load_type", "record_count", "load_timestamp", "status"])

        (audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
            .mode("append")
            .save())

        log_event("SUCCESS", f"✅ Audit log updated for {table}", table=table)

    except Exception as e:
        log_event("ERROR", f"❌ Error processing {table}: {str(e)}", table=table)

        # Insert failed audit entry (optional)
        try:
            audit_df = spark.createDataFrame([
                ("hospital_a_db", table, load_type, 0, datetime.datetime.now(), "FAILED")
            ], ["data_source", "tablename", "load_type", "record_count", "load_timestamp", "status"])

            (audit_df.write.format("bigquery")
                .option("table", BQ_AUDIT_TABLE)
                .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
                .mode("append")
                .save())
        except:
            pass

# ------------------------------------------------------------------------------------------------------------------#
# Function to Read Config File from GCS

def read_config_file():
    df = spark.read.csv(CONFIG_FILE_PATH, header=True)
    log_event("INFO", "✅ Successfully read the config file")
    return df

# ------------------------------------------------------------------------------------------------------------------#
# MAIN

config_df = read_config_file()

for row in config_df.collect():
    if row["is_active"] == '1' and row["datasource"] == "hospital_a_db":
        table = row["tablename"]
        load_type = row["load_type"]
        watermark = row["watermark_column"]

        extract_and_save_to_landing(table, load_type, watermark)

save_logs_to_gcs()
save_logs_to_bigquery()
