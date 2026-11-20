# Databricks notebook source
# INDIA URBAN MOBILITY PLATFORM

# BRONZE LAYER - DATA INGESTION

## Objective
Ingest raw Hyderabad Metro GTFS datasets into Databricks as Delta tables.

## Source
Unity Catalog Volume:
`/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/`

## Target
Catalog: anvesh
Schema: default
Format: Delta

## Initial Dataset
stops.txt

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

source_path = "/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/stops.txt"

stops_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(source_path)
)

print("Source record count:", stops_df.count())

stops_df.printSchema()

display(stops_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import (
    current_timestamp,
    lit
)

bronze_stops_df = (
    stops_df
    .withColumn("_source_file", lit("stops.txt"))
    .withColumn("_source_system", lit("HMRL_GTFS"))
    .withColumn("_ingestion_timestamp", current_timestamp())
)

display(bronze_stops_df.limit(5))

print("Bronze record count:", bronze_stops_df.count())

bronze_stops_df.printSchema()

# COMMAND ----------

target_table = "anvesh.default.bronze_stops"

(
    bronze_stops_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

print(f"Successfully created table: {target_table}")

# COMMAND ----------

bronze_check_df = spark.table("anvesh.default.bronze_stops")

print("Bronze table count:", bronze_check_df.count())

display(bronze_check_df.limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL anvesh.default.bronze_stops;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

def ingest_gtfs_file(file_name, table_name):

    source_path = (
        f"/Volumes/anvesh/default/test/"
        f"bronze/gtfs/hyderabad_metro/{file_name}"
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(source_path)
    )

    bronze_df = (
        df
        .withColumn("_source_file", lit(file_name))
        .withColumn("_source_system", lit("HMRL_GTFS"))
        .withColumn("_ingestion_timestamp", current_timestamp())
    )

    target_table = f"anvesh.default.{table_name}"

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

    print(f"File: {file_name}")
    print(f"Target: {target_table}")
    print(f"Records: {spark.table(target_table).count()}")
    print("Status: SUCCESS")

# COMMAND ----------

ingest_gtfs_file("routes.txt", "bronze_routes")

# COMMAND ----------

gtfs_datasets = {
    "agency.txt": "bronze_agency",
    "calendar.txt": "bronze_calendar",
    "trips.txt": "bronze_trips",
    "stop_times.txt": "bronze_stop_times",
    "shapes.txt": "bronze_shapes",
    "fare_attributes.txt": "bronze_fare_attributes",
    "fare_rules.txt": "bronze_fare_rules",
    "feed_info.txt": "bronze_feed_info"
}

print(gtfs_datasets)
print("Total remaining datasets:", len(gtfs_datasets))

# COMMAND ----------

for file_name, table_name in gtfs_datasets.items():

    ingest_gtfs_file(file_name, table_name)

# COMMAND ----------

bronze_tables = [
    "bronze_stops",
    "bronze_routes",
    "bronze_agency",
    "bronze_calendar",
    "bronze_trips",
    "bronze_stop_times",
    "bronze_shapes",
    "bronze_fare_attributes",
    "bronze_fare_rules",
    "bronze_feed_info"
]

results = []

for table_name in bronze_tables:

    df = spark.table(f"anvesh.default.{table_name}")

    record_count = df.count()

    results.append(
        (table_name, record_count, "PASS")
    )

for table_name, count, status in results:
    print(f"{table_name}: {count} | {status}")

print("\nTotal tables:", len(results))
print("Total records:", sum(row[1] for row in results))

# COMMAND ----------

import uuid
from datetime import datetime, timezone

ingestion_run_id = str(uuid.uuid4())

ingestion_start_time = datetime.now(timezone.utc)

print("Ingestion Run ID:", ingestion_run_id)
print("Ingestion Start Time:", ingestion_start_time)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)

audit_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("source_file", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("record_count", LongType(), True),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True),
    StructField("started_at", TimestampType(), False),
    StructField("completed_at", TimestampType(), True)
])

audit_records = []

print("Audit schema created successfully")
print("Run ID:", ingestion_run_id)

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql.functions import lit, current_timestamp

def ingest_gtfs_file(file_name, table_name):

    started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    target_table = f"anvesh.default.{table_name}"

    record_count = None
    error_message = None
    status = "FAILED"

    try:

        source_path = (
            f"/Volumes/anvesh/default/test/"
            f"bronze/gtfs/hyderabad_metro/{file_name}"
        )

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(source_path)
        )

        bronze_df = (
            df
            .withColumn("_source_file", lit(file_name))
            .withColumn("_source_system", lit("HMRL_GTFS"))
            .withColumn("_ingestion_run_id", lit(ingestion_run_id))
            .withColumn("_ingestion_timestamp", current_timestamp())
        )

        (
            bronze_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        record_count = spark.table(target_table).count()

        status = "SUCCESS"

    except Exception as e:

        error_message = str(e)[:4000]

    completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

    audit_records.append((
        ingestion_run_id,
        "hyderabad_metro_gtfs_bronze",
        file_name,
        target_table,
        record_count,
        status,
        error_message,
        started_at,
        completed_at
    ))

    print(f"File: {file_name}")
    print(f"Target: {target_table}")
    print(f"Records: {record_count}")
    print(f"Status: {status}")

    if error_message:
        print("Error:", error_message)

    return status

# COMMAND ----------

ingest_gtfs_file("routes.txt", "bronze_routes")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE anvesh.default.bronze_routes
# MAGIC ADD COLUMNS (
# MAGIC     _ingestion_run_id STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE anvesh.default.bronze_routes;

# COMMAND ----------

ingest_gtfs_file("routes.txt", "bronze_routes")

# COMMAND ----------

from pyspark.sql.functions import col

routes_check = spark.table("anvesh.default.bronze_routes")

display(
    routes_check.select(
        "route_id",
        "_source_file",
        "_source_system",
        "_ingestion_run_id",
        "_ingestion_timestamp"
    )
)

# COMMAND ----------

print(
    "Records with current run ID:",
    routes_check.filter(
        col("_ingestion_run_id") == ingestion_run_id
    ).count()
)

# COMMAND ----------

audit_df = spark.createDataFrame(
    audit_records,
    schema=audit_schema
)

display(audit_df)

# COMMAND ----------

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.bronze_ingestion_audit")
)

print("Audit records saved successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     source_file,
# MAGIC     target_table,
# MAGIC     record_count,
# MAGIC     status,
# MAGIC     error_message,
# MAGIC     started_at,
# MAGIC     completed_at
# MAGIC FROM anvesh.default.bronze_ingestion_audit
# MAGIC ORDER BY started_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE anvesh.default.bronze_ingestion_audit;

# COMMAND ----------

def run_bronze_pipeline(datasets):

    pipeline_started_at = datetime.now(timezone.utc)

    print("Starting Bronze Pipeline")
    print("Run ID:", ingestion_run_id)
    print("Total datasets:", len(datasets))

    for file_name, table_name in datasets.items():

        try:
            ingest_gtfs_file(file_name, table_name)

        except Exception as e:
            print(f"Unexpected failure for {file_name}: {str(e)}")

    print("\nBronze Pipeline Execution Completed")

    print("Total attempts:", len(datasets))
    print(
        "Successful:",
        sum(1 for record in audit_records
            if record[0] == ingestion_run_id
            and record[5] == "SUCCESS")
    )
    print(
        "Failed:",
        sum(1 for record in audit_records
            if record[0] == ingestion_run_id
            and record[5] == "FAILED")
    )

# COMMAND ----------

import uuid
from datetime import datetime, timezone

ingestion_run_id = str(uuid.uuid4())

ingestion_start_time = datetime.now(timezone.utc)

audit_records = []

print("New Pipeline Run ID:", ingestion_run_id)
print("Pipeline Start Time:", ingestion_start_time)
print("Audit records initialized:", len(audit_records))

# COMMAND ----------

all_gtfs_datasets = {
    "stops.txt": "bronze_stops",
    "routes.txt": "bronze_routes",
    "agency.txt": "bronze_agency",
    "calendar.txt": "bronze_calendar",
    "trips.txt": "bronze_trips",
    "stop_times.txt": "bronze_stop_times",
    "shapes.txt": "bronze_shapes",
    "fare_attributes.txt": "bronze_fare_attributes",
    "fare_rules.txt": "bronze_fare_rules",
    "feed_info.txt": "bronze_feed_info"
}

run_bronze_pipeline(all_gtfs_datasets)

# COMMAND ----------

tables_to_update = [
    "bronze_stops",
    "bronze_agency",
    "bronze_calendar",
    "bronze_trips",
    "bronze_stop_times",
    "bronze_shapes",
    "bronze_fare_attributes",
    "bronze_fare_rules",
    "bronze_feed_info"
]

for table_name in tables_to_update:

    spark.sql(f"""
        ALTER TABLE anvesh.default.{table_name}
        ADD COLUMNS (_ingestion_run_id STRING)
    """)

    print(f"Schema updated: {table_name}")

# COMMAND ----------

bronze_tables = [
    "bronze_stops",
    "bronze_routes",
    "bronze_agency",
    "bronze_calendar",
    "bronze_trips",
    "bronze_stop_times",
    "bronze_shapes",
    "bronze_fare_attributes",
    "bronze_fare_rules",
    "bronze_feed_info"
]

for table_name in bronze_tables:

    df = spark.table(f"anvesh.default.{table_name}")

    has_run_id = "_ingestion_run_id" in df.columns

    print(
        f"{table_name}: "
        f"{'PASS' if has_run_id else 'FAIL'}"
    )

# COMMAND ----------

from collections import Counter

print("Current Run ID:", ingestion_run_id)
print("Total audit records in memory:", len(audit_records))

current_run_records = [
    record for record in audit_records
    if record[0] == ingestion_run_id
]

print("Records for current run:", len(current_run_records))
print("Status counts:", Counter(record[5] for record in current_run_records))

for record in current_run_records:
    print(record[2], "->", record[5])

# COMMAND ----------

if audit_records:
    previous_audit_df = spark.createDataFrame(
        audit_records,
        schema=audit_schema
    )

    (
        previous_audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("anvesh.default.bronze_ingestion_audit")
    )

    print("Previous execution history saved.")
    print("Records saved:", len(audit_records))
else:
    print("No audit records to save.")

# COMMAND ----------

import uuid
from datetime import datetime, timezone

ingestion_run_id = str(uuid.uuid4())

ingestion_start_time = datetime.now(timezone.utc)

audit_records = []

print("New Bronze Pipeline Run")
print("------------------------")
print("Run ID:", ingestion_run_id)
print("Started At:", ingestion_start_time)
print("Audit Records:", len(audit_records))

# COMMAND ----------

run_bronze_pipeline(all_gtfs_datasets)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     source_file,
# MAGIC     target_table,
# MAGIC     record_count,
# MAGIC     status,
# MAGIC     started_at,
# MAGIC     completed_at
# MAGIC FROM anvesh.default.bronze_ingestion_audit
# MAGIC WHERE run_id = 'c1af71c4-a397-4381-95db-8274de795a31'
# MAGIC ORDER BY source_file;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     status,
# MAGIC     COUNT(*) AS total_files,
# MAGIC     SUM(record_count) AS total_records
# MAGIC FROM anvesh.default.bronze_ingestion_audit
# MAGIC WHERE run_id = 'c1af71c4-a397-4381-95db-8274de795a31'
# MAGIC GROUP BY status;

# COMMAND ----------

print("Run ID:", ingestion_run_id)
print("Audit records:", len(audit_records))

audit_df = spark.createDataFrame(
    audit_records,
    schema=audit_schema
)

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.bronze_ingestion_audit")
)

print("Latest run audit records saved successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     source_file,
# MAGIC     target_table,
# MAGIC     record_count,
# MAGIC     status
# MAGIC FROM anvesh.default.bronze_ingestion_audit
# MAGIC WHERE run_id = 'c1af71c4-a397-4381-95db-8274de795a31'
# MAGIC ORDER BY source_file;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     status,
# MAGIC     COUNT(*) AS total_files,
# MAGIC     SUM(record_count) AS total_records
# MAGIC FROM anvesh.default.bronze_ingestion_audit
# MAGIC WHERE run_id = 'c1af71c4-a397-4381-95db-8274de795a31'
# MAGIC GROUP BY status;

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct

bronze_tables = [
    "bronze_stops",
    "bronze_routes",
    "bronze_agency",
    "bronze_calendar",
    "bronze_trips",
    "bronze_stop_times",
    "bronze_shapes",
    "bronze_fare_attributes",
    "bronze_fare_rules",
    "bronze_feed_info"
]

for table_name in bronze_tables:

    df = spark.table(f"anvesh.default.{table_name}")

    print(f"\nTable: {table_name}")

    df.select(
        "_source_file",
        "_source_system",
        "_ingestion_run_id",
        "_ingestion_timestamp"
    ).show(2, truncate=False)

    print(
        "Current run records:",
        df.filter(
            col("_ingestion_run_id") == ingestion_run_id
        ).count()
    )

# COMMAND ----------

