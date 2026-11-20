# Databricks notebook source
# INDIA URBAN MOBILITY PLATFORM

# BRONZE LAYER - DATA QUALITY VALIDATION

## Objective
Validate the quality and reliability of Bronze Delta tables.

## Validation Scope
1. Table Existence Validation
2. Required Column Validation
3. NULL Value Validation
4. Duplicate Record Validation
5. Ingestion Metadata Validation
6. Source-to-Bronze Record Count Validation

## Catalog
anvesh.default

## Source System
HMRL_GTFS

# COMMAND ----------

# ==========================================
# BRONZE TABLE EXISTENCE VALIDATION
# ==========================================

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

validation_results = []

for table_name in bronze_tables:

    full_table_name = f"anvesh.default.{table_name}"

    if spark.catalog.tableExists(full_table_name):
        status = "PASS"
    else:
        status = "FAIL"

    validation_results.append(
        (table_name, status)
    )

    print(f"{table_name}: {status}")

print("\nTable Existence Validation Completed")

# COMMAND ----------

# ==========================================
# BRONZE REQUIRED COLUMN VALIDATION
# ==========================================

required_columns = {
    "bronze_stops": [
        "stop_id", "stop_name", "stop_lat", "stop_lon"
    ],
    "bronze_routes": [
        "route_id", "route_short_name"
    ],
    "bronze_agency": [
        "agency_id", "agency_name"
    ],
    "bronze_calendar": [
        "service_id", "start_date", "end_date"
    ],
    "bronze_trips": [
        "trip_id", "route_id", "service_id"
    ],
    "bronze_stop_times": [
        "trip_id", "stop_id", "stop_sequence"
    ],
    "bronze_shapes": [
        "shape_id", "shape_pt_lat", "shape_pt_lon"
    ],
    "bronze_fare_attributes": [
        "fare_id", "price", "currency_type"
    ],
    "bronze_fare_rules": [
        "fare_id", "origin_id", "destination_id"
    ],
    "bronze_feed_info": [
        "feed_publisher_name", "feed_lang"
    ]
}

metadata_columns = [
    "_source_file",
    "_source_system",
    "_ingestion_run_id",
    "_ingestion_timestamp"
]

for table_name, expected_columns in required_columns.items():

    df = spark.table(f"anvesh.default.{table_name}")

    actual_columns = set(df.columns)

    missing_columns = [
        column
        for column in expected_columns + metadata_columns
        if column not in actual_columns
    ]

    if len(missing_columns) == 0:
        status = "PASS"
    else:
        status = "FAIL"

    print(f"{table_name}: {status}")

    if missing_columns:
        print("Missing columns:", missing_columns)

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, when

import builtins

null_validation_results = []

for table_name, columns in mandatory_columns.items():

    df = spark.table(f"anvesh.default.{table_name}")

    null_counts = df.select([
        spark_sum(
            when(col(column).isNull(), 1).otherwise(0)
        ).alias(column)
        for column in columns
    ]).first().asDict()

    total_nulls = builtins.sum(
        value or 0 for value in null_counts.values()
    )

    status = "PASS" if total_nulls == 0 else "FAIL"

    null_validation_results.append(
        (table_name, total_nulls, status)
    )

    print(
        f"{table_name}: {status} | "
        f"Total NULL values: {total_nulls}"
    )

print("\nNULL Validation Completed")

# COMMAND ----------

# ==========================================
# BRONZE DUPLICATE RECORD VALIDATION
# ==========================================

primary_keys = {
    "bronze_stops": ["stop_id"],
    "bronze_routes": ["route_id"],
    "bronze_agency": ["agency_id"],
    "bronze_calendar": ["service_id"],
    "bronze_trips": ["trip_id"],
    "bronze_stop_times": ["trip_id", "stop_sequence"],
    "bronze_shapes": ["shape_id", "shape_pt_sequence"],
    "bronze_fare_attributes": ["fare_id"],
    "bronze_fare_rules": [
        "fare_id", "origin_id", "destination_id"
    ],
    "bronze_feed_info": [
        "feed_publisher_name", "feed_lang"
    ]
}

duplicate_validation_results = []

for table_name, key_columns in primary_keys.items():

    df = spark.table(f"anvesh.default.{table_name}")

    total_records = df.count()

    distinct_records = (
        df.select(*key_columns)
        .distinct()
        .count()
    )

    duplicate_count = total_records - distinct_records

    status = "PASS" if duplicate_count == 0 else "FAIL"

    duplicate_validation_results.append(
        (table_name, duplicate_count, status)
    )

    print(
        f"{table_name}: {status} | "
        f"Duplicate records: {duplicate_count}"
    )

print("\nDuplicate Validation Completed")

# COMMAND ----------

ingestion_run_id = "c1af71c4-a397-4381-95db-8274de795a31"

print("Validating ingestion run:", ingestion_run_id)

# COMMAND ----------

# ==========================================
# BRONZE INGESTION METADATA VALIDATION
# ==========================================

from pyspark.sql.functions import col, count, when

metadata_columns = [
    "_source_file",
    "_source_system",
    "_ingestion_run_id",
    "_ingestion_timestamp"
]

metadata_validation_results = []

for table_name in bronze_tables:

    df = spark.table(f"anvesh.default.{table_name}")

    total_records = df.count()

    null_condition = None

    for column in metadata_columns:
        condition = col(column).isNull()

        null_condition = (
            condition if null_condition is None
            else null_condition | condition
        )

    invalid_records = df.filter(null_condition).count()

    wrong_run_records = df.filter(
        col("_ingestion_run_id") != ingestion_run_id
    ).count()

    status = (
        "PASS"
        if invalid_records == 0 and wrong_run_records == 0
        else "FAIL"
    )

    metadata_validation_results.append(
        (table_name, total_records, invalid_records,
         wrong_run_records, status)
    )

    print(
        f"{table_name}: {status} | "
        f"Total: {total_records} | "
        f"Invalid metadata: {invalid_records} | "
        f"Wrong run ID: {wrong_run_records}"
    )

print("\nMetadata Validation Completed")

# COMMAND ----------

# ==========================================
# SOURCE-TO-BRONZE RECORD COUNT VALIDATION
# ==========================================

expected_counts = {
    "bronze_stops": 705,
    "bronze_routes": 3,
    "bronze_agency": 1,
    "bronze_calendar": 3,
    "bronze_trips": 2820,
    "bronze_stop_times": 61236,
    "bronze_shapes": 2450,
    "bronze_fare_attributes": 10,
    "bronze_fare_rules": 3249,
    "bronze_feed_info": 1
}

count_validation_results = []

for table_name, expected_count in expected_counts.items():

    df = spark.table(f"anvesh.default.{table_name}")

    actual_count = df.count()

    difference = actual_count - expected_count

    status = "PASS" if difference == 0 else "FAIL"

    count_validation_results.append(
        (table_name, expected_count, actual_count, difference, status)
    )

    print(
        f"{table_name}: {status} | "
        f"Expected: {expected_count} | "
        f"Actual: {actual_count} | "
        f"Difference: {difference}"
    )

print("\nSource-to-Bronze Validation Completed")

# COMMAND ----------

# ==========================================
# CONSOLIDATED BRONZE QUALITY REPORT
# ==========================================

report_records = []

# 1. Table existence
for table_name, status in validation_results:
    report_records.append((
        table_name,
        "TABLE_EXISTENCE",
        0,
        0 if status == "PASS" else 1,
        status
    ))

# 2. Required column validation
for table_name, expected_cols in required_columns.items():

    actual_cols = set(
        spark.table(f"anvesh.default.{table_name}").columns
    )

    missing = [
        c for c in expected_cols + metadata_columns
        if c not in actual_cols
    ]

    report_records.append((
        table_name,
        "REQUIRED_COLUMNS",
        spark.table(f"anvesh.default.{table_name}").count(),
        len(missing),
        "PASS" if len(missing) == 0 else "FAIL"
    ))

# 3. NULL validation
for table_name, null_count, status in null_validation_results:
    report_records.append((
        table_name,
        "NULL_VALIDATION",
        spark.table(f"anvesh.default.{table_name}").count(),
        null_count,
        status
    ))

# 4. Duplicate validation
for table_name, duplicate_count, status in duplicate_validation_results:
    report_records.append((
        table_name,
        "DUPLICATE_VALIDATION",
        spark.table(f"anvesh.default.{table_name}").count(),
        duplicate_count,
        status
    ))

# 5. Metadata validation
for table_name, total, invalid, wrong_run, status in metadata_validation_results:
    report_records.append((
        table_name,
        "METADATA_VALIDATION",
        total,
        invalid + wrong_run,
        status
    ))

# 6. Source-to-Bronze reconciliation
for table_name, expected, actual, difference, status in count_validation_results:
    report_records.append((
        table_name,
        "COUNT_RECONCILIATION",
        actual,
        abs(difference),
        status
    ))

print("Total validation report rows:", len(report_records))

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

report_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("validation_type", StringType(), False),
    StructField("total_records", LongType(), False),
    StructField("failed_records", LongType(), False),
    StructField("status", StringType(), False)
])

bronze_quality_df = spark.createDataFrame(
    report_records,
    schema=report_schema
)

display(bronze_quality_df)

# COMMAND ----------

# Display validation summary

bronze_quality_df.groupBy(
    "validation_type",
    "status"
).count().orderBy(
    "validation_type",
    "status"
).show(truncate=False)

# COMMAND ----------

# Overall validation summary

bronze_quality_df.groupBy(
    "status"
).count().show()

# COMMAND ----------

# ==========================================
# SAVE BRONZE QUALITY REPORT
# ==========================================

target_table = "anvesh.default.bronze_quality_report"

(
    bronze_quality_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

print("Quality report saved successfully!")
print("Target table:", target_table)
print("Total report records:", spark.table(target_table).count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     validation_type,
# MAGIC     status,
# MAGIC     COUNT(*) AS total_checks,
# MAGIC     SUM(failed_records) AS total_failures
# MAGIC FROM anvesh.default.bronze_quality_report
# MAGIC GROUP BY validation_type, status
# MAGIC ORDER BY validation_type;

# COMMAND ----------

# ==========================================
# VALIDATION RUN INITIALIZATION
# ==========================================

import uuid
from datetime import datetime, timezone

# Unique validation execution ID
validation_run_id = str(uuid.uuid4())

# Bronze ingestion run being validated
source_ingestion_run_id = (
    "c1af71c4-a397-4381-95db-8274de795a31"
)

# Validation execution timestamp
validation_timestamp = datetime.now(timezone.utc)

print("Validation Run Initialized")
print("--------------------------------")
print("Validation Run ID:", validation_run_id)
print("Source Ingestion Run ID:", source_ingestion_run_id)
print("Validation Timestamp:", validation_timestamp)

# COMMAND ----------

# Add run information to every validation record

enriched_report_records = [
    (
        record[0],
        record[1],
        record[2],
        record[3],
        record[4],
        validation_run_id,
        source_ingestion_run_id,
        validation_timestamp.replace(tzinfo=None)
    )
    for record in report_records
]

print("Enriched report records:", len(enriched_report_records))
print("Validation Run ID:", validation_run_id)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)

enriched_report_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("validation_type", StringType(), False),
    StructField("total_records", LongType(), False),
    StructField("failed_records", LongType(), False),
    StructField("status", StringType(), False),
    StructField("validation_run_id", StringType(), False),
    StructField("source_ingestion_run_id", StringType(), False),
    StructField("validation_timestamp", TimestampType(), False)
])

print("Enriched report schema created successfully!")

# COMMAND ----------

# Create enriched Bronze quality DataFrame

enriched_quality_df = spark.createDataFrame(
    enriched_report_records,
    schema=enriched_report_schema
)

print("Enriched DataFrame created successfully!")

print("Total records:", enriched_quality_df.count())

enriched_quality_df.printSchema()

display(enriched_quality_df.limit(5))

# COMMAND ----------

# ==========================================
# SAVE HISTORICAL BRONZE QUALITY REPORT
# ==========================================

target_table = "anvesh.default.bronze_quality_history"

(
    enriched_quality_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(target_table)
)

print("Historical quality report saved!")
print("Target table:", target_table)

print(
    "Total historical records:",
    spark.table(target_table).count()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     validation_run_id,
# MAGIC     source_ingestion_run_id,
# MAGIC     validation_type,
# MAGIC     status,
# MAGIC     COUNT(*) AS total_checks,
# MAGIC     SUM(failed_records) AS total_failures
# MAGIC FROM anvesh.default.bronze_quality_history
# MAGIC GROUP BY
# MAGIC     validation_run_id,
# MAGIC     source_ingestion_run_id,
# MAGIC     validation_type,
# MAGIC     status
# MAGIC ORDER BY validation_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(DISTINCT validation_run_id) AS validation_runs,
# MAGIC     COUNT(DISTINCT source_ingestion_run_id) AS ingestion_runs
# MAGIC FROM anvesh.default.bronze_quality_history;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(DISTINCT validation_run_id) AS validation_runs,
# MAGIC     COUNT(DISTINCT source_ingestion_run_id) AS ingestion_runs
# MAGIC FROM anvesh.default.bronze_quality_history;

# COMMAND ----------

# ==========================================
# DATA QUALITY FAILURE HANDLING
# ==========================================

def check_validation_status(validation_df):

    failed_count = (
        validation_df
        .filter("status = 'FAIL'")
        .count()
    )

    if failed_count > 0:
        raise Exception(
            f"DATA QUALITY FAILED! "
            f"{failed_count} validation checks failed."
        )

    print("DATA QUALITY PASSED!")
    print("All validation checks are successful.")


check_validation_status(enriched_quality_df)

# COMMAND ----------

# Create a temporary failed validation record

from pyspark.sql.functions import lit

failed_test_df = (
    enriched_quality_df
    .limit(1)
    .withColumn("status", lit("FAIL"))
    .withColumn("failed_records", lit(5))
)

display(failed_test_df)

# COMMAND ----------

check_validation_status(failed_test_df)

# COMMAND ----------

print("Original report status:")

enriched_quality_df.groupBy("status").count().show()

print("Temporary test status:")

failed_test_df.groupBy("status").count().show()

# COMMAND ----------

check_validation_status(
    spark.table("anvesh.default.bronze_quality_history")
)

# COMMAND ----------

from pyspark.sql.functions import col, lit

failed_quality_report = (
    spark.table("anvesh.default.bronze_quality_history")
    .limit(1)
    .withColumn("status", lit("FAIL"))
    .withColumn("failed_records", lit(5))
)

display(failed_quality_report)

# COMMAND ----------

from pyspark.sql.functions import col

def bronze_quality_gate(quality_df):

    failed_count = (
        quality_df
        .filter(col("status") != "PASS")
        .count()
    )

    if failed_count > 0:
        raise Exception(
            f"Pipeline blocked! "
            f"{failed_count} validation checks failed."
        )

    print("BRONZE QUALITY GATE: PASSED")
    print("Downstream processing is allowed.")

# COMMAND ----------

from pyspark.sql.functions import col

def bronze_quality_gate(quality_df):

    failed_count = (
        quality_df
        .filter(col("status") != "PASS")
        .count()
    )

    if failed_count > 0:
        raise Exception(
            f"Pipeline blocked! "
            f"{failed_count} validation checks failed."
        )

    print("BRONZE QUALITY GATE: PASSED")
    print("Downstream processing is allowed.")

print("Pipeline gate function defined successfully")

# COMMAND ----------

