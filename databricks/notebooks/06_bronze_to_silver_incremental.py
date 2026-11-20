# Databricks notebook source
# INDIA URBAN MOBILITY PLATFORM

# BRONZE TO SILVER - INCREMENTAL PROCESSING

## Objective
Implement incremental data processing from Bronze Delta tables to Silver Delta tables.

## Processing Scope
1. Read Bronze Delta source
2. Validate incoming records
3. Identify new and changed records
4. Perform Delta MERGE
5. Maintain audit information
6. Validate target data

## Source
anvesh.default.bronze_routes

## Target
anvesh.default.demo_silver_routes

# COMMAND ----------

# Read Bronze routes table
bronze_routes_df = spark.table(
    "anvesh.default.bronze_routes"
)

# Display schema
bronze_routes_df.printSchema()

# Display record count
print("Bronze Routes Count:", bronze_routes_df.count())

# Display records
display(bronze_routes_df)

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    current_timestamp
)

# Read Bronze source
bronze_routes_df = spark.table(
    "anvesh.default.bronze_routes"
)

# Apply Silver transformations
silver_routes_df = (
    bronze_routes_df
    .select(
        trim(col("route_id")).alias("route_id"),
        trim(col("agency_id")).alias("agency_id"),
        trim(col("route_short_name")).alias("route_short_name"),
        trim(col("route_long_name")).alias("route_long_name"),
        col("route_type").cast("integer").alias("route_type"),
        trim(col("route_color")).alias("route_color"),
        trim(col("route_text_color")).alias("route_text_color"),
        col("route_sort_order").cast("integer").alias("route_sort_order"),
        col("_source_system").alias("_source_system"),
        col("_ingestion_run_id").alias("_ingestion_run_id"),
        current_timestamp().alias("processed_at")
    )
)

# Save as a separate Silver demo Delta table
(
    silver_routes_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("anvesh.default.demo_silver_routes")
)

print("Silver demo table created successfully!")
print(
    "Silver Routes Count:",
    spark.table("anvesh.default.demo_silver_routes").count()
)

display(spark.table("anvesh.default.demo_silver_routes"))

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

# Create simulated incremental source data
incoming_data = [
    Row(
        route_id="BLUE",
        agency_id="HMRL",
        route_short_name="C3_BLUE_UPDATED",
        route_long_name="Nagole - Raidurg - UPDATED",
        route_type=1,
        route_color="007ABB",
        route_text_color="FFFFFF",
        route_sort_order=1,
        _source_system="HMRL_GTFS",
        _ingestion_run_id="SIMULATED_RUN_002"
    ),

    Row(
        route_id="YELLOW",
        agency_id="HMRL",
        route_short_name="C4_YELLOW",
        route_long_name="Simulated Yellow Line",
        route_type=1,
        route_color="FFFF00",
        route_text_color="000000",
        route_sort_order=4,
        _source_system="HMRL_GTFS",
        _ingestion_run_id="SIMULATED_RUN_002"
    )
]

incoming_routes_df = spark.createDataFrame(incoming_data)

incoming_routes_df = incoming_routes_df.withColumn(
    "processed_at",
    current_timestamp()
)

print("Incoming Records:", incoming_routes_df.count())

display(incoming_routes_df)

# COMMAND ----------

from pyspark.sql.functions import col, count

# 1. Check duplicate route IDs
duplicate_routes = (
    incoming_routes_df
    .groupBy("route_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

duplicate_count = duplicate_routes.count()

print("Duplicate route IDs:", duplicate_count)

# 2. Check mandatory NULL values
null_count = (
    incoming_routes_df
    .filter(
        col("route_id").isNull() |
        col("route_short_name").isNull() |
        col("agency_id").isNull()
    )
    .count()
)

print("Records with mandatory NULL values:", null_count)

# 3. Validation result
if duplicate_count == 0 and null_count == 0:
    print("INCREMENTAL SOURCE VALIDATION: PASSED")
else:
    raise Exception("Incremental source validation failed!")

# COMMAND ----------

from delta.tables import DeltaTable

target_table = "anvesh.default.demo_silver_routes"

# Load target Delta table
delta_target = DeltaTable.forName(
    spark,
    target_table
)

# Define business key
merge_condition = """
target.route_id = source.route_id
"""

# Execute incremental MERGE
(
    delta_target.alias("target")
    .merge(
        incoming_routes_df.alias("source"),
        merge_condition
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("Incremental MERGE completed successfully!")

# COMMAND ----------

silver_result_df = spark.table(
    "anvesh.default.demo_silver_routes"
)

print("Total Silver Records:", silver_result_df.count())

display(
    silver_result_df.orderBy("route_id")
)

# COMMAND ----------

silver_result_df = spark.table(
    "anvesh.default.demo_silver_routes"
)

display(
    silver_result_df
    .select(
        "route_id",
        "route_short_name",
        "route_long_name",
        "_source_system",
        "_ingestion_run_id",
        "processed_at"
    )
    .orderBy("route_id")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS anvesh.default.silver_incremental_audit (
# MAGIC     pipeline_run_id STRING,
# MAGIC     pipeline_name STRING,
# MAGIC     source_table STRING,
# MAGIC     target_table STRING,
# MAGIC     source_record_count BIGINT,
# MAGIC     inserted_count BIGINT,
# MAGIC     updated_count BIGINT,
# MAGIC     target_record_count BIGINT,
# MAGIC     status STRING,
# MAGIC     started_at TIMESTAMP,
# MAGIC     completed_at TIMESTAMP,
# MAGIC     error_message STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE anvesh.default.silver_incremental_audit;

# COMMAND ----------

import uuid
from datetime import datetime, timezone

# Generate unique pipeline run ID
pipeline_run_id = str(uuid.uuid4())

# Pipeline details
pipeline_name = "bronze_to_silver_incremental_routes"

source_table = "anvesh.default.bronze_routes"
target_table = "anvesh.default.demo_silver_routes"

# Execution timestamps
started_at = datetime.now(timezone.utc).replace(tzinfo=None)

# Initialize metrics
source_record_count = incoming_routes_df.count()
inserted_count = 1
updated_count = 1
target_record_count = spark.table(target_table).count()

status = "SUCCESS"
error_message = None

print("Pipeline Run ID:", pipeline_run_id)
print("Source Records:", source_record_count)
print("Inserted Records:", inserted_count)
print("Updated Records:", updated_count)
print("Target Records:", target_record_count)
print("Status:", status)

# COMMAND ----------

from datetime import datetime, timezone

# Capture completion timestamp
completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

# Prepare audit record
audit_record = [(
    pipeline_run_id,
    pipeline_name,
    source_table,
    target_table,
    source_record_count,
    inserted_count,
    updated_count,
    target_record_count,
    status,
    started_at,
    completed_at,
    error_message
)]

# Create audit DataFrame
audit_df = spark.createDataFrame(
    audit_record,
    schema="""
        pipeline_run_id STRING,
        pipeline_name STRING,
        source_table STRING,
        target_table STRING,
        source_record_count LONG,
        inserted_count LONG,
        updated_count LONG,
        target_record_count LONG,
        status STRING,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        error_message STRING
    """
)

# Append audit record
(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.silver_incremental_audit")
)

print("Audit record saved successfully!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM anvesh.default.silver_incremental_audit
# MAGIC ORDER BY started_at DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY anvesh.default.demo_silver_routes;

# COMMAND ----------


# Read Delta transaction history
history_df = spark.sql("""
    DESCRIBE HISTORY anvesh.default.demo_silver_routes
""")

# Get the latest MERGE operation
latest_merge = (
    history_df
    .filter("operation = 'MERGE'")
    .orderBy("version", ascending=False)
    .first()
)

if latest_merge is None:
    raise Exception("No MERGE operation found!")

# Extract actual operation metrics
metrics = latest_merge["operationMetrics"]

source_count = int(metrics.get("numSourceRows", 0))
inserted_count = int(metrics.get("numTargetRowsInserted", 0))
updated_count = int(metrics.get("numTargetRowsUpdated", 0))
deleted_count = int(metrics.get("numTargetRowsDeleted", 0))

print("SOURCE RECORDS:", source_count)
print("INSERTED RECORDS:", inserted_count)
print("UPDATED RECORDS:", updated_count)
print("DELETED RECORDS:", deleted_count)
print("MERGE VERSION:", latest_merge["version"])
print("EXECUTION TIME:", metrics.get("executionTimeMs"), "ms")


# COMMAND ----------


from datetime import datetime, timezone

# Get the final target record count
target_record_count = spark.table(
    "anvesh.default.demo_silver_routes"
).count()

# Use actual Delta MERGE metrics
source_record_count = source_count
inserted_count = inserted_count
updated_count = updated_count

# Execution status
status = "SUCCESS"
error_message = None

completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

# Prepare audit record
audit_record = [(
    pipeline_run_id,
    pipeline_name,
    source_table,
    target_table,
    source_record_count,
    inserted_count,
    updated_count,
    target_record_count,
    status,
    started_at,
    completed_at,
    error_message
)]

audit_df = spark.createDataFrame(
    audit_record,
    schema="""
        pipeline_run_id STRING,
        pipeline_name STRING,
        source_table STRING,
        target_table STRING,
        source_record_count LONG,
        inserted_count LONG,
        updated_count LONG,
        target_record_count LONG,
        status STRING,
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        error_message STRING
    """
)

# Append audit record
(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.silver_incremental_audit")
)

print("Automatic MERGE metrics saved to audit table!")
print("Inserted:", inserted_count)
print("Updated:", updated_count)
print("Target:", target_record_count)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_run_id,
# MAGIC     pipeline_name,
# MAGIC     source_record_count,
# MAGIC     inserted_count,
# MAGIC     updated_count,
# MAGIC     target_record_count,
# MAGIC     status,
# MAGIC     error_message
# MAGIC FROM anvesh.default.silver_incremental_audit
# MAGIC ORDER BY completed_at DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_audit_records,
# MAGIC     COUNT(DISTINCT pipeline_run_id) AS unique_pipeline_runs
# MAGIC FROM anvesh.default.silver_incremental_audit;

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable

from pyspark.sql.functions import (
    col,
    trim,
    current_timestamp
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)

print("Pipeline dependencies imported successfully!")

# COMMAND ----------


def run_bronze_to_silver_incremental(
    source_table,
    target_table,
    key_columns,
    pipeline_name,
    audit_table
):

    # 1. Initialize pipeline tracking
    pipeline_run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    source_record_count = 0
    inserted_count = 0
    updated_count = 0
    target_record_count = 0

    status = "FAILED"
    error_message = None

    try:

        # 2. Read Bronze source
        bronze_df = spark.table(source_table)

        source_record_count = bronze_df.count()

        if source_record_count == 0:
            raise ValueError("Bronze source table is empty.")

        # 3. Apply Silver transformations
        silver_source_df = (
            bronze_df
            .select(
                trim(col("route_id")).alias("route_id"),
                trim(col("agency_id")).alias("agency_id"),
                trim(col("route_short_name")).alias("route_short_name"),
                trim(col("route_long_name")).alias("route_long_name"),
                col("route_type").cast("integer").alias("route_type"),
                trim(col("route_color")).alias("route_color"),
                trim(col("route_text_color")).alias("route_text_color"),
                col("route_sort_order").cast("integer").alias("route_sort_order"),
                col("_source_system").alias("_source_system"),
                col("_ingestion_run_id").alias("_ingestion_run_id"),
                current_timestamp().alias("processed_at")
            )
        )

        # 4. Validate mandatory fields
        invalid_records = (
            silver_source_df
            .filter(
                col("route_id").isNull() |
                (col("route_id") == "") |
                col("agency_id").isNull() |
                col("route_short_name").isNull()
            )
            .count()
        )

        if invalid_records > 0:
            raise ValueError(
                f"Source contains {invalid_records} invalid records."
            )

        # 5. Validate duplicate source keys
        source_duplicates = (
            silver_source_df
            .groupBy(*key_columns)
            .count()
            .filter("count > 1")
            .count()
        )

        if source_duplicates > 0:
            raise ValueError(
                f"Source contains {source_duplicates} duplicate key groups."
            )

        # 6. Capture existing target count
        target_df = spark.table(target_table)
        target_record_count = target_df.count()

        # 7. Validate target duplicate keys
        target_duplicates = (
            target_df
            .groupBy(*key_columns)
            .count()
            .filter("count > 1")
            .count()
        )

        if target_duplicates > 0:
            raise ValueError(
                f"Target contains {target_duplicates} duplicate key groups."
            )

        # 8. Execute Delta MERGE
        merge_condition = " AND ".join(
            [f"target.{key} = source.{key}" for key in key_columns]
        )

        delta_target = DeltaTable.forName(
            spark,
            target_table
        )

        (
            delta_target.alias("target")
            .merge(
                silver_source_df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        # 9. Extract actual Delta MERGE metrics
        latest_merge = (
            spark.sql(f"DESCRIBE HISTORY {target_table}")
            .filter("operation = 'MERGE'")
            .orderBy("version", ascending=False)
            .first()
        )

        if latest_merge is None:
            raise Exception("MERGE transaction not found in Delta history.")

        metrics = latest_merge["operationMetrics"]

        source_record_count = int(
            metrics.get("numSourceRows", source_record_count)
        )

        inserted_count = int(
            metrics.get("numTargetRowsInserted", 0)
        )

        updated_count = int(
            metrics.get("numTargetRowsUpdated", 0)
        )

        # 10. Capture final target count
        target_record_count = spark.table(target_table).count()

        status = "SUCCESS"

        print("Bronze-to-Silver MERGE completed successfully!")

    except Exception as e:

        error_message = str(e)[:4000]
        status = "FAILED"

        print("Incremental pipeline failed!")
        print("Error:", error_message)

    finally:

        # 11. Prepare one audit record
        completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

        audit_schema = StructType([
            StructField("pipeline_run_id", StringType(), True),
            StructField("pipeline_name", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("source_record_count", LongType(), True),
            StructField("inserted_count", LongType(), True),
            StructField("updated_count", LongType(), True),
            StructField("target_record_count", LongType(), True),
            StructField("status", StringType(), True),
            StructField("started_at", TimestampType(), True),
            StructField("completed_at", TimestampType(), True),
            StructField("error_message", StringType(), True)
        ])

        audit_record = [(
            pipeline_run_id,
            pipeline_name,
            source_table,
            target_table,
            source_record_count,
            inserted_count,
            updated_count,
            target_record_count,
            status,
            started_at,
            completed_at,
            error_message
        )]

        audit_df = spark.createDataFrame(
            audit_record,
            schema=audit_schema
        )

        (
            audit_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(audit_table)
        )

        print("Pipeline Run ID:", pipeline_run_id)
        print("Source Records:", source_record_count)
        print("Inserted Records:", inserted_count)
        print("Updated Records:", updated_count)
        print("Target Records:", target_record_count)
        print("Pipeline Status:", status)

    if status == "FAILED":
        raise RuntimeError(
            f"Pipeline failed. Run ID: {pipeline_run_id}. "
            f"Error: {error_message}"
        )

    return {
        "pipeline_run_id": pipeline_run_id,
        "status": status,
        "source_records": source_record_count,
        "inserted_records": inserted_count,
        "updated_records": updated_count,
        "target_records": target_record_count
    }


print("Reusable Bronze-to-Silver pipeline function defined!")


# COMMAND ----------


pipeline_result = run_bronze_to_silver_incremental(
    source_table="anvesh.default.bronze_routes",
    target_table="anvesh.default.demo_silver_routes",
    key_columns=["route_id"],
    pipeline_name="bronze_to_silver_incremental_routes",
    audit_table="anvesh.default.silver_incremental_audit"
)

print("\nPipeline Execution Result:")
print(pipeline_result)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_run_id,
# MAGIC     source_table,
# MAGIC     target_table,
# MAGIC     source_record_count,
# MAGIC     inserted_count,
# MAGIC     updated_count,
# MAGIC     target_record_count,
# MAGIC     status,
# MAGIC     error_message
# MAGIC FROM anvesh.default.silver_incremental_audit
# MAGIC WHERE pipeline_run_id =
# MAGIC     '64d23408-d97f-40b9-8e3d-4504c1a286d0';

# COMMAND ----------

silver_df = spark.table(
    "anvesh.default.demo_silver_routes"
)

display(
    silver_df.select(
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_color",
        "_ingestion_run_id"
    ).orderBy("route_id")
)

# COMMAND ----------

from pyspark.sql.functions import col, trim

# Read Bronze source
bronze_df = spark.table(
    "anvesh.default.bronze_routes"
)

# Prepare source business columns
source_df = bronze_df.select(
    trim(col("route_id")).alias("route_id"),
    trim(col("agency_id")).alias("agency_id"),
    trim(col("route_short_name")).alias("route_short_name"),
    trim(col("route_long_name")).alias("route_long_name"),
    col("route_type"),
    trim(col("route_color")).alias("route_color"),
    trim(col("route_text_color")).alias("route_text_color"),
    col("route_sort_order")
)

# Read existing Silver data
target_df = spark.table(
    "anvesh.default.demo_silver_routes"
)

# Compare source and target business attributes
comparison_df = (
    source_df.alias("source")
    .join(
        target_df.alias("target"),
        col("source.route_id") == col("target.route_id"),
        "left"
    )
)

# Identify new and changed records
changed_records_df = comparison_df.filter(
    col("target.route_id").isNull() |
    ~(
        col("source.agency_id").eqNullSafe(col("target.agency_id")) &
        col("source.route_short_name").eqNullSafe(col("target.route_short_name")) &
        col("source.route_long_name").eqNullSafe(col("target.route_long_name")) &
        col("source.route_type").eqNullSafe(col("target.route_type")) &
        col("source.route_color").eqNullSafe(col("target.route_color")) &
        col("source.route_text_color").eqNullSafe(col("target.route_text_color")) &
        col("source.route_sort_order").eqNullSafe(col("target.route_sort_order"))
    )
)

print("Source records:", source_df.count())
print("Changed or new records:", changed_records_df.count())

display(changed_records_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# Create a simulated changed Bronze batch
simulated_change_df = (
    source_df
    .filter(col("route_id") == "BLUE")
    .withColumn(
        "route_short_name",
        lit("C3_BLUE_UPDATED_V2")
    )
)

print("Simulated changed records:", simulated_change_df.count())

display(simulated_change_df)

# COMMAND ----------

# Compare simulated source against existing Silver data
simulated_comparison_df = (
    simulated_change_df.alias("source")
    .join(
        target_df.alias("target"),
        col("source.route_id") == col("target.route_id"),
        "left"
    )
)

simulated_changed_df = simulated_comparison_df.filter(
    ~(
        col("source.agency_id").eqNullSafe(col("target.agency_id")) &
        col("source.route_short_name").eqNullSafe(col("target.route_short_name")) &
        col("source.route_long_name").eqNullSafe(col("target.route_long_name")) &
        col("source.route_type").eqNullSafe(col("target.route_type")) &
        col("source.route_color").eqNullSafe(col("target.route_color")) &
        col("source.route_text_color").eqNullSafe(col("target.route_text_color")) &
        col("source.route_sort_order").eqNullSafe(col("target.route_sort_order"))
    )
)

print("Changed records detected:", simulated_changed_df.count())

display(simulated_changed_df)

# COMMAND ----------


from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp

# Add the missing metadata columns to simulated source
merge_source_df = (
    simulated_change_df
    .withColumn("_source_system", lit("HMRL_GTFS"))
    .withColumn("_ingestion_run_id", lit("SIMULATED_RUN_003"))
    .withColumn("processed_at", current_timestamp())
)

target_table = "anvesh.default.demo_silver_routes"

delta_target = DeltaTable.forName(
    spark,
    target_table
)

merge_condition = "target.route_id = source.route_id"

change_condition = """
NOT (
    target.agency_id <=> source.agency_id
    AND target.route_short_name <=> source.route_short_name
    AND target.route_long_name <=> source.route_long_name
    AND target.route_type <=> source.route_type
    AND target.route_color <=> source.route_color
    AND target.route_text_color <=> source.route_text_color
    AND target.route_sort_order <=> source.route_sort_order
)
"""

(
    delta_target.alias("target")
    .merge(
        merge_source_df.alias("source"),
        merge_condition
    )
    .whenMatchedUpdate(
        condition=change_condition,
        set={
            "agency_id": "source.agency_id",
            "route_short_name": "source.route_short_name",
            "route_long_name": "source.route_long_name",
            "route_type": "source.route_type",
            "route_color": "source.route_color",
            "route_text_color": "source.route_text_color",
            "route_sort_order": "source.route_sort_order",
            "_source_system": "source._source_system",
            "_ingestion_run_id": "source._ingestion_run_id",
            "processed_at": "source.processed_at"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Conditional MERGE completed successfully!")


# COMMAND ----------

result_df = spark.table(
    "anvesh.default.demo_silver_routes"
)

display(
    result_df.select(
        "route_id",
        "route_short_name",
        "_ingestion_run_id"
    ).orderBy("route_id")
)

# COMMAND ----------

history_df = spark.sql("""
    DESCRIBE HISTORY anvesh.default.demo_silver_routes
""")

latest_merge = (
    history_df
    .filter("operation = 'MERGE'")
    .orderBy("version", ascending=False)
    .first()
)

metrics = latest_merge["operationMetrics"]

print("MERGE VERSION:", latest_merge["version"])
print("SOURCE RECORDS:", metrics.get("numSourceRows"))
print("INSERTED RECORDS:", metrics.get("numTargetRowsInserted"))
print("UPDATED RECORDS:", metrics.get("numTargetRowsUpdated"))
print("DELETED RECORDS:", metrics.get("numTargetRowsDeleted"))

# COMMAND ----------


def run_bronze_to_silver_incremental(
    source_table,
    target_table,
    key_columns,
    pipeline_name,
    audit_table
):

    # 1. Initialize pipeline tracking
    pipeline_run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    source_record_count = 0
    inserted_count = 0
    updated_count = 0
    target_record_count = 0

    status = "FAILED"
    error_message = None

    try:

        # 2. Read Bronze source
        bronze_df = spark.table(source_table)

        source_record_count = bronze_df.count()

        if source_record_count == 0:
            raise ValueError("Bronze source table is empty.")

        # 3. Apply Silver transformations
        silver_source_df = (
            bronze_df
            .select(
                trim(col("route_id")).alias("route_id"),
                trim(col("agency_id")).alias("agency_id"),
                trim(col("route_short_name")).alias("route_short_name"),
                trim(col("route_long_name")).alias("route_long_name"),
                col("route_type").cast("integer").alias("route_type"),
                trim(col("route_color")).alias("route_color"),
                trim(col("route_text_color")).alias("route_text_color"),
                col("route_sort_order").cast("integer").alias("route_sort_order"),
                col("_source_system").alias("_source_system"),
                col("_ingestion_run_id").alias("_ingestion_run_id"),
                current_timestamp().alias("processed_at")
            )
        )

        # 4. Validate mandatory fields
        invalid_records = (
            silver_source_df
            .filter(
                col("route_id").isNull() |
                (col("route_id") == "") |
                col("agency_id").isNull() |
                col("route_short_name").isNull()
            )
            .count()
        )

        if invalid_records > 0:
            raise ValueError(
                f"Source contains {invalid_records} invalid records."
            )

        # 5. Validate duplicate source keys
        source_duplicates = (
            silver_source_df
            .groupBy(*key_columns)
            .count()
            .filter("count > 1")
            .count()
        )

        if source_duplicates > 0:
            raise ValueError(
                f"Source contains {source_duplicates} duplicate key groups."
            )

        # 6. Read and validate existing target
        target_df = spark.table(target_table)
        target_record_count = target_df.count()

        target_duplicates = (
            target_df
            .groupBy(*key_columns)
            .count()
            .filter("count > 1")
            .count()
        )

        if target_duplicates > 0:
            raise ValueError(
                f"Target contains {target_duplicates} duplicate key groups."
            )

        # 7. Build MERGE conditions
        merge_condition = " AND ".join(
            [f"target.{key} = source.{key}" for key in key_columns]
        )

        change_condition = """
            NOT (
                target.agency_id <=> source.agency_id
                AND target.route_short_name <=> source.route_short_name
                AND target.route_long_name <=> source.route_long_name
                AND target.route_type <=> source.route_type
                AND target.route_color <=> source.route_color
                AND target.route_text_color <=> source.route_text_color
                AND target.route_sort_order <=> source.route_sort_order
            )
        """

        # 8. Execute conditional Delta MERGE
        delta_target = DeltaTable.forName(
            spark,
            target_table
        )

        (
            delta_target.alias("target")
            .merge(
                silver_source_df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdate(
                condition=change_condition,
                set={
                    "agency_id": "source.agency_id",
                    "route_short_name": "source.route_short_name",
                    "route_long_name": "source.route_long_name",
                    "route_type": "source.route_type",
                    "route_color": "source.route_color",
                    "route_text_color": "source.route_text_color",
                    "route_sort_order": "source.route_sort_order",
                    "_source_system": "source._source_system",
                    "_ingestion_run_id": "source._ingestion_run_id",
                    "processed_at": "source.processed_at"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        # 9. Extract actual Delta MERGE metrics
        latest_merge = (
            spark.sql(f"DESCRIBE HISTORY {target_table}")
            .filter("operation = 'MERGE'")
            .orderBy("version", ascending=False)
            .first()
        )

        if latest_merge is None:
            raise Exception("MERGE transaction not found.")

        metrics = latest_merge["operationMetrics"]

        source_record_count = int(
            metrics.get("numSourceRows", source_record_count)
        )

        inserted_count = int(
            metrics.get("numTargetRowsInserted", 0)
        )

        updated_count = int(
            metrics.get("numTargetRowsUpdated", 0)
        )

        # 10. Capture final target count
        target_record_count = spark.table(target_table).count()

        status = "SUCCESS"

        print("Conditional Bronze-to-Silver MERGE completed!")

    except Exception as e:

        error_message = str(e)[:4000]
        status = "FAILED"

        print("Incremental pipeline failed!")
        print("Error:", error_message)

    finally:

        # 11. Write one audit record for this execution
        completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

        audit_schema = StructType([
            StructField("pipeline_run_id", StringType(), True),
            StructField("pipeline_name", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("source_record_count", LongType(), True),
            StructField("inserted_count", LongType(), True),
            StructField("updated_count", LongType(), True),
            StructField("target_record_count", LongType(), True),
            StructField("status", StringType(), True),
            StructField("started_at", TimestampType(), True),
            StructField("completed_at", TimestampType(), True),
            StructField("error_message", StringType(), True)
        ])

        audit_record = [(
            pipeline_run_id,
            pipeline_name,
            source_table,
            target_table,
            source_record_count,
            inserted_count,
            updated_count,
            target_record_count,
            status,
            started_at,
            completed_at,
            error_message
        )]

        audit_df = spark.createDataFrame(
            audit_record,
            schema=audit_schema
        )

        (
            audit_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(audit_table)
        )

        print("Pipeline Run ID:", pipeline_run_id)
        print("Source Records:", source_record_count)
        print("Inserted Records:", inserted_count)
        print("Updated Records:", updated_count)
        print("Target Records:", target_record_count)
        print("Pipeline Status:", status)

    if status == "FAILED":
        raise RuntimeError(
            f"Pipeline failed. Run ID: {pipeline_run_id}. "
            f"Error: {error_message}"
        )

    return {
        "pipeline_run_id": pipeline_run_id,
        "status": status,
        "source_records": source_record_count,
        "inserted_records": inserted_count,
        "updated_records": updated_count,
        "target_records": target_record_count
    }


print("Conditional incremental pipeline function defined successfully!")


# COMMAND ----------

import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable

from pyspark.sql.functions import (
    col,
    trim,
    current_timestamp
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)

print("All dependencies imported successfully!")

# COMMAND ----------

pipeline_result = run_bronze_to_silver_incremental(
    source_table="anvesh.default.bronze_routes",
    target_table="anvesh.default.demo_silver_routes",
    key_columns=["route_id"],
    pipeline_name="bronze_to_silver_incremental_routes",
    audit_table="anvesh.default.silver_incremental_audit"
)

print("\nPipeline Execution Result:")
print(pipeline_result)

# COMMAND ----------

final_silver_df = spark.table(
    "anvesh.default.demo_silver_routes"
)

display(
    final_silver_df.select(
        "route_id",
        "route_short_name",
        "_ingestion_run_id"
    ).orderBy("route_id")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_run_id,
# MAGIC     source_record_count,
# MAGIC     inserted_count,
# MAGIC     updated_count,
# MAGIC     target_record_count,
# MAGIC     status,
# MAGIC     error_message
# MAGIC FROM anvesh.default.silver_incremental_audit
# MAGIC WHERE pipeline_run_id =
# MAGIC 'bdc8b4b2-3bdd-4d60-84b8-4c7a4d4d07a6';

# COMMAND ----------

from pyspark.sql import Row

new_route_data = [
    Row(
        route_id="PURPLE",
        agency_id="HMRL",
        route_short_name="C5_PURPLE",
        route_long_name="Simulated Purple Line",
        route_type=1,
        route_color="800080",
        route_text_color="FFFFFF",
        route_sort_order=5,
        _source_system="HMRL_GTFS",
        _ingestion_run_id="SIMULATED_RUN_004"
    )
]

new_route_df = spark.createDataFrame(new_route_data)

new_route_df = new_route_df.withColumn(
    "processed_at",
    current_timestamp()
)

print("New incoming records:", new_route_df.count())

display(new_route_df)

# COMMAND ----------

from pyspark.sql.functions import col, count

# Check duplicate route IDs within incoming batch
duplicate_count = (
    new_route_df
    .groupBy("route_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
    .count()
)

# Check whether route already exists in Silver
existing_count = (
    spark.table("anvesh.default.demo_silver_routes")
    .filter(col("route_id") == "PURPLE")
    .count()
)

# Check mandatory fields
invalid_count = (
    new_route_df
    .filter(
        col("route_id").isNull() |
        col("agency_id").isNull() |
        col("route_short_name").isNull()
    )
    .count()
)

print("Duplicate source keys:", duplicate_count)
print("Existing target records:", existing_count)
print("Invalid records:", invalid_count)

if duplicate_count == 0 and existing_count == 0 and invalid_count == 0:
    print("NEW ROUTE VALIDATION: PASSED")
else:
    raise Exception("New route validation failed!")

# COMMAND ----------

from delta.tables import DeltaTable

target_table = "anvesh.default.demo_silver_routes"

delta_target = DeltaTable.forName(spark, target_table)

(
    delta_target.alias("target")
    .merge(
        new_route_df.alias("source"),
        "target.route_id = source.route_id"
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
                target.agency_id <=> source.agency_id
                AND target.route_short_name <=> source.route_short_name
                AND target.route_long_name <=> source.route_long_name
                AND target.route_type <=> source.route_type
                AND target.route_color <=> source.route_color
                AND target.route_text_color <=> source.route_text_color
                AND target.route_sort_order <=> source.route_sort_order
            )
        """,
        set={
            "agency_id": "source.agency_id",
            "route_short_name": "source.route_short_name",
            "route_long_name": "source.route_long_name",
            "route_type": "source.route_type",
            "route_color": "source.route_color",
            "route_text_color": "source.route_text_color",
            "route_sort_order": "source.route_sort_order",
            "_source_system": "source._source_system",
            "_ingestion_run_id": "source._ingestion_run_id",
            "processed_at": "source.processed_at"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Delta MERGE completed successfully.")

# COMMAND ----------

silver_routes_df = spark.table(
    "anvesh.default.demo_silver_routes"
)

print("Total Silver routes:", silver_routes_df.count())

display(
    silver_routes_df
    .filter(col("route_id") == "PURPLE")
)

# COMMAND ----------

history_df = (
    spark.sql("""
        DESCRIBE HISTORY anvesh.default.demo_silver_routes
    """)
)

display(
    history_df
    .select(
        "version",
        "operation",
        "operationMetrics",
        "timestamp"
    )
    .limit(5)
)

# COMMAND ----------

history_df = spark.sql("""
    DESCRIBE HISTORY anvesh.default.demo_silver_routes
""")

display(
    history_df
    .select(
        "version",
        "operation",
        "operationMetrics",
        "timestamp"
    )
    .limit(1)
)

# COMMAND ----------

display(
    spark.sql("""
        DESCRIBE HISTORY anvesh.default.demo_silver_routes
    """)
    .select("version", "operation", "operationMetrics", "timestamp")
    .limit(3)
)

# COMMAND ----------

from pyspark.sql.functions import col

purple_count = (
    spark.table("anvesh.default.demo_silver_routes")
    .filter(col("route_id") == "PURPLE")
    .count()
)

total_count = spark.table(
    "anvesh.default.demo_silver_routes"
).count()

print("PURPLE records:", purple_count)
print("Total Silver records:", total_count)

# COMMAND ----------

latest_merge = (
    spark.sql("""
        DESCRIBE HISTORY anvesh.default.demo_silver_routes
    """)
    .filter("operation = 'MERGE'")
    .orderBy(col("version").desc())
    .first()
)

print("Latest Delta version:", latest_merge["version"])
print("Operation:", latest_merge["operation"])
print("Metrics:", latest_merge["operationMetrics"])

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

target_table = "anvesh.default.demo_silver_routes"

delta_target = DeltaTable.forName(spark, target_table)

(
    delta_target.alias("target")
    .merge(
        new_route_df.alias("source"),
        "target.route_id = source.route_id"
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
                target.agency_id <=> source.agency_id
                AND target.route_short_name <=> source.route_short_name
                AND target.route_long_name <=> source.route_long_name
                AND target.route_type <=> source.route_type
                AND target.route_color <=> source.route_color
                AND target.route_text_color <=> source.route_text_color
                AND target.route_sort_order <=> source.route_sort_order
            )
        """,
        set={
            "agency_id": "source.agency_id",
            "route_short_name": "source.route_short_name",
            "route_long_name": "source.route_long_name",
            "route_type": "source.route_type",
            "route_color": "source.route_color",
            "route_text_color": "source.route_text_color",
            "route_sort_order": "source.route_sort_order",
            "_source_system": "source._source_system",
            "_ingestion_run_id": "source._ingestion_run_id",
            "processed_at": "source.processed_at"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Second MERGE execution completed.")

# COMMAND ----------

display(
    spark.sql("""
        DESCRIBE HISTORY anvesh.default.demo_silver_routes
    """)
    .select("version", "operation", "operationMetrics")
    .limit(2)
)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# Read the existing demo Silver routes
cdc_demo_df = (
    spark.table("anvesh.default.demo_silver_routes")
    .withColumn("is_active", lit(True))
    .withColumn("deleted_at", lit(None).cast("timestamp"))
)

# Create a separate CDC demo table
(
    cdc_demo_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("anvesh.default.demo_cdc_routes")
)

print("CDC demo table created successfully.")
print("Total records:", cdc_demo_df.count())

display(
    spark.table("anvesh.default.demo_cdc_routes")
)

# COMMAND ----------

