# Databricks notebook source

# # INDIA URBAN MOBILITY PLATFORM

# # CHANGE DATA CAPTURE (CDC) AND SOFT DELETE HANDLING

# ## Objective
# Implement CDC concepts using Delta Lake MERGE operations.

# ## Learning Scope
# 1. Understand Change Data Capture (CDC)
# 2. Identify INSERT, UPDATE and DELETE events
# 3. Implement soft deletes
# 4. Implement hard deletes
# 5. Track record changes
# 6. Maintain CDC audit history
# 7. Validate incremental CDC processing
# 8. Test idempotency

# ## Source Table
# anvesh.default.demo_silver_routes

# ## Target Table
# anvesh.default.demo_cdc_routes

# ## Environment
# Databricks Free Edition
# PySpark
# Delta Lake


# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# Read existing Silver demo table
cdc_demo_df = (
    spark.table("anvesh.default.demo_silver_routes")
    .withColumn("is_active", lit(True))
    .withColumn("deleted_at", lit(None).cast("timestamp"))
    .withColumn("cdc_processed_at", current_timestamp())
)

# Write to CDC demo table with schema migration enabled
(
    cdc_demo_df.write
    .format("delta")
    .option("mergeSchema", "true")
    .mode("overwrite")
    .saveAsTable("anvesh.default.demo_cdc_routes")
)

print("CDC demo table created successfully")
print("Total records:", cdc_demo_df.count())

display(
    spark.table("anvesh.default.demo_cdc_routes")
)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

cdc_events = [
    Row(
        route_id="RED",
        route_short_name="C1_RED_UPDATED",
        route_long_name="Updated Red Line",
        agency_id="HMRL",
        route_type=1,
        route_color="FF0000",
        route_text_color="FFFFFF",
        route_sort_order=1,
        operation="UPDATE"
    ),
    Row(
        route_id="YELLOW",
        route_short_name="C6_YELLOW",
        route_long_name="Simulated Yellow Line",
        agency_id="HMRL",
        route_type=1,
        route_color="FFFF00",
        route_text_color="000000",
        route_sort_order=6,
        operation="INSERT"
    ),
    Row(
        route_id="GREEN",
        route_short_name="C2_GREEN",
        route_long_name="Green Line",
        agency_id="HMRL",
        route_type=1,
        route_color="008000",
        route_text_color="FFFFFF",
        route_sort_order=2,
        operation="DELETE"
    )
]

cdc_source_df = spark.createDataFrame(cdc_events)

cdc_source_df = cdc_source_df.withColumn(
    "event_timestamp",
    current_timestamp()
)

print("Incoming CDC events:", cdc_source_df.count())

display(cdc_source_df)

# COMMAND ----------

from pyspark.sql.functions import col

# Count each CDC operation
cdc_source_df.groupBy("operation").count().show()

# Check duplicate route IDs
duplicate_count = (
    cdc_source_df
    .groupBy("route_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

# Check invalid operations
invalid_operation_count = (
    cdc_source_df
    .filter(~col("operation").isin("INSERT", "UPDATE", "DELETE"))
    .count()
)

# Check missing route IDs
null_route_count = (
    cdc_source_df
    .filter(col("route_id").isNull())
    .count()
)

print("Duplicate route IDs:", duplicate_count)
print("Invalid operations:", invalid_operation_count)
print("Null route IDs:", null_route_count)

# COMMAND ----------

cdc_target_df = spark.table(
    "anvesh.default.demo_cdc_routes"
)

print("Current target records:", cdc_target_df.count())

display(
    cdc_target_df
    .select(
        "route_id",
        "route_short_name",
        "is_active",
        "deleted_at"
    )
    .orderBy("route_id")
)

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp
)

target_table = "anvesh.default.demo_cdc_routes"

delta_target = DeltaTable.forName(spark, target_table)

(
    delta_target.alias("target")
    .merge(
        cdc_source_df.alias("source"),
        "target.route_id = source.route_id"
    )

    # Update existing records
    .whenMatchedUpdate(
        condition="source.operation = 'UPDATE'",
        set={
            "route_short_name": "source.route_short_name",
            "route_long_name": "source.route_long_name",
            "agency_id": "source.agency_id",
            "route_type": "source.route_type",
            "route_color": "source.route_color",
            "route_text_color": "source.route_text_color",
            "route_sort_order": "source.route_sort_order",
            "is_active": "true",
            "deleted_at": "CAST(NULL AS TIMESTAMP)",
            "cdc_processed_at": "current_timestamp()"
        }
    )

    # Soft delete existing records
    .whenMatchedUpdate(
        condition="source.operation = 'DELETE'",
        set={
            "is_active": "false",
            "deleted_at": "current_timestamp()",
            "cdc_processed_at": "current_timestamp()"
        }
    )

    # Insert new records
    .whenNotMatchedInsert(
        condition="source.operation = 'INSERT'",
        values={
            "route_id": "source.route_id",
            "route_short_name": "source.route_short_name",
            "route_long_name": "source.route_long_name",
            "agency_id": "source.agency_id",
            "route_type": "source.route_type",
            "route_color": "source.route_color",
            "route_text_color": "source.route_text_color",
            "route_sort_order": "source.route_sort_order",
            "_source_system": "'HMRL_GTFS'",
            "_ingestion_run_id": "'CDC_SIMULATED_BATCH_001'",
            "processed_at": "current_timestamp()",
            "is_active": "true",
            "deleted_at": "CAST(NULL AS TIMESTAMP)",
            "cdc_processed_at": "current_timestamp()"
        }
    )
    .execute()
)

print("CDC MERGE completed successfully.")

# COMMAND ----------

cdc_result_df = spark.table(
    "anvesh.default.demo_cdc_routes"
)

print("Total records:", cdc_result_df.count())

display(
    cdc_result_df
    .select(
        "route_id",
        "route_short_name",
        "is_active",
        "deleted_at"
    )
    .orderBy("route_id")
)

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_routes")
    .select(
        "route_id",
        "route_short_name",
        "is_active",
        "deleted_at"
    )
    .orderBy("route_id")
)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

new_cdc_event = [
    Row(
        route_id="ORANGE",
        route_short_name="C7_ORANGE",
        route_long_name="Simulated Orange Line",
        agency_id="HMRL",
        route_type=1,
        route_color="FFA500",
        route_text_color="FFFFFF",
        route_sort_order=7,
        operation="INSERT"
    )
]

new_cdc_df = spark.createDataFrame(new_cdc_event)

new_cdc_df = new_cdc_df.withColumn(
    "event_timestamp",
    current_timestamp()
)

display(new_cdc_df)

# COMMAND ----------

from delta.tables import DeltaTable

target_table = "anvesh.default.demo_cdc_routes"

delta_target = DeltaTable.forName(spark, target_table)

(
    delta_target.alias("target")
    .merge(
        new_cdc_df.alias("source"),
        "target.route_id = source.route_id"
    )
    .whenNotMatchedInsert(
        condition="source.operation = 'INSERT'",
        values={
            "route_id": "source.route_id",
            "route_short_name": "source.route_short_name",
            "route_long_name": "source.route_long_name",
            "agency_id": "source.agency_id",
            "route_type": "source.route_type",
            "route_color": "source.route_color",
            "route_text_color": "source.route_text_color",
            "route_sort_order": "source.route_sort_order",
            "_source_system": "'HMRL_GTFS'",
            "_ingestion_run_id": "'CDC_SIMULATED_BATCH_002'",
            "processed_at": "current_timestamp()",
            "is_active": "true",
            "deleted_at": "CAST(NULL AS TIMESTAMP)",
            "cdc_processed_at": "current_timestamp()"
        }
    )
    .execute()
)

print("CDC INSERT completed successfully.")

# COMMAND ----------

cdc_target_df = spark.table(
    "anvesh.default.demo_cdc_routes"
)

print("Total records:", cdc_target_df.count())

display(
    cdc_target_df
    .filter(col("route_id") == "ORANGE")
)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType
)

cdc_audit_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("route_id", StringType(), False),
    StructField("operation", StringType(), False),
    StructField("event_timestamp", TimestampType(), True),
    StructField("processed_at", TimestampType(), True),
    StructField("status", StringType(), False)
])

empty_audit_df = spark.createDataFrame(
    [],
    cdc_audit_schema
)

(
    empty_audit_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("anvesh.default.demo_cdc_audit")
)

print("CDC audit table created successfully")

display(
    spark.table("anvesh.default.demo_cdc_audit")
)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

audit_records = [
    Row(
        event_id="CDC-001",
        pipeline_name="urban_mobility_cdc",
        route_id="RED",
        operation="UPDATE",
        event_timestamp=None,
        processed_at=None,
        status="SUCCESS"
    ),
    Row(
        event_id="CDC-002",
        pipeline_name="urban_mobility_cdc",
        route_id="GREEN",
        operation="DELETE",
        event_timestamp=None,
        processed_at=None,
        status="SUCCESS"
    ),
    Row(
        event_id="CDC-003",
        pipeline_name="urban_mobility_cdc",
        route_id="ORANGE",
        operation="INSERT",
        event_timestamp=None,
        processed_at=None,
        status="SUCCESS"
    )
]

audit_df = spark.createDataFrame(
    audit_records,
    cdc_audit_schema
)

audit_df = (
    audit_df
    .withColumn("event_timestamp", current_timestamp())
    .withColumn("processed_at", current_timestamp())
)

print("Audit columns:", len(audit_df.columns))
print("Audit records:", audit_df.count())

display(audit_df)

# COMMAND ----------

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.demo_cdc_audit")
)

print("CDC audit records saved successfully.")

# COMMAND ----------

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.demo_cdc_audit")
)

print("CDC audit records saved successfully.")

print(
    "Total audit records:",
    spark.table("anvesh.default.demo_cdc_audit").count()
)

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_audit")
    .orderBy("event_id")
)

# COMMAND ----------

from pyspark.sql.functions import col, count

audit_table = spark.table(
    "anvesh.default.demo_cdc_audit"
)

duplicates_df = (
    audit_table
    .groupBy("event_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

display(duplicates_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

window_spec = (
    Window
    .partitionBy("event_id")
    .orderBy(desc("processed_at"))
)

deduplicated_df = (
    audit_table
    .withColumn(
        "row_num",
        row_number().over(window_spec)
    )
    .filter(col("row_num") == 1)
    .drop("row_num")
)

(
    deduplicated_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("anvesh.default.demo_cdc_audit")
)

print(
    "Audit records after deduplication:",
    spark.table("anvesh.default.demo_cdc_audit").count()
)

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_audit")
    .orderBy("event_id")
)

# COMMAND ----------

import uuid

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    monotonically_increasing_id,
    concat
)

def write_cdc_audit(
    source_df,
    pipeline_name,
    audit_table
):
    pipeline_run_id = str(uuid.uuid4())

    audit_df = (
        source_df
        .select(
            col("route_id"),
            col("operation"),
            col("event_timestamp")
        )
        .withColumn(
            "event_id",
            concat(
                lit(pipeline_run_id + "_"),
                monotonically_increasing_id().cast("string")
            )
        )
        .withColumn(
            "pipeline_name",
            lit(pipeline_name)
        )
        .withColumn(
            "processed_at",
            current_timestamp()
        )
        .withColumn(
            "status",
            lit("SUCCESS")
        )
        .select(
            "event_id",
            "pipeline_name",
            "route_id",
            "operation",
            "event_timestamp",
            "processed_at",
            "status"
        )
    )

    audit_count = audit_df.count()

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(audit_table)
    )

    print("CDC audit logging completed.")
    print("Pipeline run ID:", pipeline_run_id)
    print("Audit events:", audit_count)

# COMMAND ----------

write_cdc_audit(
    source_df=new_cdc_df,
    pipeline_name="urban_mobility_cdc",
    audit_table="anvesh.default.demo_cdc_audit"
)

# COMMAND ----------

audit_result_df = spark.table(
    "anvesh.default.demo_cdc_audit"
)

print("Total audit records:", audit_result_df.count())

display(
    audit_result_df
    .orderBy("processed_at", ascending=False)
)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)

run_audit_schema = StructType([
    StructField("pipeline_run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("source_event_count", LongType(), True),
    StructField("inserted_count", LongType(), True),
    StructField("updated_count", LongType(), True),
    StructField("soft_deleted_count", LongType(), True),
    StructField("target_record_count", LongType(), True),
    StructField("status", StringType(), False),
    StructField("started_at", TimestampType(), True),
    StructField("completed_at", TimestampType(), True),
    StructField("error_message", StringType(), True)
])

empty_run_audit_df = spark.createDataFrame(
    [],
    run_audit_schema
)

(
    empty_run_audit_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("anvesh.default.demo_cdc_run_audit")
)

print("CDC run-level audit table created successfully.")

display(
    spark.table("anvesh.default.demo_cdc_run_audit")
)

# COMMAND ----------

history_df = spark.sql("""
    DESCRIBE HISTORY anvesh.default.demo_cdc_routes
""")

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

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

test_cdc_events = [
    Row(
        route_id="SILVER",
        route_short_name="C8_SILVER",
        route_long_name="Simulated Silver Line",
        agency_id="HMRL",
        route_type=1,
        route_color="C0C0C0",
        route_text_color="000000",
        route_sort_order=8,
        operation="INSERT"
    )
]

test_cdc_df = (
    spark.createDataFrame(test_cdc_events)
    .withColumn("event_timestamp", current_timestamp())
)

print("Incoming CDC events:", test_cdc_df.count())

display(test_cdc_df)

# COMMAND ----------


import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number


def run_cdc_pipeline(source_df, target_table, audit_table):

    pipeline_run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    source_count = 0
    deduplicated_count = 0
    inserted_count = 0
    updated_count = 0
    soft_deleted_count = 0
    stale_event_count = 0
    target_count = 0

    status = "FAILED"
    error_message = None

    try:
        # 1. Capture target count
        target_count = spark.table(target_table).count()

        # 2. Validate source
        source_count = source_df.count()

        if source_count == 0:
            raise ValueError("CDC source batch is empty")

        required_columns = [
            "route_id",
            "operation",
            "event_timestamp",
            "source_event_id",
            "source_sequence",
            "route_short_name",
            "route_long_name",
            "agency_id",
            "route_type",
            "route_color",
            "route_text_color",
            "route_sort_order"
        ]

        missing_columns = [
            c for c in required_columns
            if c not in source_df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing CDC columns: {missing_columns}"
            )

        invalid_count = (
            source_df
            .filter(
                col("route_id").isNull()
                | col("operation").isNull()
                | col("event_timestamp").isNull()
                | col("source_event_id").isNull()
                | col("source_sequence").isNull()
                | (~col("operation").isin(
                    "INSERT", "UPDATE", "DELETE"
                ))
            )
            .count()
        )

        if invalid_count > 0:
            raise ValueError("Invalid CDC records found")

        # 3. Reject duplicate source event IDs
        duplicate_event_ids = (
            source_df
            .groupBy("source_event_id")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        if duplicate_event_ids > 0:
            raise ValueError("Duplicate source event IDs found")

        # 4. Reject ambiguous ordering for a route
        ambiguous_events = (
            source_df
            .groupBy(
                "route_id",
                "event_timestamp",
                "source_sequence"
            )
            .count()
            .filter(col("count") > 1)
            .count()
        )

        if ambiguous_events > 0:
            raise ValueError(
                "Ambiguous CDC events: same route, timestamp "
                "and sequence"
            )

        # 5. Deduplicate: newest timestamp, then highest sequence
        event_window = (
            Window
            .partitionBy("route_id")
            .orderBy(
                col("event_timestamp").desc(),
                col("source_sequence").desc()
            )
        )

        deduplicated_df = (
            source_df
            .withColumn(
                "_row_number",
                row_number().over(event_window)
            )
            .filter(col("_row_number") == 1)
            .drop("_row_number")
        )

        deduplicated_count = deduplicated_df.count()

        # 6. Identify stale events
        target_df = spark.table(target_table)

        target_ordering = target_df.select(
            "route_id",
            "last_event_timestamp",
            "last_source_sequence",
            "is_active"
        )

        comparison_df = (
            deduplicated_df.alias("source")
            .join(
                target_ordering.alias("target"),
                col("source.route_id") == col("target.route_id"),
                "left"
            )
        )

        stale_events_df = comparison_df.filter(
            col("target.route_id").isNotNull()
            & col("target.last_event_timestamp").isNotNull()
            & (
                (
                    col("source.event_timestamp")
                    < col("target.last_event_timestamp")
                )
                | (
                    (
                        col("source.event_timestamp")
                        == col("target.last_event_timestamp")
                    )
                    & (
                        col("source.source_sequence")
                        <= col("target.last_source_sequence")
                    )
                )
            )
        )

        stale_event_count = stale_events_df.count()

        # 7. Keep only events newer than the target
        eligible_source_df = (
            comparison_df
            .filter(
                col("target.route_id").isNull()
                | col("target.last_event_timestamp").isNull()
                | (
                    col("source.event_timestamp")
                    > col("target.last_event_timestamp")
                )
                | (
                    (
                        col("source.event_timestamp")
                        == col("target.last_event_timestamp")
                    )
                    & (
                        col("source.source_sequence")
                        > col("target.last_source_sequence")
                    )
                )
            )
            .select("source.*")
        )

        # 8. Calculate eligible soft deletes
        soft_deleted_count = (
            eligible_source_df
            .filter(col("operation") == "DELETE")
            .join(
                target_df
                .filter(col("is_active") == True)
                .select("route_id"),
                on="route_id",
                how="inner"
            )
            .count()
        )

        print("Source events:", source_count)
        print("Events after deduplication:", deduplicated_count)
        print("Stale events rejected:", stale_event_count)
        print("Eligible soft deletes:", soft_deleted_count)

        # 9. Capture Delta version
        previous_version = (
            spark.sql(f"DESCRIBE HISTORY {target_table}")
            .agg({"version": "max"})
            .first()[0]
        )

        # 10. Execute CDC MERGE
        target = DeltaTable.forName(spark, target_table)

        (
            target.alias("target")
            .merge(
                eligible_source_df.alias("source"),
                "target.route_id = source.route_id"
            )

            .whenMatchedUpdate(
                condition="""
                    source.operation = 'UPDATE'
                    AND (
                        target.last_event_timestamp IS NULL
                        OR source.event_timestamp >
                           target.last_event_timestamp
                        OR (
                            source.event_timestamp =
                            target.last_event_timestamp
                            AND source.source_sequence >
                            target.last_source_sequence
                        )
                    )
                """,
                set={
                    "route_short_name": "source.route_short_name",
                    "route_long_name": "source.route_long_name",
                    "agency_id": "source.agency_id",
                    "route_type": "source.route_type",
                    "route_color": "source.route_color",
                    "route_text_color": "source.route_text_color",
                    "route_sort_order": "source.route_sort_order",
                    "is_active": "true",
                    "deleted_at": "CAST(NULL AS TIMESTAMP)",
                    "cdc_processed_at": "current_timestamp()",
                    "last_event_timestamp": "source.event_timestamp",
                    "last_source_sequence": "source.source_sequence"
                }
            )

            .whenMatchedUpdate(
                condition="""
                    source.operation = 'DELETE'
                    AND target.is_active = true
                    AND (
                        target.last_event_timestamp IS NULL
                        OR source.event_timestamp >
                           target.last_event_timestamp
                        OR (
                            source.event_timestamp =
                            target.last_event_timestamp
                            AND source.source_sequence >
                            target.last_source_sequence
                        )
                    )
                """,
                set={
                    "is_active": "false",
                    "deleted_at": "current_timestamp()",
                    "cdc_processed_at": "current_timestamp()",
                    "last_event_timestamp": "source.event_timestamp",
                    "last_source_sequence": "source.source_sequence"
                }
            )

            .whenNotMatchedInsert(
                condition="source.operation = 'INSERT'",
                values={
                    "route_id": "source.route_id",
                    "route_short_name": "source.route_short_name",
                    "route_long_name": "source.route_long_name",
                    "agency_id": "source.agency_id",
                    "route_type": "source.route_type",
                    "route_color": "source.route_color",
                    "route_text_color": "source.route_text_color",
                    "route_sort_order": "source.route_sort_order",
                    "_source_system": "'HMRL_GTFS'",
                    "_ingestion_run_id": "'CDC_PIPELINE'",
                    "processed_at": "current_timestamp()",
                    "is_active": "true",
                    "deleted_at": "CAST(NULL AS TIMESTAMP)",
                    "cdc_processed_at": "current_timestamp()",
                    "last_event_timestamp": "source.event_timestamp",
                    "last_source_sequence": "source.source_sequence"
                }
            )
            .execute()
        )

        # 11. Read Delta transaction metrics
        latest_history = (
            spark.sql(f"DESCRIBE HISTORY {target_table}")
            .filter(col("version") > previous_version)
            .filter(col("operation") == "MERGE")
            .orderBy(col("version").desc())
            .first()
        )

        if latest_history is None:
            raise RuntimeError("MERGE transaction not found")

        metrics = latest_history["operationMetrics"]

        inserted_count = int(
            metrics.get("numTargetRowsInserted", "0")
        )

        total_updated = int(
            metrics.get("numTargetRowsUpdated", "0")
        )

        updated_count = max(
            total_updated - soft_deleted_count,
            0
        )

        target_count = spark.table(target_table).count()

        status = "SUCCESS"

    except Exception as error:
        error_message = str(error)[:4000]
        status = "FAILED"

        print("CDC pipeline failed:", error_message)

    finally:
        completed_at = datetime.now(timezone.utc)

        audit_record = [(
            pipeline_run_id,
            "urban_mobility_cdc",
            source_count,
            inserted_count,
            updated_count,
            soft_deleted_count,
            target_count,
            status,
            started_at,
            completed_at,
            error_message
        )]

        audit_df = spark.createDataFrame(
            audit_record,
            run_audit_schema
        )

        audit_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(audit_table)

        print("\n========== CDC PIPELINE SUMMARY ==========")
        print("Pipeline run ID:", pipeline_run_id)
        print("Source events:", source_count)
        print("Events after deduplication:", deduplicated_count)
        print("Stale events rejected:", stale_event_count)
        print("Inserted:", inserted_count)
        print("Updated:", updated_count)
        print("Soft deleted:", soft_deleted_count)
        print("Target records:", target_count)
        print("Status:", status)
        print("Error:", error_message)
        print("==========================================")

    if status == "FAILED":
        raise RuntimeError(
            f"CDC pipeline failed: {error_message}"
        )


# COMMAND ----------

run_cdc_pipeline(
    source_df=test_cdc_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_run_audit")
    .orderBy("completed_at", ascending=False)
)

# COMMAND ----------

from pyspark.sql.functions import col

silver_count = (
    spark.table("anvesh.default.demo_cdc_routes")
    .filter(col("route_id") == "SILVER")
    .count()
)

print("SILVER records before rerun:", silver_count)

# COMMAND ----------

run_cdc_pipeline(
    source_df=test_cdc_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_run_audit")
    .orderBy("started_at", ascending=False)
)

# COMMAND ----------

hard_delete_demo_df = (
    spark.table("anvesh.default.demo_cdc_routes")
    .filter(col("route_id").isin("SILVER", "ORANGE"))
)

(
    hard_delete_demo_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("anvesh.default.demo_hard_delete")
)

print(
    "Hard delete demo records:",
    spark.table("anvesh.default.demo_hard_delete").count()
)

display(
    spark.table("anvesh.default.demo_hard_delete")
)

# COMMAND ----------

hard_delete_df = spark.table(
    "anvesh.default.demo_hard_delete"
)

print("Records before deletion:", hard_delete_df.count())

display(
    hard_delete_df.select(
        "route_id",
        "route_short_name",
        "is_active"
    )
)

# COMMAND ----------

from delta.tables import DeltaTable

hard_delete_target = DeltaTable.forName(
    spark,
    "anvesh.default.demo_hard_delete"
)

(
    hard_delete_target
    .delete("route_id = 'ORANGE'")
)

print("Hard DELETE operation completed.")

# COMMAND ----------

hard_delete_result = spark.table(
    "anvesh.default.demo_hard_delete"
)

print("Records after deletion:", hard_delete_result.count())

display(hard_delete_result)

# COMMAND ----------

display(
    spark.sql("""
        DESCRIBE HISTORY anvesh.default.demo_hard_delete
    """)
    .select(
        "version",
        "operation",
        "operationMetrics",
        "timestamp"
    )
    .limit(5)
)

# COMMAND ----------

historical_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .table("anvesh.default.demo_hard_delete")
)

print("Historical version 0 records:", historical_df.count())

display(
    historical_df.select(
        "route_id",
        "route_short_name",
        "is_active"
    )
)

# COMMAND ----------

current_df = spark.table(
    "anvesh.default.demo_hard_delete"
)

historical_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .table("anvesh.default.demo_hard_delete")
)

print("Historical version 0:", historical_df.count())
print("Current version:", current_df.count())

print("Historical route IDs:")
historical_df.select("route_id").show()

print("Current route IDs:")
current_df.select("route_id").show()

# COMMAND ----------

spark.sql("""
    DESCRIBE HISTORY anvesh.default.demo_hard_delete
""").select(
    "version",
    "operation",
    "timestamp"
).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE anvesh.default.demo_hard_delete
# MAGIC TO VERSION AS OF 0;

# COMMAND ----------

restored_df = spark.table(
    "anvesh.default.demo_hard_delete"
)

print("Records after RESTORE:", restored_df.count())

display(
    restored_df.select(
        "route_id",
        "route_short_name",
        "is_active"
    )
)

# COMMAND ----------

display(
    spark.sql("""
        DESCRIBE HISTORY anvesh.default.demo_hard_delete
    """)
    .select(
        "version",
        "operation",
        "operationMetrics",
        "timestamp"
    )
    .limit(5)
)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

invalid_cdc_events = [
    Row(
        route_id="FAIL_TEST",
        route_short_name="C9_FAIL",
        route_long_name="Failure Test Route",
        agency_id="HMRL",
        route_type=1,
        route_color="000000",
        route_text_color="FFFFFF",
        route_sort_order=9,
        operation="INSERT"
    ),
    Row(
        route_id="FAIL_TEST",
        route_short_name="C9_FAIL_DUP",
        route_long_name="Duplicate Failure Test Route",
        agency_id="HMRL",
        route_type=1,
        route_color="111111",
        route_text_color="FFFFFF",
        route_sort_order=10,
        operation="INSERT"
    )
]

invalid_cdc_df = (
    spark.createDataFrame(invalid_cdc_events)
    .withColumn("event_timestamp", current_timestamp())
)

print("Invalid batch records:", invalid_cdc_df.count())

display(invalid_cdc_df)

# COMMAND ----------

run_cdc_pipeline(
    source_df=invalid_cdc_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_run_audit")
    .filter(
        col("pipeline_run_id") ==
        "05a51224-d75b-4cf0-bc58-77da4ab53063"
    )
)

# COMMAND ----------

target_df = spark.table(
    "anvesh.default.demo_cdc_routes"
)

print("Actual target records:", target_df.count())

print(
    "FAIL_TEST records:",
    target_df.filter(
        col("route_id") == "FAIL_TEST"
    ).count()
)

# COMMAND ----------

audit_df = spark.table("anvesh.default.demo_cdc_run_audit")

audit_df.filter(
    audit_df.pipeline_run_id == "1089c7fa-0d55-4ccb-a9a6-970d6642949d"
).show(truncate=False)

# COMMAND ----------

# 2. Capture current Delta version
history = spark.sql(
    f"DESCRIBE HISTORY {target_table}"
)

# COMMAND ----------

print("CDC notebook execution test")

# COMMAND ----------


from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp

insert_test_events = [
    Row(
        route_id="CDC_TEST_001",
        route_short_name="C10_TEST",
        route_long_name="CDC Insert Test Route",
        agency_id="HMRL",
        route_type=1,
        route_color="123456",
        route_text_color="FFFFFF",
        route_sort_order=11,
        operation="INSERT"
    )
]

insert_test_df = (
    spark.createDataFrame(insert_test_events)
    .withColumn("event_timestamp", current_timestamp())
)

display(insert_test_df)


# COMMAND ----------

run_cdc_pipeline(
    source_df=insert_test_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------


update_test_events = [
    Row(
        route_id="CDC_TEST_001",
        route_short_name="C10_UPDATED",
        route_long_name="CDC Updated Test Route",
        agency_id="HMRL",
        route_type=1,
        route_color="654321",
        route_text_color="FFFFFF",
        route_sort_order=12,
        operation="UPDATE"
    )
]

update_test_df = (
    spark.createDataFrame(update_test_events)
    .withColumn("event_timestamp", current_timestamp())
)

display(update_test_df)


# COMMAND ----------

run_cdc_pipeline(
    source_df=update_test_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------


delete_test_events = [
    Row(
        route_id="CDC_TEST_001",
        route_short_name="C10_UPDATED",
        route_long_name="CDC Updated Test Route",
        agency_id="HMRL",
        route_type=1,
        route_color="654321",
        route_text_color="FFFFFF",
        route_sort_order=12,
        operation="DELETE"
    )
]

delete_test_df = (
    spark.createDataFrame(delete_test_events)
    .withColumn("event_timestamp", current_timestamp())
)

display(delete_test_df)


# COMMAND ----------

run_cdc_pipeline(
    source_df=delete_test_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------

from pyspark.sql.functions import col

display(
    spark.table("anvesh.default.demo_cdc_routes")
    .filter(col("route_id") == "CDC_TEST_001")
    .select(
        "route_id",
        "route_short_name",
        "route_long_name",
        "is_active",
        "deleted_at",
        "cdc_processed_at"
    )
)

# COMMAND ----------

run_cdc_pipeline(
    source_df=delete_test_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------

reactivate_events = [
    Row(
        route_id="CDC_TEST_001",
        route_short_name="C10_REACTIVATED",
        route_long_name="Reactivated Test Route",
        agency_id="HMRL",
        route_type=1,
        route_color="123456",
        route_text_color="FFFFFF",
        route_sort_order=13,
        operation="UPDATE"
    )
]

reactivate_df = (
    spark.createDataFrame(reactivate_events)
    .withColumn("event_timestamp", current_timestamp())
)

display(reactivate_df)

# COMMAND ----------

run_cdc_pipeline(
    source_df=reactivate_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_routes")
    .filter(col("route_id") == "CDC_TEST_001")
    .select(
        "route_id",
        "route_short_name",
        "route_long_name",
        "is_active",
        "deleted_at",
        "cdc_processed_at"
    )
)

# COMMAND ----------

spark.table(
    "anvesh.default.demo_cdc_routes"
).printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

spark.sql("""
    ALTER TABLE anvesh.default.demo_cdc_routes
    ADD COLUMNS (
        last_event_timestamp TIMESTAMP
    )
""")

print("CDC event ordering column added successfully")

# COMMAND ----------

spark.table(
    "anvesh.default.demo_cdc_routes"
).printSchema()

# COMMAND ----------

spark.sql("""
    UPDATE anvesh.default.demo_cdc_routes
    SET last_event_timestamp = cdc_processed_at
    WHERE last_event_timestamp IS NULL
      AND cdc_processed_at IS NOT NULL
""")

print("Existing CDC timestamps initialized")

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_routes")
    .select(
        "route_id",
        "last_event_timestamp",
        "cdc_processed_at"
    )
)

# COMMAND ----------


from pyspark.sql import Row
from datetime import datetime, timezone

ordering_test_events = [
    Row(
        route_id="CDC_TEST_001",
        route_short_name="NEWER_EVENT",
        route_long_name="Newer CDC Event",
        agency_id="HMRL",
        route_type=1,
        route_color="123456",
        route_text_color="FFFFFF",
        route_sort_order=20,
        operation="UPDATE",
        event_timestamp=datetime(2026, 11, 19, 5, 10, 0, tzinfo=timezone.utc)
    ),
    Row(
        route_id="CDC_TEST_001",
        route_short_name="OLDER_EVENT",
        route_long_name="Older CDC Event",
        agency_id="HMRL",
        route_type=1,
        route_color="654321",
        route_text_color="FFFFFF",
        route_sort_order=19,
        operation="UPDATE",
        event_timestamp=datetime(2026, 11, 19, 5, 5, 0, tzinfo=timezone.utc)
    )
]

ordering_test_df = spark.createDataFrame(ordering_test_events)

display(
    ordering_test_df.orderBy(col("event_timestamp"))
)


# COMMAND ----------


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

event_window = (
    Window
    .partitionBy("route_id")
    .orderBy(col("event_timestamp").desc())
)

latest_cdc_df = (
    ordering_test_df
    .withColumn(
        "row_num",
        row_number().over(event_window)
    )
    .filter(col("row_num") == 1)
    .drop("row_num")
)

display(latest_cdc_df)


# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_routes")
    .filter(col("route_id") == "CDC_TEST_001")
    .select(
        "route_id",
        "route_short_name",
        "is_active",
        "last_event_timestamp"
    )
)

# COMMAND ----------

target_timestamp = (
    spark.table("anvesh.default.demo_cdc_routes")
    .filter(col("route_id") == "CDC_TEST_001")
    .select("last_event_timestamp")
)

display(
    latest_cdc_df.alias("source")
    .join(
        target_timestamp.alias("target"),
        latest_cdc_df.route_id == "CDC_TEST_001"
    )
    .select(
        col("source.route_id"),
        col("source.route_short_name"),
        col("source.event_timestamp"),
        col("target.last_event_timestamp")
    )
)

# COMMAND ----------

run_cdc_pipeline(
    source_df=ordering_test_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_routes")
    .filter(col("route_id") == "CDC_TEST_001")
    .select(
        "route_id",
        "route_short_name",
        "is_active",
        "last_event_timestamp"
    )
)

# COMMAND ----------

from pyspark.sql import Row
from datetime import datetime, timezone

stale_event_data = [
    Row(
        route_id="CDC_TEST_001",
        route_short_name="STALE_OLDER_EVENT",
        route_long_name="Stale Older Route Update",
        agency_id="HMRL",
        route_type=1,
        route_color="654321",
        route_text_color="FFFFFF",
        route_sort_order=18,
        operation="UPDATE",
        event_timestamp=datetime(
            2026, 11, 19, 5, 5, 0,
            tzinfo=timezone.utc
        )
    )
]

stale_event_df = spark.createDataFrame(stale_event_data)

display(stale_event_df)

# COMMAND ----------

run_cdc_pipeline(
    source_df=stale_event_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_routes")
    .filter(col("route_id") == "CDC_TEST_001")
    .select(
        "route_id",
        "route_short_name",
        "route_long_name",
        "is_active",
        "last_event_timestamp"
    )
)

# COMMAND ----------


from pyspark.sql import Row
from datetime import datetime, timezone

stale_delete_events = [
    Row(
        route_id="CDC_TEST_001",
        route_short_name="OLD_DELETE",
        route_long_name="Stale Delete Event",
        agency_id="HMRL",
        route_type=1,
        route_color="000000",
        route_text_color="FFFFFF",
        route_sort_order=17,
        operation="DELETE",
        event_timestamp=datetime(
            2026, 11, 19, 5, 5, 0,
            tzinfo=timezone.utc
        )
    )
]

stale_delete_df = (
    spark.createDataFrame(stale_delete_events)
)

display(stale_delete_df)


# COMMAND ----------

run_cdc_pipeline(
    source_df=stale_delete_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_routes")
    .filter(col("route_id") == "CDC_TEST_001")
    .select(
        "route_id",
        "route_short_name",
        "is_active",
        "deleted_at",
        "last_event_timestamp"
    )
)

# COMMAND ----------

equal_timestamp_events = [
    Row(
        route_id="CDC_TEST_001",
        route_short_name="EQUAL_EVENT_A",
        route_long_name="Equal Timestamp Event A",
        agency_id="HMRL",
        route_type=1,
        route_color="111111",
        route_text_color="FFFFFF",
        route_sort_order=21,
        operation="UPDATE",
        event_timestamp=datetime(
            2026, 11, 19, 5, 15, 0,
            tzinfo=timezone.utc
        )
    ),
    Row(
        route_id="CDC_TEST_001",
        route_short_name="EQUAL_EVENT_B",
        route_long_name="Equal Timestamp Event B",
        agency_id="HMRL",
        route_type=1,
        route_color="222222",
        route_text_color="FFFFFF",
        route_sort_order=22,
        operation="UPDATE",
        event_timestamp=datetime(
            2026, 11, 19, 5, 15, 0,
            tzinfo=timezone.utc
        )
    )
]

equal_timestamp_df = spark.createDataFrame(equal_timestamp_events)

display(equal_timestamp_df)

# COMMAND ----------

run_cdc_pipeline(
    source_df=equal_timestamp_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------


from pyspark.sql import Row
from datetime import datetime, timezone

event_id_test_data = [
    Row(
        route_id="CDC_TEST_001",
        route_short_name="EVENT_SEQUENCE_101",
        route_long_name="Event Sequence Test 101",
        agency_id="HMRL",
        route_type=1,
        route_color="111111",
        route_text_color="FFFFFF",
        route_sort_order=21,
        operation="UPDATE",
        event_timestamp=datetime(
            2026, 11, 19, 5, 15, 0,
            tzinfo=timezone.utc
        ),
        source_event_id="HMRL-EVENT-101",
        source_sequence=101
    ),
    Row(
        route_id="CDC_TEST_001",
        route_short_name="EVENT_SEQUENCE_102",
        route_long_name="Event Sequence Test 102",
        agency_id="HMRL",
        route_type=1,
        route_color="222222",
        route_text_color="FFFFFF",
        route_sort_order=22,
        operation="UPDATE",
        event_timestamp=datetime(
            2026, 11, 19, 5, 15, 0,
            tzinfo=timezone.utc
        ),
        source_event_id="HMRL-EVENT-102",
        source_sequence=102
    )
]

event_id_test_df = spark.createDataFrame(event_id_test_data)

display(event_id_test_df)


# COMMAND ----------

spark.sql("""
    ALTER TABLE anvesh.default.demo_cdc_routes
    ADD COLUMNS (
        last_source_sequence BIGINT
    )
""")

print("Source sequence column added successfully")

# COMMAND ----------

spark.table(
    "anvesh.default.demo_cdc_routes"
).printSchema()

# COMMAND ----------

spark.sql("""
    UPDATE anvesh.default.demo_cdc_routes
    SET last_source_sequence = 0
    WHERE last_source_sequence IS NULL
""")

print("Existing route sequences initialized")

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_cdc_routes")
    .select(
        "route_id",
        "last_event_timestamp",
        "last_source_sequence"
    )
)

# COMMAND ----------

run_cdc_pipeline(
    source_df=event_id_test_df,
    target_table="anvesh.default.demo_cdc_routes",
    audit_table="anvesh.default.demo_cdc_run_audit"
)

# COMMAND ----------

