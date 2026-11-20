# Databricks notebook source
# INDIA URBAN MOBILITY PLATFORM

# DELTA LAKE - INCREMENTAL DATA PROCESSING

## Objective
Implement incremental data processing using Delta Lake.

## Topics
1. Append vs Overwrite
2. Incremental Data Ingestion
3. MERGE Operations (Upsert)
4. Idempotency
5. Schema Evolution
6. Delta Lake Time Travel

## Catalog
anvesh.default

## Source Tables
Bronze Delta Tables

# COMMAND ----------

bronze_routes_df = spark.table("anvesh.default.bronze_routes")

print("Current Bronze Routes Count:")
print(bronze_routes_df.count())

display(bronze_routes_df)

# COMMAND ----------

from pyspark.sql import Row

test_routes = [
    Row(route_id="TEST_01", route_short_name="TEST_RED"),
    Row(route_id="TEST_02", route_short_name="TEST_BLUE")
]

test_routes_df = spark.createDataFrame(test_routes)

display(test_routes_df)

# COMMAND ----------

(
    test_routes_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.test_delta_routes")
)

print("Append operation completed successfully!")

# COMMAND ----------

test_table = spark.table("anvesh.default.test_delta_routes")

print("Total records:", test_table.count())

display(test_table)

# COMMAND ----------

second_batch = [
    Row(route_id="TEST_03", route_short_name="TEST_GREEN"),
    Row(route_id="TEST_04", route_short_name="TEST_YELLOW")
]

second_batch_df = spark.createDataFrame(second_batch)

(
    second_batch_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.test_delta_routes")
)

print("Second batch appended successfully!")

# COMMAND ----------

test_table = spark.table("anvesh.default.test_delta_routes")

print("Total records:", test_table.count())

display(test_table)

# COMMAND ----------

overwrite_batch = [
    Row(route_id="NEW_01", route_short_name="NEW_RED"),
    Row(route_id="NEW_02", route_short_name="NEW_BLUE")
]

overwrite_batch_df = spark.createDataFrame(overwrite_batch)

(
    overwrite_batch_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("anvesh.default.test_delta_routes")
)

print("Overwrite operation completed!")

# COMMAND ----------

test_table = spark.table("anvesh.default.test_delta_routes")

print("Total records:", test_table.count())

display(test_table)

# COMMAND ----------

from pyspark.sql import Row

incremental_batch = [
    Row(route_id="INC_01", route_short_name="INC_RED"),
    Row(route_id="INC_02", route_short_name="INC_BLUE")
]

incremental_df = spark.createDataFrame(incremental_batch)

print("Incoming incremental records:", incremental_df.count())

# COMMAND ----------

(
    incremental_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.test_delta_routes")
)

print("Incremental batch loaded successfully!")

# COMMAND ----------

final_df = spark.table("anvesh.default.test_delta_routes")

print("Total records:", final_df.count())

display(final_df)

# COMMAND ----------

display(
    spark.table("anvesh.default.test_delta_routes")
    .orderBy("route_id")
)

# COMMAND ----------

(
    incremental_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.test_delta_routes")
)

print("Same batch processed again!")

# COMMAND ----------

duplicate_test_df = spark.table("anvesh.default.test_delta_routes")

print("Total records:", duplicate_test_df.count())

print(
    "Duplicate route IDs:",
    duplicate_test_df.count()
    - duplicate_test_df.select("route_id").distinct().count()
)

display(duplicate_test_df.orderBy("route_id"))

# COMMAND ----------

merge_batch = [
    Row(route_id="INC_01", route_short_name="UPDATED_RED"),
    Row(route_id="INC_03", route_short_name="INC_GREEN")
]

merge_df = spark.createDataFrame(merge_batch)

display(merge_df)

# COMMAND ----------

merge_df.createOrReplaceTempView("incoming_routes")

spark.sql("""
    MERGE INTO anvesh.default.test_delta_routes AS target
    USING incoming_routes AS source
    ON target.route_id = source.route_id

    WHEN MATCHED THEN
        UPDATE SET
            target.route_short_name = source.route_short_name

    WHEN NOT MATCHED THEN
        INSERT (
            route_id,
            route_short_name
        )
        VALUES (
            source.route_id,
            source.route_short_name
        )
""")

print("Delta MERGE completed successfully!")

# COMMAND ----------

final_merge_df = spark.table("anvesh.default.test_delta_routes")

print("Total records:", final_merge_df.count())

display(final_merge_df.orderBy("route_id"))

# COMMAND ----------

final_merge_df = spark.table("anvesh.default.test_delta_routes")

print("Total records:", final_merge_df.count())

print("INC_01 records:")
display(
    final_merge_df
    .filter("route_id = 'INC_01'")
)

print("INC_03 records:")
display(
    final_merge_df
    .filter("route_id = 'INC_03'")
)

# COMMAND ----------

display(
    spark.sql("""
        SELECT route_id, route_short_name, COUNT(*) AS record_count
        FROM anvesh.default.test_delta_routes
        WHERE route_id IN ('INC_01', 'INC_03')
        GROUP BY route_id, route_short_name
        ORDER BY route_id
    """)
)

# COMMAND ----------

spark.sql("""
    SELECT
        route_id,
        COUNT(*) AS occurrences
    FROM anvesh.default.test_delta_routes
    GROUP BY route_id
    HAVING COUNT(*) > 1
    ORDER BY route_id
""").show()

# COMMAND ----------

spark.sql("""
    SELECT
        route_id,
        COUNT(*) AS occurrences
    FROM anvesh.default.test_delta_routes
    GROUP BY route_id
    HAVING COUNT(*) > 1
    ORDER BY route_id
""").show(100, truncate=False)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

routes_df = spark.table("anvesh.default.test_delta_routes")

window_spec = (
    Window
    .partitionBy("route_id")
    .orderBy(
        col("route_short_name").desc()
    )
)

deduplicated_df = (
    routes_df
    .withColumn("_row_num", row_number().over(window_spec))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

(
    deduplicated_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("anvesh.default.test_delta_routes")
)

print("Duplicate cleanup completed!")
print("Records after cleanup:", spark.table(
    "anvesh.default.test_delta_routes"
).count())

# COMMAND ----------

spark.sql("""
    MERGE INTO anvesh.default.test_delta_routes AS target
    USING incoming_routes AS source
    ON target.route_id = source.route_id

    WHEN MATCHED THEN
        UPDATE SET
            target.route_short_name = source.route_short_name

    WHEN NOT MATCHED THEN
        INSERT (
            route_id,
            route_short_name
        )
        VALUES (
            source.route_id,
            source.route_short_name
        )
""")

print("Second MERGE execution completed!")

print(
    "Total records:",
    spark.table("anvesh.default.test_delta_routes").count()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY anvesh.default.test_delta_routes;

# COMMAND ----------

version_5_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 5)
    .table("anvesh.default.test_delta_routes")
)

print("Records in Version 5:", version_5_df.count())

display(version_5_df.orderBy("route_id"))

# COMMAND ----------

current_df = spark.table("anvesh.default.test_delta_routes")

print("Version 5 records:", version_5_df.count())
print("Current version records:", current_df.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE anvesh.default.test_delta_routes
# MAGIC TO VERSION AS OF 5;

# COMMAND ----------

restored_df = spark.table("anvesh.default.test_delta_routes")

print("Records after restore:", restored_df.count())

display(restored_df.orderBy("route_id"))

# COMMAND ----------

from pyspark.sql import Row

new_routes = [
    Row(
        route_id="GREEN",
        route_short_name="Green Line",
        route_status="ACTIVE"
    ),
    Row(
        route_id="YELLOW",
        route_short_name="Yellow Line",
        route_status="ACTIVE"
    )
]

new_routes_df = spark.createDataFrame(new_routes)

display(new_routes_df)

# COMMAND ----------

existing_df = spark.table("anvesh.default.test_delta_routes")

print("EXISTING DELTA TABLE SCHEMA")
existing_df.printSchema()

print("INCOMING DATAFRAME SCHEMA")
new_routes_df.printSchema()

# COMMAND ----------

(
    new_routes_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("anvesh.default.test_delta_routes")
)

print("Schema evolution completed successfully!")

# COMMAND ----------

updated_df = spark.table("anvesh.default.test_delta_routes")

print("UPDATED TABLE SCHEMA")
updated_df.printSchema()

print("TOTAL RECORDS:", updated_df.count())

display(updated_df.orderBy("route_id"))

# COMMAND ----------

updated_df = spark.table("anvesh.default.test_delta_routes")

print("Total records:", updated_df.count())

display(
    updated_df.orderBy("route_id")
)

# COMMAND ----------

from pyspark.sql.functions import col

updated_df = spark.table("anvesh.default.test_delta_routes")

print("Records with NULL route_status:")

display(
    updated_df
    .filter(col("route_status").isNull())
)

print("Records with ACTIVE status:")

display(
    updated_df
    .filter(col("route_status") == "ACTIVE")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE anvesh.default.test_delta_routes;

# COMMAND ----------

table_details = spark.sql("""
    DESCRIBE DETAIL anvesh.default.test_delta_routes
""")

display(
    table_details.select(
        "format",
        "numFiles",
        "sizeInBytes",
        "location"
    )
)

# COMMAND ----------

spark.sql("""
    OPTIMIZE anvesh.default.test_delta_routes
""")

print("OPTIMIZE completed successfully!")

# COMMAND ----------

display(
    spark.sql("""
        DESCRIBE DETAIL anvesh.default.test_delta_routes
    """).select("numFiles", "sizeInBytes")
)

# COMMAND ----------

spark.sql("""
    DESCRIBE HISTORY anvesh.default.test_delta_routes
""").select(
    "version",
    "timestamp",
    "operation"
).show(30, truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM anvesh.default.test_delta_routes;

# COMMAND ----------

spark.sql("""
    DESCRIBE HISTORY anvesh.default.test_delta_routes
""").select(
    "version",
    "operation",
    "timestamp"
).show(20, truncate=False)

# COMMAND ----------

final_table = spark.table("anvesh.default.test_delta_routes")

print("Current record count:", final_table.count())

display(final_table.orderBy("route_id"))

# COMMAND ----------

bronze_routes_df = spark.table("anvesh.default.bronze_routes")

print("Total records:", bronze_routes_df.count())

print("Table schema:")
bronze_routes_df.printSchema()

display(bronze_routes_df)

# COMMAND ----------

(
    bronze_routes_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("anvesh.default.demo_incremental_routes")
)

print("Incremental target created successfully!")

print(
    "Target records:",
    spark.table("anvesh.default.demo_incremental_routes").count()
)

# COMMAND ----------

target_routes_df = spark.table(
    "anvesh.default.demo_incremental_routes"
)

display(
    target_routes_df.select(
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_color",
        "_ingestion_run_id"
    ).orderBy("route_id")
)

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

# Existing BLUE route with updated details
updated_blue = (
    target_routes_df
    .filter(col("route_id") == "BLUE")
    .withColumn(
        "route_short_name",
        lit("C3_BLUE_UPDATED_SIMULATED")
    )
    .withColumn(
        "_ingestion_run_id",
        lit("SIMULATED_RUN_001")
    )
    .withColumn(
        "_ingestion_timestamp",
        current_timestamp()
    )
)

# Create a simulated new route using an existing row's schema
new_yellow = (
    target_routes_df
    .filter(col("route_id") == "RED")
    .withColumn("route_id", lit("TEST_YELLOW"))
    .withColumn("route_short_name", lit("TEST_YELLOW"))
    .withColumn("route_long_name", lit("SIMULATED TEST ROUTE"))
    .withColumn("route_color", lit("FFFF00"))
    .withColumn("route_text_color", lit("000000"))
    .withColumn("route_sort_order", lit(4))
    .withColumn("_ingestion_run_id", lit("SIMULATED_RUN_001"))
    .withColumn("_ingestion_timestamp", current_timestamp())
)

# Combine both records
incoming_routes_df = updated_blue.unionByName(new_yellow)

print("Incoming records:", incoming_routes_df.count())

display(incoming_routes_df)

# COMMAND ----------

incoming_routes_df.createOrReplaceTempView(
    "incoming_routes"
)

spark.sql("""
    MERGE INTO anvesh.default.demo_incremental_routes AS target

    USING incoming_routes AS source

    ON target.route_id = source.route_id

    WHEN MATCHED THEN
        UPDATE SET *

    WHEN NOT MATCHED THEN
        INSERT *
""")

print("Incremental MERGE completed successfully!")

# COMMAND ----------

updated_target_df = spark.table(
    "anvesh.default.demo_incremental_routes"
)

print("Total target records:", updated_target_df.count())

display(
    updated_target_df.select(
        "route_id",
        "route_short_name",
        "route_long_name",
        "_ingestion_run_id"
    ).orderBy("route_id")
)

# COMMAND ----------

spark.sql("""
    SELECT
        route_id,
        route_short_name,
        _ingestion_run_id
    FROM anvesh.default.demo_incremental_routes
    WHERE route_id IN ('BLUE', 'TEST_YELLOW')
    ORDER BY route_id
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
    MERGE INTO anvesh.default.demo_incremental_routes AS target
    USING incoming_routes AS source
    ON target.route_id = source.route_id

    WHEN MATCHED THEN
        UPDATE SET *

    WHEN NOT MATCHED THEN
        INSERT *
""")

print("Second incremental MERGE completed!")

# COMMAND ----------

target_df = spark.table(
    "anvesh.default.demo_incremental_routes"
)

print("Total records:", target_df.count())
print("Unique route IDs:", target_df.select("route_id").distinct().count())

# COMMAND ----------

import uuid
from datetime import datetime, timezone

pipeline_run_id = str(uuid.uuid4())

pipeline_started_at = datetime.now(timezone.utc)

pipeline_name = "hyderabad_metro_incremental_routes"

print("Pipeline Name:", pipeline_name)
print("Pipeline Run ID:", pipeline_run_id)
print("Started At:", pipeline_started_at)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS anvesh.default.demo_incremental_routes_audit (
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
# MAGIC DESCRIBE TABLE anvesh.default.demo_incremental_routes_audit;

# COMMAND ----------

source_record_count = incoming_routes_df.count()

target_record_count = (
    spark.table("anvesh.default.demo_incremental_routes")
    .count()
)

print("Source records:", source_record_count)
print("Target records:", target_record_count)
print("Pipeline run ID:", pipeline_run_id)

# COMMAND ----------

spark.sql("""
    MERGE INTO anvesh.default.demo_incremental_routes AS target
    USING incoming_routes AS source
    ON target.route_id = source.route_id

    WHEN MATCHED THEN
        UPDATE SET *

    WHEN NOT MATCHED THEN
        INSERT *
""")

print("Audited MERGE execution completed!")

# COMMAND ----------

merge_history_df = spark.sql("""
    DESCRIBE HISTORY anvesh.default.demo_incremental_routes
""")

latest_merge = (
    merge_history_df
    .filter("operation = 'MERGE'")
    .orderBy("version", ascending=False)
    .first()
)

print("MERGE Version:", latest_merge["version"])
print("Operation Metrics:")

metrics = latest_merge["operationMetrics"]

print("Inserted:", metrics.get("numTargetRowsInserted"))
print("Updated:", metrics.get("numTargetRowsUpdated"))
print("Source Rows:", metrics.get("numSourceRows"))

# COMMAND ----------

from datetime import datetime, timezone

pipeline_completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
pipeline_started_at_naive = pipeline_started_at.replace(tzinfo=None)

inserted_count = int(metrics.get("numTargetRowsInserted", 0))
updated_count = int(metrics.get("numTargetRowsUpdated", 0))

audit_record = [(
    pipeline_run_id,
    pipeline_name,
    "anvesh.default.bronze_routes",
    "anvesh.default.demo_incremental_routes",
    source_record_count,
    inserted_count,
    updated_count,
    target_record_count,
    "SUCCESS",
    pipeline_started_at_naive,
    pipeline_completed_at,
    None
)]

print("Audit record prepared successfully!")
print(audit_record)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, TimestampType
)

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

audit_df = spark.createDataFrame(
    audit_record,
    schema=audit_schema
)

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.demo_incremental_routes_audit")
)

print("Audit record saved successfully!")

# COMMAND ----------

display(
    spark.table("anvesh.default.demo_incremental_routes_audit")
    .filter(
        col("pipeline_run_id") == pipeline_run_id
    )
)

# COMMAND ----------

from delta.tables import DeltaTable

def execute_incremental_merge(
    source_df,
    target_table,
    key_columns
):

    # 1. Validate incoming data
    if source_df.limit(1).count() == 0:
        raise ValueError("Incoming source DataFrame is empty.")

    # 2. Check duplicate business keys in source
    duplicate_count = (
        source_df
        .groupBy(*key_columns)
        .count()
        .filter("count > 1")
        .count()
    )

    if duplicate_count > 0:
        raise ValueError(
            f"Source contains {duplicate_count} duplicate key groups."
        )

    # 3. Check duplicate business keys in target
    target_df = spark.table(target_table)

    target_duplicate_count = (
        target_df
        .groupBy(*key_columns)
        .count()
        .filter("count > 1")
        .count()
    )

    if target_duplicate_count > 0:
        raise ValueError(
            f"Target contains {target_duplicate_count} "
            "duplicate key groups. Clean target before MERGE."
        )

    # 4. Build merge condition
    merge_condition = " AND ".join(
        [f"target.{key} = source.{key}" for key in key_columns]
    )

    # 5. Execute Delta MERGE
    delta_target = DeltaTable.forName(spark, target_table)

    (
        delta_target.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Incremental MERGE completed successfully!")


print("Updated incremental merge function defined successfully!")

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

target_routes_df = spark.table(
    "anvesh.default.demo_incremental_routes"
)

# Updated existing BLUE route
updated_blue = (
    target_routes_df
    .filter(col("route_id") == "BLUE")
    .withColumn(
        "route_short_name",
        lit("C3_BLUE_UPDATED_SIMULATED")
    )
    .withColumn(
        "_ingestion_run_id",
        lit("SIMULATED_RUN_001")
    )
    .withColumn(
        "_ingestion_timestamp",
        current_timestamp()
    )
)

# New simulated route
new_yellow = (
    target_routes_df
    .filter(col("route_id") == "RED")
    .withColumn("route_id", lit("TEST_YELLOW"))
    .withColumn("route_short_name", lit("TEST_YELLOW"))
    .withColumn("route_long_name", lit("SIMULATED TEST ROUTE"))
    .withColumn("route_color", lit("FFFF00"))
    .withColumn("route_text_color", lit("000000"))
    .withColumn("route_sort_order", lit(4))
    .withColumn("_ingestion_run_id", lit("SIMULATED_RUN_001"))
    .withColumn("_ingestion_timestamp", current_timestamp())
)

incoming_routes_df = updated_blue.unionByName(new_yellow)

print("Incoming records:", incoming_routes_df.count())

# COMMAND ----------

execute_incremental_merge(
    source_df=incoming_routes_df,
    target_table="anvesh.default.demo_incremental_routes",
    key_columns=["route_id"]
)

# COMMAND ----------


import uuid
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)


def execute_incremental_merge(
    source_df,
    source_name,
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

    # 2. Capture target count BEFORE source validation
    target_df = spark.table(target_table)
    target_record_count = target_df.count()

    try:

        # 3. Validate source
        source_record_count = source_df.count()

        if source_record_count == 0:
            raise ValueError("Incoming source DataFrame is empty.")

        # 4. Validate source duplicate keys
        source_duplicates = (
            source_df
            .groupBy(*key_columns)
            .count()
            .filter("count > 1")
            .count()
        )

        if source_duplicates > 0:
            raise ValueError(
                f"Source contains {source_duplicates} duplicate key groups."
            )

        # 5. Validate target duplicate keys
        target_duplicates = (
            target_df
            .groupBy(*key_columns)
            .count()
            .filter("count > 1")
            .count()
        )

        if target_duplicates > 0:
            raise ValueError(
                f"Target contains {target_duplicates} "
                "duplicate key groups."
            )

        # 6. Build dynamic MERGE condition
        merge_condition = " AND ".join(
            [f"target.{key} = source.{key}" for key in key_columns]
        )

        # 7. Execute Delta MERGE
        delta_target = DeltaTable.forName(spark, target_table)

        (
            delta_target.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        # 8. Capture actual Delta MERGE metrics
        latest_merge = (
            spark.sql(f"DESCRIBE HISTORY {target_table}")
            .filter("operation = 'MERGE'")
            .orderBy("version", ascending=False)
            .first()
        )

        metrics = latest_merge["operationMetrics"]

        inserted_count = int(
            metrics.get("numTargetRowsInserted", 0)
        )

        updated_count = int(
            metrics.get("numTargetRowsUpdated", 0)
        )

        # 9. Get final target count after successful MERGE
        target_record_count = spark.table(target_table).count()

        status = "SUCCESS"

        print("Incremental MERGE completed successfully!")

    except Exception as e:

        error_message = str(e)[:4000]
        status = "FAILED"

        print("Incremental pipeline failed!")
        print("Error:", error_message)

    finally:

        # 10. Prepare audit record
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
            source_name,
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
            f"Incremental pipeline failed. Run ID: {pipeline_run_id}. "
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


print("Updated incremental pipeline function defined successfully!")


# COMMAND ----------

try:
    execute_incremental_merge(
        source_df=duplicate_source_df,
        source_name="duplicate_test_batch",
        target_table="anvesh.default.demo_incremental_routes",
        key_columns=["route_id"],
        pipeline_name="hyderabad_metro_incremental_routes",
        audit_table="anvesh.default.demo_incremental_routes_audit"
    )

except RuntimeError as e:
    print("Expected pipeline failure captured:")
    print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_run_id,
# MAGIC     source_table,
# MAGIC     source_record_count,
# MAGIC     inserted_count,
# MAGIC     updated_count,
# MAGIC     target_record_count,
# MAGIC     status,
# MAGIC     error_message
# MAGIC FROM anvesh.default.demo_incremental_routes_audit
# MAGIC WHERE pipeline_run_id = '91b915cc-7527-4e39-992d-bbc656cfced2';

# COMMAND ----------


pipeline_result = execute_incremental_merge(
    source_df=incoming_routes_df,
    source_name="simulated_incremental_batch",
    target_table="anvesh.default.demo_incremental_routes",
    key_columns=["route_id"],
    pipeline_name="hyderabad_metro_incremental_routes",
    audit_table="anvesh.default.demo_incremental_routes_audit"
)

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
# MAGIC FROM anvesh.default.demo_incremental_routes_audit
# MAGIC WHERE pipeline_run_id = 'ea04f18c-8981-498e-aa15-77bc3ae7651e';

# COMMAND ----------

pipeline_result = execute_incremental_merge(
    source_df=incoming_routes_df,
    source_name="simulated_incremental_batch",
    target_table="anvesh.default.demo_incremental_routes",
    key_columns=["route_id"],
    pipeline_name="hyderabad_metro_incremental_routes",
    audit_table="anvesh.default.demo_incremental_routes_audit"
)

print(pipeline_result)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_records,
# MAGIC     COUNT(DISTINCT route_id) AS unique_route_ids
# MAGIC FROM anvesh.default.demo_incremental_routes;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_run_id,
# MAGIC     source_table,
# MAGIC     source_record_count,
# MAGIC     inserted_count,
# MAGIC     updated_count,
# MAGIC     target_record_count,
# MAGIC     status,
# MAGIC     error_message
# MAGIC FROM anvesh.default.demo_incremental_routes_audit
# MAGIC ORDER BY started_at DESC
# MAGIC LIMIT 10;

# COMMAND ----------

