# Databricks notebook source
# # Advanced PySpark – Window Functions

# ## Project
# India Urban Mobility & Traffic Intelligence Platform

# ## Objective
# Master PySpark window functions using real Hyderabad Metro datasets.

# ## Topics
# 1. Window Functions Fundamentals
# 2. row_number(), rank(), dense_rank()
# 3. lag() and lead()
# 4. Running Totals
# 5. Moving Averages
# 6. Advanced Window Operations
# 7. Practical Project Implementation

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load Gold fact table
df = spark.table("anvesh.default.gold_fact_stop_schedule")

# Display total records
print("Total records:", df.count())

# Display schema
df.printSchema()

# Display sample records
display(df.limit(10))

# COMMAND ----------

# Calculate total scheduled stops for each trip
trip_stop_counts = (
    df
    .groupBy("route_id", "trip_id")
    .agg(
        F.count("stop_id").alias("total_stops")
    )
)

display(trip_stop_counts.orderBy("route_id", F.desc("total_stops")))

# COMMAND ----------

trip_stop_counts = (
    df
    .groupBy("route_id", "trip_id")
    .agg(
        F.count("stop_id").alias("total_stops")
    )
)

print("Total trips:", trip_stop_counts.count())

display(
    trip_stop_counts.orderBy(
        "route_id",
        F.desc("total_stops")
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("route_id")
    .orderBy(F.desc("total_stops"))
)

ranked_trips = (
    trip_stop_counts
    .withColumn(
        "trip_rank",
        F.row_number().over(window_spec)
    )
)

display(
    ranked_trips.orderBy("route_id", "trip_rank")
)

# COMMAND ----------

top_5_trips = (
    ranked_trips
    .filter(F.col("trip_rank") <= 5)
    .orderBy("route_id", "trip_rank")
)

display(top_5_trips)

# COMMAND ----------

top_5_trips = (
    ranked_trips
    .filter(F.col("trip_rank") <= 5)
    .orderBy("route_id", "trip_rank")
)

display(top_5_trips)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

dense_window = (
    Window
    .partitionBy("route_id")
    .orderBy(F.desc("total_stops"))
)

dense_ranked_trips = (
    trip_stop_counts
    .withColumn(
        "dense_rank",
        F.dense_rank().over(dense_window)
    )
)

display(
    dense_ranked_trips
    .orderBy("route_id", "dense_rank")
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

stop_window = (
    Window
    .partitionBy("trip_id")
    .orderBy("stop_sequence")
)

trip_stop_comparison = (
    spark.table("anvesh.default.silver_stop_times")
    .withColumn(
        "previous_stop",
        F.lag("stop_id", 1).over(stop_window)
    )
    .withColumn(
        "next_stop",
        F.lead("stop_id", 1).over(stop_window)
    )
)

display(
    trip_stop_comparison
    .filter(F.col("trip_id") == "SA_118662")
    .orderBy("stop_sequence")
)

# COMMAND ----------

display(
    trip_stop_comparison
    .filter(F.col("trip_id") == "SA_118662")
    .select(
        "trip_id",
        "stop_sequence",
        "stop_id",
        "previous_stop",
        "next_stop"
    )
    .orderBy("stop_sequence")
)

# COMMAND ----------

running_window = (
    Window
    .partitionBy("trip_id")
    .orderBy("stop_sequence")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow
    )
)

running_stop_count = (
    spark.table("anvesh.default.silver_stop_times")
    .withColumn(
        "cumulative_stops",
        F.count("*").over(running_window)
    )
)

display(
    running_stop_count
    .filter(F.col("trip_id") == "SA_118662")
    .select(
        "trip_id",
        "stop_sequence",
        "stop_id",
        "cumulative_stops"
    )
    .orderBy("stop_sequence")
)

# COMMAND ----------

moving_window = (
    Window
    .partitionBy("trip_id")
    .orderBy("stop_sequence")
    .rowsBetween(-2, 0)
)

moving_average_df = (
    spark.table("anvesh.default.silver_stop_times")
    .withColumn(
        "avg_distance_last_3_stops",
        F.round(
            F.avg("shape_dist_traveled").over(moving_window),
            2
        )
    )
)

display(
    moving_average_df
    .filter(F.col("trip_id") == "SA_118662")
    .select(
        "trip_id",
        "stop_sequence",
        "stop_id",
        "shape_dist_traveled",
        "avg_distance_last_3_stops"
    )
    .orderBy("stop_sequence")
)

# COMMAND ----------

distance_window = (
    Window
    .partitionBy("trip_id")
    .orderBy("stop_sequence")
)

stop_distance_df = (
    spark.table("anvesh.default.silver_stop_times")
    .withColumn(
        "previous_distance",
        F.lag("shape_dist_traveled").over(distance_window)
    )
    .withColumn(
        "distance_from_previous_stop",
        F.col("shape_dist_traveled") - F.col("previous_distance")
    )
)

display(
    stop_distance_df
    .filter(F.col("trip_id") == "SA_118662")
    .select(
        "trip_id",
        "stop_sequence",
        "stop_id",
        "shape_dist_traveled",
        "previous_distance",
        "distance_from_previous_stop"
    )
    .orderBy("stop_sequence")
)

# COMMAND ----------

stop_times_df = spark.table("anvesh.default.silver_stop_times")
trips_df = spark.table("anvesh.default.silver_trips")

stop_times_with_route = (
    stop_times_df
    .join(
        trips_df.select("trip_id", "route_id"),
        on="trip_id",
        how="inner"
    )
)

display(
    stop_times_with_route.select(
        "trip_id",
        "route_id",
        "stop_sequence",
        "stop_id",
        "shape_dist_traveled"
    )
)

# COMMAND ----------

print("Original stop times:", stop_times_df.count())
print("Joined stop times:", stop_times_with_route.count())
print("Distinct routes:", stop_times_with_route.select("route_id").distinct().count())

# COMMAND ----------

distance_window = (
    Window
    .partitionBy("trip_id")
    .orderBy("stop_sequence")
)

route_distances = (
    stop_times_with_route
    .withColumn(
        "previous_distance",
        F.lag("shape_dist_traveled").over(distance_window)
    )
    .withColumn(
        "distance_between_stops",
        F.col("shape_dist_traveled") - F.col("previous_distance")
    )
    .filter(F.col("previous_distance").isNotNull())
)

display(
    route_distances.select(
        "route_id",
        "trip_id",
        "stop_sequence",
        "stop_id",
        "distance_between_stops"
    )
)

# COMMAND ----------

print("Total consecutive-stop records:", route_distances.count())
print("Null distance differences:",
      route_distances.filter(
          F.col("distance_between_stops").isNull()
      ).count())

# COMMAND ----------

route_rank_window = (
    Window
    .partitionBy("route_id")
    .orderBy(F.desc("distance_between_stops"))
)

ranked_distances = (
    route_distances
    .withColumn(
        "distance_rank",
        F.row_number().over(route_rank_window)
    )
)

top_3_distances = (
    ranked_distances
    .filter(F.col("distance_rank") <= 3)
    .select(
        "route_id",
        "trip_id",
        "stop_sequence",
        "stop_id",
        "distance_between_stops",
        "distance_rank"
    )
    .orderBy("route_id", "distance_rank")
)

display(top_3_distances)

# COMMAND ----------

from datetime import datetime, timezone
import uuid

PIPELINE_NAME = "india_urban_mobility_pipeline"
PIPELINE_RUN_ID = str(uuid.uuid4())

WORKSPACE_FOLDER = "/Users/anveshp222002@gmail.com"

NOTEBOOK_PATHS = {
    "bronze_ingestion": f"{WORKSPACE_FOLDER}/00_bronze_ingestion",
    "bronze_quality": f"{WORKSPACE_FOLDER}/04_bronze_quality_validation",
    "bronze_to_silver": f"{WORKSPACE_FOLDER}/01_bronze_to_silver_stops",
    "silver_to_gold": f"{WORKSPACE_FOLDER}/02_silver_to_gold",
    "gold_quality": f"{WORKSPACE_FOLDER}/03_gold_quality_validation"
}

print("Pipeline:", PIPELINE_NAME)
print("Run ID:", PIPELINE_RUN_ID)
print("Configuration initialized:", len(NOTEBOOK_PATHS), "notebooks")

# COMMAND ----------


# ============================================
# NOTEBOOK EXECUTION ENGINE
# ============================================

import time
from datetime import datetime, timezone

TIMEOUT_SECONDS = 3600

pipeline_results = []


def execute_notebook(task_name, notebook_path):

    print("\n" + "=" * 60)
    print(f"STARTING TASK: {task_name}")
    print(f"NOTEBOOK: {notebook_path}")
    print("=" * 60)

    started_at = datetime.now(timezone.utc)
    start_time = time.time()

    try:
        result = dbutils.notebook.run(
            notebook_path,
            TIMEOUT_SECONDS,
            {
                "pipeline_run_id": PIPELINE_RUN_ID,
                "pipeline_name": PIPELINE_NAME
            }
        )

        execution_time = round(time.time() - start_time, 2)

        task_result = {
            "task_name": task_name,
            "status": "SUCCESS",
            "execution_time_seconds": execution_time,
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc),
            "result": result,
            "error": None
        }

        pipeline_results.append(task_result)

        print(f"TASK COMPLETED: {task_name}")
        print(f"Execution time: {execution_time} seconds")
        print(f"Notebook result: {result}")

        return result

    except Exception as e:

        execution_time = round(time.time() - start_time, 2)

        error_message = str(e)

        task_result = {
            "task_name": task_name,
            "status": "FAILED",
            "execution_time_seconds": execution_time,
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc),
            "result": None,
            "error": error_message
        }

        pipeline_results.append(task_result)

        print("\nTASK FAILED:", task_name)
        print("Execution time:", execution_time)
        print("\nFULL ERROR DETAILS:")
        print(repr(e))

        raise



# COMMAND ----------

assert PIPELINE_RUN_ID is not None
assert len(NOTEBOOK_PATHS) == 5

print("Pipeline configuration: PASS")
print("Run ID:", PIPELINE_RUN_ID)

# COMMAND ----------


PIPELINE_TASKS = [
    ("bronze_ingestion", NOTEBOOK_PATHS["bronze_ingestion"]),
    ("bronze_quality", NOTEBOOK_PATHS["bronze_quality"]),
    ("bronze_to_silver", NOTEBOOK_PATHS["bronze_to_silver"]),
    ("silver_to_gold", NOTEBOOK_PATHS["silver_to_gold"]),
    ("gold_quality", NOTEBOOK_PATHS["gold_quality"])
]




def run_pipeline():

    print("=" * 60)
    print("STARTING INDIA URBAN MOBILITY PIPELINE")
    print("Pipeline Run ID:", PIPELINE_RUN_ID)
    print("=" * 60)

    pipeline_results.clear()

    bronze_ingestion_run_id = None

    for task_name, notebook_path in PIPELINE_TASKS:

        result = execute_notebook(task_name, notebook_path)

        if result is False:
            print(f"PIPELINE STOPPED: {task_name}")
            return "FAILED"

        # Capture successful Bronze ingestion run
        if task_name == "bronze_ingestion":

            audit_df = spark.table(
                "anvesh.default.bronze_ingestion_audit"
            )

            latest_run = (
                audit_df
                .groupBy("run_id")
                .agg(
                    F.count("*").alias("dataset_count"),
                    F.sum("record_count").alias("total_records"),
                    F.countDistinct("status").alias("status_count"),
                    F.min("status").alias("run_status"),
                    F.max("completed_at").alias("completed_at")
                )
                .filter(
                    (F.col("dataset_count") == 10) &
                    (F.col("total_records") == 70478) &
                    (F.col("status_count") == 1) &
                    (F.col("run_status") == "SUCCESS")
                )
                .orderBy(F.col("completed_at").desc())
                .first()
            )

            if latest_run is None:
                raise Exception("Bronze ingestion verification failed.")

            bronze_ingestion_run_id = latest_run["run_id"]

            print("Bronze ingestion verified:", bronze_ingestion_run_id)

        # Bronze quality gate
        if task_name == "bronze_quality":

            if bronze_ingestion_run_id is None:
                raise Exception("Bronze ingestion Run ID missing.")

            validate_bronze_quality(bronze_ingestion_run_id)

            print("BRONZE QUALITY GATE PASSED")

        # Silver quality gate
        if task_name == "bronze_to_silver":

            validate_silver_quality()

            print("SILVER QUALITY GATE PASSED")

    print("\nPIPELINE TASKS COMPLETED")
    return "SUCCESS"


# COMMAND ----------

# Inspect Bronze quality report

table_name = "anvesh.default.bronze_quality_report"

if spark.catalog.tableExists(table_name):

    quality_df = spark.table(table_name)

    print("Bronze quality report exists.")
    print("\nSchema:")
    quality_df.printSchema()

    print("\nTotal report records:", quality_df.count())

    display(quality_df)

else:
    print("Bronze quality report table not found.")

# COMMAND ----------

history_table = "anvesh.default.bronze_quality_history"

if spark.catalog.tableExists(history_table):

    history_df = spark.table(history_table)

    print("Bronze quality history exists.")
    history_df.printSchema()

    display(history_df)

else:
    print("Bronze quality history table not found.")

# COMMAND ----------

from pyspark.sql import functions as F

history_df = spark.table(
    "anvesh.default.bronze_quality_history"
)

print("Total validation records:", history_df.count())

display(
    history_df
    .orderBy(F.col("validation_timestamp").desc())
    .limit(20)
)

# COMMAND ----------

display(
    history_df
    .groupBy("validation_run_id", "source_ingestion_run_id", "status")
    .agg(
        F.count("*").alias("validation_checks"),
        F.sum("failed_records").alias("total_failed_records"),
        F.max("validation_timestamp").alias("validation_timestamp")
    )
    .orderBy(F.col("validation_timestamp").desc())
)

# COMMAND ----------

from pyspark.sql import functions as F

def validate_bronze_quality(ingestion_run_id):

    history_table = "anvesh.default.bronze_quality_history"

    history_df = spark.table(history_table)

    latest_validation = (
        history_df
        .filter(
            F.col("source_ingestion_run_id") == ingestion_run_id
        )
        .groupBy("validation_run_id")
        .agg(
            F.count("*").alias("total_checks"),
            F.sum(
                F.when(F.col("status") == "PASS", 1).otherwise(0)
            ).alias("passed_checks"),
            F.sum(
                F.when(F.col("status") != "PASS", 1).otherwise(0)
            ).alias("failed_checks"),
            F.sum("failed_records").alias("failed_records"),
            F.max("validation_timestamp").alias("validated_at")
        )
        .orderBy(F.col("validated_at").desc())
        .limit(1)
    )

    results = latest_validation.collect()

    if not results:
        raise Exception(
            f"No Bronze quality results found for ingestion run {ingestion_run_id}"
        )

    result = results[0]

    print("Validation run:", result["validation_run_id"])
    print("Total checks:", result["total_checks"])
    print("Passed checks:", result["passed_checks"])
    print("Failed checks:", result["failed_checks"])
    print("Failed records:", result["failed_records"])

    if (
        result["total_checks"] != 60
        or result["passed_checks"] != 60
        or result["failed_checks"] != 0
        or result["failed_records"] != 0
    ):
        raise Exception("BRONZE QUALITY GATE FAILED")

    print("BRONZE QUALITY GATE PASSED")

    return True

# COMMAND ----------

validate_bronze_quality(
    "c1af71c4-a397-4381-95db-8274de795a31"
)

# COMMAND ----------

# Inspect ingestion run ID variables

for variable_name, variable_value in globals().copy().items():
    if "run_id" in variable_name.lower():
        print(f"{variable_name} = {variable_value}")

# COMMAND ----------

from pyspark.sql import functions as F

audit_df = spark.table(
    "anvesh.default.bronze_ingestion_audit"
)

latest_ingestion_run = (
    audit_df
    .groupBy("run_id")
    .agg(
        F.count("*").alias("dataset_count"),
        F.sum("record_count").alias("total_records"),
        F.countDistinct("status").alias("status_count"),
        F.min("status").alias("run_status"),
        F.max("completed_at").alias("completed_at")
    )
    .filter(
        (F.col("dataset_count") == 10) &
        (F.col("total_records") == 70478) &
        (F.col("status_count") == 1) &
        (F.col("run_status") == "SUCCESS")
    )
    .orderBy(F.col("completed_at").desc())
    .first()
)

if latest_ingestion_run is None:
    raise Exception("No complete successful Bronze ingestion run found.")

BRONZE_INGESTION_RUN_ID = latest_ingestion_run["run_id"]

print("Bronze ingestion Run ID:", BRONZE_INGESTION_RUN_ID)
print("Datasets:", latest_ingestion_run["dataset_count"])
print("Records:", latest_ingestion_run["total_records"])
print("Status:", latest_ingestion_run["run_status"])

# COMMAND ----------

# ============================================
# BRONZE QUALITY GATE
# ============================================

bronze_quality_passed = validate_bronze_quality(
    BRONZE_INGESTION_RUN_ID
)

if not bronze_quality_passed:
    raise Exception("Bronze quality validation failed.")

print("Bronze quality gate: PASSED")
print("Ingestion Run ID:", BRONZE_INGESTION_RUN_ID)

# COMMAND ----------

# ============================================
# SILVER QUALITY INFRASTRUCTURE
# ============================================

silver_audit_table = "anvesh.default.silver_incremental_audit"

if spark.catalog.tableExists(silver_audit_table):

    silver_audit_df = spark.table(silver_audit_table)

    print("Silver audit table exists.")

    print("\nSchema:")
    silver_audit_df.printSchema()

    print("\nTotal records:", silver_audit_df.count())

    display(silver_audit_df)

else:
    print("Silver audit table not found.")

# COMMAND ----------

# ============================================
# SILVER LAYER VALIDATION
# ============================================

silver_tables = {
    "agency": 1,
    "calendar": 3,
    "fare_attributes": 10,
    "fare_rules": 3249,
    "feed_info": 1,
    "routes": 3,
    "shapes": 2450,
    "stop_times": 61236,
    "stops": 705,
    "trips": 2820
}

validation_results = []

for dataset, expected_count in silver_tables.items():

    table_name = f"anvesh.default.silver_{dataset}"

    try:
        actual_count = spark.table(table_name).count()

        status = (
            "PASS"
            if actual_count == expected_count
            else "FAIL"
        )

        validation_results.append({
            "table_name": table_name,
            "expected_count": expected_count,
            "actual_count": actual_count,
            "status": status
        })

    except Exception as e:

        validation_results.append({
            "table_name": table_name,
            "expected_count": expected_count,
            "actual_count": None,
            "status": "ERROR: " + str(e)[:150]
        })

silver_validation_df = spark.createDataFrame(validation_results)

display(silver_validation_df)

passed = sum(
    1 for row in validation_results
    if row["status"] == "PASS"
)

failed = len(validation_results) - passed

print("Total tables:", len(validation_results))
print("Passed:", passed)
print("Failed:", failed)

if failed > 0:
    raise Exception("Silver table count validation failed.")

print("SILVER TABLE COUNT VALIDATION PASSED")

# COMMAND ----------


from pyspark.sql import functions as F

duplicate_checks = {
    "silver_agency": ["agency_id"],
    "silver_calendar": ["service_id"],
    "silver_fare_attributes": ["fare_id"],
    "silver_fare_rules": ["fare_id", "origin_id", "destination_id"],
    "silver_feed_info": ["feed_publisher_name", "feed_start_date", "feed_end_date"],
    "silver_routes": ["route_id"],
    "silver_shapes": ["shape_id", "shape_pt_sequence"],
    "silver_stop_times": ["trip_id", "stop_sequence"],
    "silver_stops": ["stop_id"],
    "silver_trips": ["trip_id"]
}

duplicate_results = []

for table, keys in duplicate_checks.items():

    df = spark.table(f"anvesh.default.{table}")

    missing_columns = [key for key in keys if key not in df.columns]

    if missing_columns:
        raise Exception(
            f"{table}: Missing key columns {missing_columns}"
        )

    duplicate_count = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    duplicate_results.append({
        "table_name": table,
        "business_key": ", ".join(keys),
        "duplicate_groups": duplicate_count,
        "status": "PASS" if duplicate_count == 0 else "FAIL"
    })

display(spark.createDataFrame(duplicate_results))

passed = sum(r["status"] == "PASS" for r in duplicate_results)
failed = len(duplicate_results) - passed

print("Total checks:", len(duplicate_results))
print("Passed:", passed)
print("Failed:", failed)

if failed:
    raise Exception("Silver duplicate key validation failed.")

print("SILVER DUPLICATE KEY VALIDATION PASSED")


# COMMAND ----------

# ============================================
# SILVER REFERENTIAL INTEGRITY VALIDATION
# ============================================

from pyspark.sql import functions as F

relationships = [
    ("silver_trips", "route_id", "silver_routes", "route_id"),
    ("silver_trips", "service_id", "silver_calendar", "service_id"),
    ("silver_stop_times", "trip_id", "silver_trips", "trip_id"),
    ("silver_stop_times", "stop_id", "silver_stops", "stop_id"),
    ("silver_trips", "shape_id", "silver_shapes", "shape_id"),
    ("silver_fare_rules", "fare_id", "silver_fare_attributes", "fare_id")
]

integrity_results = []

for source_table, source_col, target_table, target_col in relationships:

    source_df = spark.table(f"anvesh.default.{source_table}")
    target_df = spark.table(f"anvesh.default.{target_table}")

    orphan_count = (
        source_df
        .filter(F.col(source_col).isNotNull())
        .join(
            target_df.select(target_col).distinct(),
            source_df[source_col] == target_df[target_col],
            "left_anti"
        )
        .count()
    )

    integrity_results.append({
        "relationship": f"{source_table}.{source_col} -> {target_table}.{target_col}",
        "orphan_records": orphan_count,
        "status": "PASS" if orphan_count == 0 else "FAIL"
    })

display(spark.createDataFrame(integrity_results))

passed = sum(r["status"] == "PASS" for r in integrity_results)
failed = len(integrity_results) - passed

print("Total relationships:", len(integrity_results))
print("Passed:", passed)
print("Failed:", failed)

if failed:
    raise Exception("Silver referential integrity validation failed.")

print("SILVER REFERENTIAL INTEGRITY VALIDATION PASSED")

# COMMAND ----------


from pyspark.sql import functions as F

def validate_silver_quality():

    print("=" * 55)
    print("STARTING SILVER QUALITY GATE")
    print("=" * 55)

    # 1. Table count validation
    expected_counts = {
        "agency": 1,
        "calendar": 3,
        "fare_attributes": 10,
        "fare_rules": 3249,
        "feed_info": 1,
        "routes": 3,
        "shapes": 2450,
        "stop_times": 61236,
        "stops": 705,
        "trips": 2820
    }

    for dataset, expected in expected_counts.items():
        actual = spark.table(
            f"anvesh.default.silver_{dataset}"
        ).count()

        if actual != expected:
            raise Exception(
                f"Silver count mismatch: {dataset}. "
                f"Expected {expected}, got {actual}"
            )

    print("Table count checks: 10/10 PASS")

    # 2. Duplicate key validation
    duplicate_checks = {
        "silver_agency": ["agency_id"],
        "silver_calendar": ["service_id"],
        "silver_fare_attributes": ["fare_id"],
        "silver_fare_rules": [
            "fare_id", "origin_id", "destination_id"
        ],
        "silver_feed_info": [
            "feed_publisher_name",
            "feed_start_date",
            "feed_end_date"
        ],
        "silver_routes": ["route_id"],
        "silver_shapes": ["shape_id", "shape_pt_sequence"],
        "silver_stop_times": ["trip_id", "stop_sequence"],
        "silver_stops": ["stop_id"],
        "silver_trips": ["trip_id"]
    }

    for table, keys in duplicate_checks.items():

        df = spark.table(f"anvesh.default.{table}")

        duplicates = (
            df.groupBy(*keys)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if duplicates > 0:
            raise Exception(
                f"Duplicate keys found in {table}: {duplicates}"
            )

    print("Duplicate key checks: 10/10 PASS")

    # 3. Referential integrity validation
    relationships = [
        ("silver_trips", "route_id", "silver_routes", "route_id"),
        ("silver_trips", "service_id", "silver_calendar", "service_id"),
        ("silver_stop_times", "trip_id", "silver_trips", "trip_id"),
        ("silver_stop_times", "stop_id", "silver_stops", "stop_id"),
        ("silver_trips", "shape_id", "silver_shapes", "shape_id"),
        ("silver_fare_rules", "fare_id",
         "silver_fare_attributes", "fare_id")
    ]

    for source_table, source_col, target_table, target_col in relationships:

        source_df = spark.table(
            f"anvesh.default.{source_table}"
        )

        target_df = spark.table(
            f"anvesh.default.{target_table}"
        )

        orphans = (
            source_df
            .filter(F.col(source_col).isNotNull())
            .join(
                target_df.select(target_col).distinct(),
                source_df[source_col] == target_df[target_col],
                "left_anti"
            )
            .count()
        )

        if orphans > 0:
            raise Exception(
                f"Referential integrity failed: "
                f"{source_table}.{source_col}"
            )

    print("Referential integrity checks: 6/6 PASS")
    print("=" * 55)
    print("SILVER QUALITY GATE PASSED")
    print("Total checks: 26/26")

    return True


# COMMAND ----------

validate_silver_quality()

# COMMAND ----------

# ============================================
# GOLD QUALITY INFRASTRUCTURE INSPECTION
# ============================================

gold_tables = spark.sql("SHOW TABLES IN anvesh.default")

display(
    gold_tables.filter(
        F.lower(F.col("tableName")).contains("gold")
        | F.lower(F.col("tableName")).contains("quality")
    )
)

# COMMAND ----------

# ============================================
# GOLD LAYER VALIDATION
# ============================================

gold_expected_counts = {
    "gold_dim_station": 57,
    "gold_dim_route": 3,
    "gold_dim_date": 1279,
    "gold_fact_trip": 2820,
    "gold_fact_stop_schedule": 61236,
    "gold_fare_summary": 3249,
    "gold_route_summary": 3,
    "gold_station_summary": 57
}

gold_results = []

for table, expected_count in gold_expected_counts.items():

    table_name = f"anvesh.default.{table}"

    try:
        actual_count = spark.table(table_name).count()

        status = (
            "PASS" if actual_count == expected_count else "FAIL"
        )

        gold_results.append({
            "table_name": table,
            "expected_count": expected_count,
            "actual_count": actual_count,
            "status": status
        })

    except Exception as e:

        gold_results.append({
            "table_name": table,
            "expected_count": expected_count,
            "actual_count": None,
            "status": f"ERROR: {str(e)[:150]}"
        })

display(spark.createDataFrame(gold_results))

passed = sum(r["status"] == "PASS" for r in gold_results)
failed = len(gold_results) - passed

print("Total Gold tables:", len(gold_results))
print("Passed:", passed)
print("Failed:", failed)

if failed:
    raise Exception("Gold table validation failed.")

print("GOLD TABLE COUNT VALIDATION PASSED")

# COMMAND ----------

# ============================================
# GOLD PRIMARY KEY VALIDATION
# ============================================

from pyspark.sql import functions as F

gold_keys = {
    "gold_dim_station": ["station_id"],
    "gold_dim_route": ["route_id"],
    "gold_dim_date": ["date_key"],
    "gold_fact_trip": ["trip_id"],
    "gold_fact_stop_schedule": ["trip_id", "stop_sequence"],
    "gold_fare_summary": ["origin_id", "destination_id", "fare_id"],
    "gold_route_summary": ["route_id"],
    "gold_station_summary": ["station_id"]
}

gold_key_results = []

for table, keys in gold_keys.items():

    df = spark.table(f"anvesh.default.{table}")

    missing_columns = [
        key for key in keys if key not in df.columns
    ]

    if missing_columns:
        raise Exception(
            f"{table}: Missing key columns {missing_columns}"
        )

    null_condition = None

    for key in keys:
        condition = F.col(key).isNull()
        null_condition = (
            condition if null_condition is None
            else null_condition | condition
        )

    null_key_count = df.filter(null_condition).count()

    duplicate_count = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    status = (
        "PASS"
        if null_key_count == 0 and duplicate_count == 0
        else "FAIL"
    )

    gold_key_results.append({
        "table_name": table,
        "business_key": ", ".join(keys),
        "null_keys": null_key_count,
        "duplicate_groups": duplicate_count,
        "status": status
    })

display(spark.createDataFrame(gold_key_results))

passed = sum(r["status"] == "PASS" for r in gold_key_results)
failed = len(gold_key_results) - passed

print("Total key checks:", len(gold_key_results))
print("Passed:", passed)
print("Failed:", failed)

if failed:
    raise Exception("Gold primary key validation failed.")

print("GOLD PRIMARY KEY VALIDATION PASSED")

# COMMAND ----------

# ============================================
# GOLD REFERENTIAL INTEGRITY VALIDATION
# ============================================

from pyspark.sql import functions as F

gold_relationships = [
    (
        "gold_fact_trip", "route_id",
        "gold_dim_route", "route_id"
    ),
    (
        "gold_fact_trip", "service_id",
        "silver_calendar", "service_id"
    ),
    (
        "gold_fact_stop_schedule", "trip_id",
        "gold_fact_trip", "trip_id"
    ),
    (
        "gold_fact_stop_schedule", "stop_id",
        "silver_stops", "stop_id"
    ),
    (
        "gold_fact_trip", "shape_id",
        "silver_shapes", "shape_id"
    ),
    (
        "gold_fare_summary", "fare_id",
        "silver_fare_attributes", "fare_id"
    ),
    (
        "gold_station_summary", "station_id",
        "gold_dim_station", "station_id"
    ),
    (
        "gold_route_summary", "route_id",
        "gold_dim_route", "route_id"
    )
]

gold_integrity_results = []

for source_table, source_col, target_table, target_col in gold_relationships:

    source_df = spark.table(f"anvesh.default.{source_table}")
    target_df = spark.table(f"anvesh.default.{target_table}")

    orphan_count = (
        source_df
        .filter(F.col(source_col).isNotNull())
        .join(
            target_df.select(target_col).distinct(),
            source_df[source_col] == target_df[target_col],
            "left_anti"
        )
        .count()
    )

    gold_integrity_results.append({
        "relationship": f"{source_table}.{source_col} -> {target_table}.{target_col}",
        "orphan_records": orphan_count,
        "status": "PASS" if orphan_count == 0 else "FAIL"
    })

display(spark.createDataFrame(gold_integrity_results))

passed = sum(
    r["status"] == "PASS"
    for r in gold_integrity_results
)

failed = len(gold_integrity_results) - passed

print("Total relationships:", len(gold_integrity_results))
print("Passed:", passed)
print("Failed:", failed)

if failed:
    raise Exception("Gold referential integrity validation failed.")

print("GOLD REFERENTIAL INTEGRITY VALIDATION PASSED")

# COMMAND ----------

# ============================================
# GOLD SOURCE-TO-TARGET RECONCILIATION
# ============================================

reconciliation_pairs = [
    ("silver_trips", "gold_fact_trip"),
    ("silver_stop_times", "gold_fact_stop_schedule"),
    ("silver_routes", "gold_dim_route"),
    ("silver_stops_stations", "gold_dim_station"),
    ("silver_routes_summary", "gold_route_summary"),
    ("silver_stops_summary", "gold_station_summary"),
    ("silver_fare_rules", "gold_fare_summary")
]

reconciliation_results = []

for source, target in reconciliation_pairs:

    if source == "silver_stops_stations":
        source_count = (
            spark.table("anvesh.default.silver_stops")
            .filter(F.col("location_type") == 1)
            .count()
        )

    elif source == "silver_routes_summary":
        source_count = (
            spark.table("anvesh.default.silver_routes")
            .count()
        )

    elif source == "silver_stops_summary":
        source_count = (
            spark.table("anvesh.default.silver_stops")
            .filter(F.col("location_type") == 1)
            .count()
        )

    else:
        source_count = spark.table(
            f"anvesh.default.{source}"
        ).count()

    target_count = spark.table(
        f"anvesh.default.{target}"
    ).count()

    status = "PASS" if source_count == target_count else "FAIL"

    reconciliation_results.append({
        "source": source,
        "target": target,
        "source_count": source_count,
        "target_count": target_count,
        "status": status
    })

display(spark.createDataFrame(reconciliation_results))

failed = sum(
    row["status"] == "FAIL"
    for row in reconciliation_results
)

print("Total reconciliation checks:", len(reconciliation_results))
print("Passed:", len(reconciliation_results) - failed)
print("Failed:", failed)

if failed:
    raise Exception("Gold reconciliation failed.")

print("GOLD RECONCILIATION PASSED")

# COMMAND ----------


from pyspark.sql import functions as F

def validate_gold_quality():

    print("=" * 55)
    print("STARTING GOLD QUALITY GATE")
    print("=" * 55)

    # 1. Gold table counts
    expected_counts = {
        "gold_dim_station": 57,
        "gold_dim_route": 3,
        "gold_dim_date": 1279,
        "gold_fact_trip": 2820,
        "gold_fact_stop_schedule": 61236,
        "gold_fare_summary": 3249,
        "gold_route_summary": 3,
        "gold_station_summary": 57
    }

    for table, expected in expected_counts.items():

        actual = spark.table(
            f"anvesh.default.{table}"
        ).count()

        if actual != expected:
            raise Exception(
                f"Gold count mismatch: {table}. "
                f"Expected {expected}, got {actual}"
            )

    print("Table count checks: 8/8 PASS")

    # 2. Primary and composite keys
    gold_keys = {
        "gold_dim_station": ["station_id"],
        "gold_dim_route": ["route_id"],
        "gold_dim_date": ["date_key"],
        "gold_fact_trip": ["trip_id"],
        "gold_fact_stop_schedule": ["trip_id", "stop_sequence"],
        "gold_fare_summary": [
            "origin_id", "destination_id", "fare_id"
        ],
        "gold_route_summary": ["route_id"],
        "gold_station_summary": ["station_id"]
    }

    for table, keys in gold_keys.items():

        df = spark.table(f"anvesh.default.{table}")

        null_condition = None

        for key in keys:
            condition = F.col(key).isNull()
            null_condition = (
                condition if null_condition is None
                else null_condition | condition
            )

        null_count = df.filter(null_condition).count()

        duplicate_count = (
            df.groupBy(*keys)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if null_count > 0 or duplicate_count > 0:
            raise Exception(
                f"Gold key validation failed: {table}"
            )

    print("Primary/composite key checks: 8/8 PASS")

    # 3. Referential integrity
    relationships = [
        ("gold_fact_trip", "route_id",
         "gold_dim_route", "route_id"),
        ("gold_fact_trip", "service_id",
         "silver_calendar", "service_id"),
        ("gold_fact_stop_schedule", "trip_id",
         "gold_fact_trip", "trip_id"),
        ("gold_fact_stop_schedule", "stop_id",
         "silver_stops", "stop_id"),
        ("gold_fact_trip", "shape_id",
         "silver_shapes", "shape_id"),
        ("gold_fare_summary", "fare_id",
         "silver_fare_attributes", "fare_id"),
        ("gold_station_summary", "station_id",
         "gold_dim_station", "station_id"),
        ("gold_route_summary", "route_id",
         "gold_dim_route", "route_id")
    ]

    for source, source_col, target, target_col in relationships:

        source_df = spark.table(f"anvesh.default.{source}")
        target_df = spark.table(f"anvesh.default.{target}")

        orphan_count = (
            source_df
            .filter(F.col(source_col).isNotNull())
            .join(
                target_df.select(target_col).distinct(),
                source_df[source_col] == target_df[target_col],
                "left_anti"
            )
            .count()
        )

        if orphan_count > 0:
            raise Exception(
                f"Gold relationship failed: {source}.{source_col}"
            )

    print("Referential integrity checks: 8/8 PASS")

    # 4. Source-to-Gold reconciliation
    source_target_pairs = [
        ("silver_trips", "gold_fact_trip"),
        ("silver_stop_times", "gold_fact_stop_schedule"),
        ("silver_routes", "gold_dim_route"),
        ("silver_routes", "gold_route_summary"),
        ("silver_fare_rules", "gold_fare_summary")
    ]

    for source, target in source_target_pairs:

        source_count = spark.table(
            f"anvesh.default.{source}"
        ).count()

        target_count = spark.table(
            f"anvesh.default.{target}"
        ).count()

        if source_count != target_count:
            raise Exception(
                f"Reconciliation failed: {source} -> {target}"
            )

    # Station-level reconciliation
    station_source_count = (
        spark.table("anvesh.default.silver_stops")
        .filter(F.col("location_type") == 1)
        .count()
    )

    for target in ["gold_dim_station", "gold_station_summary"]:

        target_count = spark.table(
            f"anvesh.default.{target}"
        ).count()

        if station_source_count != target_count:
            raise Exception(
                f"Station reconciliation failed: {target}"
            )

    print("Source-to-Gold reconciliation checks: 7/7 PASS")

    print("=" * 55)
    print("GOLD QUALITY GATE PASSED")
    print("Total checks: 31/31")

    return True


# COMMAND ----------

validate_gold_quality()

# COMMAND ----------


def run_pipeline_with_final_gold_gate():

    print("Starting pipeline with additional Gold validation")

    pipeline_status = run_pipeline()

    if pipeline_status != "SUCCESS":
        print("Pipeline failed. Final Gold gate skipped.")
        return "FAILED"

    try:
        print("\nRunning additional Gold quality gate...")

        gold_gate_passed = validate_gold_quality()

        if not gold_gate_passed:
            raise Exception("Gold quality gate failed.")

        print("\nALL PIPELINE QUALITY GATES PASSED")
        return "SUCCESS"

    except Exception as e:
        print("\nFINAL GOLD VALIDATION FAILED:", str(e))
        return "FAILED"


# COMMAND ----------

run_pipeline_with_final_gold_gate()

# COMMAND ----------

# DBTITLE 1,Cell 47
dbutils.notebook.run(
    "/Users/anveshp222002@gmail.com/00_bronze_ingestion",
    3600,
    {}
)

# COMMAND ----------

print("Pipeline function:", run_pipeline)
print("Wrapper function:", run_pipeline_with_final_gold_gate)
print("Bronze notebook:", NOTEBOOK_PATHS["bronze_ingestion"])
print("Execution function:", execute_notebook)

# COMMAND ----------

