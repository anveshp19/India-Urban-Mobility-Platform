# Databricks notebook source
# # Module 11: Apache Spark Performance Optimization

# ## Project: India Urban Mobility Platform

# ### Learning Objectives
# 1. Understand Apache Spark architecture.
# 2. Learn Driver, Executors and Cluster Manager.
# 3. Understand Jobs, Stages and Tasks.
# 4. Learn Narrow and Wide transformations.
# 5. Understand Shuffling and Partitioning.
# 6. Optimize Spark joins.
# 7. Learn Data Skew handling.
# 8. Understand Caching and Persistence.
# 9. Analyze Spark execution plans.
# 10. Optimize PySpark pipelines.

# Environment:
# - Databricks Free Edition
# - PySpark
# - Delta Lake
# - India Urban Mobility GTFS Dataset

# COMMAND ----------

from pyspark.sql import functions as F

stops_df = spark.table("anvesh.default.silver_stops")

print("Total records:", stops_df.count())

print("Number of partitions:", stops_df.rdd.getNumPartitions())

# COMMAND ----------

print("Total records:", stops_df.count())

print("DataFrame schema:")
stops_df.printSchema()

print("Execution plan:")
stops_df.explain("formatted")

# COMMAND ----------

filtered_stops = (
    stops_df
    .filter(F.col("location_type") == 1)
)

print("Filtered station count:", filtered_stops.count())

filtered_stops.explain("formatted")

# COMMAND ----------

selected_stops = (
    stops_df
    .select(
        "stop_id",
        "stop_name",
        "location_type"
    )
)

selected_stops.explain("formatted")

# COMMAND ----------

filtered_df = (
    stops_df
    .filter(F.col("location_type") == 1)
    .select("stop_id", "stop_name")
)

print("DataFrame created successfully")

# COMMAND ----------

filtered_df = (
    stops_df
    .filter(F.col("location_type") == 1)
    .select("stop_id", "stop_name")
)

# COMMAND ----------

print("Before action")

filtered_df.count()

print("After action")

# COMMAND ----------

narrow_df = (
    stops_df
    .filter(F.col("location_type") == 1)
    .select("stop_id", "stop_name")
)

display(narrow_df)

# COMMAND ----------

wide_df = (
    stops_df
    .groupBy("location_type")
    .count()
)

display(wide_df)

# COMMAND ----------

narrow_df.explain("formatted")

# COMMAND ----------

wide_df.explain("formatted")

# COMMAND ----------

route_counts = (
    spark.table("anvesh.default.gold_fact_trip")
    .groupBy("route_id")
    .count()
)

display(route_counts)

# COMMAND ----------

route_counts.explain("formatted")

# COMMAND ----------

repartitioned_trips = (
    spark.table("anvesh.default.gold_fact_trip")
    .repartition(4, "route_id")
)

repartitioned_trips.explain("formatted")

# COMMAND ----------

repartitioned_df = (
    spark.table("anvesh.default.gold_fact_trip")
    .repartition(4)
)

repartitioned_df.explain("formatted")

# COMMAND ----------

coalesced_df = (
    spark.table("anvesh.default.gold_fact_trip")
    .coalesce(1)
)

coalesced_df.explain("formatted")

# COMMAND ----------

from pyspark.sql import functions as F

stop_times_df = spark.table("anvesh.default.silver_stop_times")
trips_df = spark.table("anvesh.default.silver_trips")

normal_join = stop_times_df.join(
    trips_df.select("trip_id", "route_id"),
    on="trip_id",
    how="inner"
)

normal_join.explain("formatted")

# COMMAND ----------

broadcast_join = stop_times_df.join(
    F.broadcast(
        trips_df.select("trip_id", "route_id")
    ),
    on="trip_id",
    how="inner"
)

broadcast_join.explain("formatted")

# COMMAND ----------

sort_merge_join = (
    stop_times_df.hint("merge")
    .join(
        trips_df.select("trip_id", "route_id").hint("merge"),
        on="trip_id",
        how="inner"
    )
)

sort_merge_join.explain("formatted")

# COMMAND ----------

route_distribution = (
    spark.table("anvesh.default.gold_fact_trip")
    .groupBy("route_id")
    .count()
    .orderBy(F.desc("count"))
)

display(route_distribution)

# COMMAND ----------

from pyspark.sql import functions as F

skew_test_df = (
    spark.range(10000)
    .withColumn(
        "route_id",
        F.when(F.col("id") < 9000, "RED")
         .when(F.col("id") < 9500, "BLUE")
         .when(F.col("id") < 9800, "GREEN")
         .otherwise("YELLOW")
    )
)

display(
    skew_test_df
    .groupBy("route_id")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

skew_repartitioned_df = (
    skew_test_df
    .repartition(4, "route_id")
)

skew_repartitioned_df.explain("formatted")

# COMMAND ----------

partition_distribution = (
    skew_repartitioned_df
    .select(
        F.spark_partition_id().alias("partition_id")
    )
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)

display(partition_distribution)

# COMMAND ----------

salted_df = (
    skew_test_df
    .withColumn(
        "salt",
        F.when(
            F.col("route_id") == "RED",
            F.pmod(F.col("id"), F.lit(4))
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "salted_route_id",
        F.concat(
            F.col("route_id"),
            F.lit("_"),
            F.col("salt")
        )
    )
)

display(
    salted_df
    .groupBy("salted_route_id")
    .count()
    .orderBy("salted_route_id")
)

# COMMAND ----------

salted_repartitioned_df = (
    salted_df
    .repartition(4, "salted_route_id")
)

salted_partition_distribution = (
    salted_repartitioned_df
    .groupBy(F.spark_partition_id().alias("partition_id"))
    .count()
    .orderBy("partition_id")
)

display(salted_partition_distribution)

# COMMAND ----------

display(
    salted_repartitioned_df
    .groupBy(
        F.spark_partition_id().alias("partition_id"),
        "salted_route_id"
    )
    .count()
    .orderBy("partition_id", "salted_route_id")
)

# COMMAND ----------

reusable_stops_df = (
    spark.table("anvesh.default.silver_stops")
    .filter(F.col("location_type") == 1)
    .select("stop_id", "stop_name", "stop_lat", "stop_lon")
)

print("DataFrame prepared successfully")

# COMMAND ----------

import time

start_time = time.perf_counter()

uncached_count = reusable_stops_df.count()

uncached_time = time.perf_counter() - start_time

print("Station count:", uncached_count)
print(f"Execution time: {uncached_time:.4f} seconds")

# COMMAND ----------

cached_stops_df = reusable_stops_df.cache()

start_time = time.perf_counter()

cached_count = cached_stops_df.count()

cached_time = time.perf_counter() - start_time

print("Station count:", cached_count)
print(f"First cached execution: {cached_time:.4f} seconds")

# COMMAND ----------

start_time = time.perf_counter()

cached_count_again = cached_stops_df.count()

second_cached_time = time.perf_counter() - start_time

print("Station count:", cached_count_again)
print(f"Second cached execution: {second_cached_time:.4f} seconds")

# COMMAND ----------

cached_stops_df.unpersist()

print("Cached DataFrame released successfully")

# COMMAND ----------



# COMMAND ----------

from pyspark import StorageLevel
from pyspark.sql import functions as F

fresh_stops_df = (
    spark.table("anvesh.default.silver_stops")
    .filter(F.col("location_type") == 1)
    .select("stop_id", "stop_name", "stop_lat", "stop_lon")
)

persisted_stops_df = fresh_stops_df.persist(
    StorageLevel.MEMORY_AND_DISK
)

print("Station count:", persisted_stops_df.count())
print("Persisted DataFrame materialized successfully")

# COMMAND ----------

persisted_stops_df.unpersist()

print("Persistence released successfully")

# COMMAND ----------

aqe_test_df = (
    spark.table("anvesh.default.silver_stop_times")
    .groupBy("trip_id")
    .count()
)

aqe_test_df.explain("formatted")

# COMMAND ----------

display(aqe_test_df)

# COMMAND ----------

aqe_test_df.explain("formatted")

# COMMAND ----------

aqe_join_df = (
    spark.table("anvesh.default.silver_stop_times")
    .join(
        spark.table("anvesh.default.silver_trips")
        .select("trip_id", "route_id"),
        on="trip_id",
        how="inner"
    )
    .groupBy("route_id")
    .count()
)

aqe_join_df.explain("formatted")

# COMMAND ----------

display(aqe_join_df)

# COMMAND ----------

display(
    spark.sql("""
        DESCRIBE DETAIL anvesh.default.test_delta_routes
    """)
)

# COMMAND ----------

small_files_df = spark.range(1000).repartition(10)

(
    small_files_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("anvesh.default.demo_small_files")
)

print("Demo table created successfully")

# COMMAND ----------

small_files_detail = spark.sql("""
    DESCRIBE DETAIL anvesh.default.demo_small_files
""")

display(
    small_files_detail.select(
        "format",
        "numFiles",
        "sizeInBytes",
        "partitionColumns"
    )
)

# COMMAND ----------

spark.sql("""
    OPTIMIZE anvesh.default.demo_small_files
""")

print("Delta OPTIMIZE completed successfully")

# COMMAND ----------

optimized_detail = spark.sql("""
    DESCRIBE DETAIL anvesh.default.demo_small_files
""")

display(
    optimized_detail.select(
        "numFiles",
        "sizeInBytes",
        "format"
    )
)

# COMMAND ----------

display(
    spark.sql("""
        DESCRIBE HISTORY anvesh.default.demo_small_files
    """)
)

# COMMAND ----------

display(
    spark.sql("""
        DESCRIBE DETAIL anvesh.default.demo_small_files
    """).select(
        "numFiles",
        "sizeInBytes",
        "format"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM anvesh.default.demo_small_files DRY RUN;

# COMMAND ----------

display(
    spark.sql("""
        DESCRIBE DETAIL anvesh.default.demo_small_files
    """).select("numFiles", "sizeInBytes")
)

# COMMAND ----------

performance_df = (
    spark.table("anvesh.default.silver_stop_times")
    .join(
        spark.table("anvesh.default.silver_trips")
        .select("trip_id", "route_id"),
        on="trip_id",
        how="inner"
    )
    .groupBy("route_id")
    .count()
)

display(performance_df)

# COMMAND ----------

broadcast_join_df = (
    spark.table("anvesh.default.silver_stop_times")
    .join(
        spark.table("anvesh.default.silver_trips")
        .select("trip_id", "route_id").hint("broadcast"),
        on="trip_id",
        how="inner"
    )
)

broadcast_join_df.explain("formatted")

# COMMAND ----------

sort_merge_join_df = (
    spark.table("anvesh.default.silver_stop_times")
    .hint("merge")
    .join(
        spark.table("anvesh.default.silver_trips")
        .select("trip_id", "route_id")
        .hint("merge"),
        on="trip_id",
        how="inner"
    )
)

sort_merge_join_df.explain("formatted")

# COMMAND ----------

case_study_df = (
    spark.table("anvesh.default.silver_stop_times")
    .join(
        spark.table("anvesh.default.silver_trips"),
        on="trip_id",
        how="inner"
    )
)

print("Case study DataFrame created")
print("Columns:", len(case_study_df.columns))

# COMMAND ----------

case_study_df.printSchema()

# COMMAND ----------

optimized_case_study_df = case_study_df.select(
    "route_id",
    "trip_id",
    "stop_id",
    "stop_sequence",
    "arrival_time",
    "departure_time"
)

optimized_case_study_df.printSchema()

print("Optimized column count:", len(optimized_case_study_df.columns))

# COMMAND ----------

optimized_case_study_df.explain("formatted")

# COMMAND ----------

from pyspark.sql import functions as F

filtered_trips_df = (
    spark.table("anvesh.default.silver_trips")
    .filter(F.col("route_id") == "RED")
    .select("trip_id", "route_id")
)

optimized_red_df = (
    spark.table("anvesh.default.silver_stop_times")
    .join(filtered_trips_df, on="trip_id", how="inner")
    .select(
        "route_id",
        "trip_id",
        "stop_id",
        "stop_sequence"
    )
)

optimized_red_df.explain("formatted")

# COMMAND ----------

print("RED stop-time records:", optimized_red_df.count())

display(
    optimized_red_df
    .groupBy("route_id")
    .count()
)

# COMMAND ----------

print("Original columns:", len(case_study_df.columns))
print("Optimized columns:", len(optimized_case_study_df.columns))

print("Original row count:", case_study_df.count())
print("Optimized row count:", optimized_case_study_df.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN anvesh.default;

# COMMAND ----------

tables = [
    "agency",
    "calendar",
    "fare_attributes",
    "fare_rules",
    "feed_info",
    "routes",
    "shapes",
    "stop_times",
    "stops",
    "trips"
]

for table in tables:
    bronze_count = spark.table(
        f"anvesh.default.bronze_{table}"
    ).count()

    silver_count = spark.table(
        f"anvesh.default.silver_{table}"
    ).count()

    print(
        f"{table}: Bronze={bronze_count}, "
        f"Silver={silver_count}"
    )

# COMMAND ----------

gold_tables = [
    "gold_dim_station",
    "gold_dim_route",
    "gold_dim_date",
    "gold_fact_trip",
    "gold_fact_stop_schedule",
    "gold_route_summary",
    "gold_station_summary",
    "gold_fare_summary"
]

for table in gold_tables:
    count = spark.table(
        f"anvesh.default.{table}"
    ).count()

    print(f"{table}: {count} records")

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "anvesh"
schema = "default"

datasets = [
    "agency",
    "calendar",
    "fare_attributes",
    "fare_rules",
    "feed_info",
    "routes",
    "shapes",
    "stop_times",
    "stops",
    "trips"
]

results = []

for dataset in datasets:
    table_name = f"{catalog}.{schema}.bronze_{dataset}"

    try:
        df = spark.table(table_name)

        results.append({
            "dataset": dataset,
            "table": table_name,
            "row_count": df.count(),
            "status": "SUCCESS"
        })

    except Exception as e:
        results.append({
            "dataset": dataset,
            "table": table_name,
            "row_count": 0,
            "status": f"FAILED: {str(e)[:150]}"
        })

display(spark.createDataFrame(results))

# COMMAND ----------

metadata_columns = [
    "_source_file",
    "_source_system",
    "_ingestion_run_id",
    "_ingestion_timestamp"
]

for dataset in datasets:
    table_name = f"{catalog}.{schema}.bronze_{dataset}"

    df = spark.table(table_name)

    missing_columns = [
        col for col in metadata_columns
        if col not in df.columns
    ]

    print(f"\nDataset: {dataset}")
    print("Metadata columns:", "PASS" if not missing_columns else "FAIL")
    print("Missing columns:", missing_columns)

# COMMAND ----------

audit_table = "anvesh.default.bronze_ingestion_audit"

if spark.catalog.tableExists(audit_table):
    audit_df = spark.table(audit_table)

    print("Audit table exists")
    print("Total audit records:", audit_df.count())

    display(audit_df.orderBy(F.col("_ingestion_timestamp").desc()))
else:
    print("Audit table does not exist")

# COMMAND ----------

audit_table = "anvesh.default.bronze_ingestion_audit"

audit_df = spark.table(audit_table)

print("Audit table schema:")
audit_df.printSchema()

print("Available columns:")
print(audit_df.columns)

# COMMAND ----------

from pyspark.sql import functions as F

audit_df = spark.table("anvesh.default.bronze_ingestion_audit")

display(
    audit_df
    .groupBy("run_id", "pipeline_name", "status")
    .agg(
        F.count("*").alias("datasets_processed"),
        F.sum("record_count").alias("total_records"),
        F.min("started_at").alias("run_started_at"),
        F.max("completed_at").alias("run_completed_at")
    )
    .orderBy(F.col("run_started_at").desc())
)

# COMMAND ----------

latest_run = (
    audit_df
    .filter(F.col("status") == "SUCCESS")
    .agg(F.max("started_at").alias("latest_started_at"))
    .first()["latest_started_at"]
)

display(
    audit_df
    .filter(F.col("started_at") == latest_run)
    .orderBy("target_table")
)

# COMMAND ----------

from pyspark.sql import functions as F

run_id = "c1af71c4-a397-4381-95db-8274de795a31"

run_df = (
    spark.table("anvesh.default.bronze_ingestion_audit")
    .filter(F.col("run_id") == run_id)
)

display(
    run_df.select(
        "source_file",
        "target_table",
        "record_count",
        "status",
        "error_message"
    ).orderBy("source_file")
)

print("Total datasets:", run_df.count())

print(
    "Total records:",
    run_df.agg(F.sum("record_count")).first()[0]
)

# COMMAND ----------

