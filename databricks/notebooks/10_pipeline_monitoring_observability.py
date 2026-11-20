# Databricks notebook source

# ============================================================
# INDIA URBAN MOBILITY PLATFORM
# MODULE 13: PIPELINE MONITORING & OBSERVABILITY
# ============================================================

# Objectives:
# 1. Monitor pipeline execution
# 2. Track Bronze ingestion metrics
# 3. Monitor dataset-level record counts
# 4. Identify failed ingestion tasks
# 5. Build pipeline execution history
# 6. Support error investigation and recovery

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timezone

CATALOG = "anvesh"
SCHEMA = "default"

print("Module 13: Pipeline Monitoring & Observability")
print("Notebook initialized successfully.")


# COMMAND ----------


# ============================================================
# STEP 1: BRONZE INGESTION AUDIT MONITORING
# ============================================================

bronze_audit_df = spark.table(
    "anvesh.default.bronze_ingestion_audit"
)

print("Total audit records:", bronze_audit_df.count())

display(
    bronze_audit_df.orderBy(
        F.col("completed_at").desc()
    )
)


# COMMAND ----------


# ============================================================
# STEP 2: INGESTION RUN SUMMARY
# ============================================================

run_summary_df = (
    bronze_audit_df
    .groupBy("run_id")
    .agg(
        F.count("*").alias("dataset_count"),
        F.sum("record_count").alias("total_records"),
        F.countDistinct("status").alias("status_types"),
        F.min("status").alias("run_status"),
        F.min("started_at").alias("started_at"),
        F.max("completed_at").alias("completed_at")
    )
    .orderBy(F.col("completed_at").desc())
)

display(run_summary_df)


# COMMAND ----------


# ============================================================
# STEP 3: FAILED INGESTION MONITORING
# ============================================================

failed_runs_df = (
    bronze_audit_df
    .filter(F.col("status") != "SUCCESS")
    .select(
        "run_id",
        "pipeline_name",
        "source_file",
        "target_table",
        "record_count",
        "status",
        "error_message",
        "started_at",
        "completed_at"
    )
    .orderBy(F.col("completed_at").desc())
)

print("Total failed dataset executions:", failed_runs_df.count())

display(failed_runs_df)


# COMMAND ----------


# ============================================================
# STEP 4: FAILURE REASON ANALYSIS
# ============================================================

failure_reason_df = (
    failed_runs_df
    .groupBy(
        "source_file",
        "error_message"
    )
    .agg(
        F.count("*").alias("failure_count")
    )
    .orderBy(F.col("failure_count").desc())
)

display(failure_reason_df)


# COMMAND ----------


# ============================================================
# STEP 5: BRONZE SCHEMA INSPECTION
# ============================================================

bronze_table = "anvesh.default.bronze_calendar"

print("EXISTING BRONZE TABLE SCHEMA")

spark.table(bronze_table).printSchema()


# COMMAND ----------


# ============================================================
# STEP 6: BRONZE SCHEMA CONSISTENCY CHECK
# ============================================================

bronze_tables = [
    "bronze_agency",
    "bronze_calendar",
    "bronze_fare_attributes",
    "bronze_fare_rules",
    "bronze_feed_info",
    "bronze_routes",
    "bronze_shapes",
    "bronze_stop_times",
    "bronze_stops",
    "bronze_trips"
]

schema_results = []

for table in bronze_tables:

    df = spark.table(f"anvesh.default.{table}")

    columns = df.columns

    has_run_id = "_ingestion_run_id" in columns
    has_source_file = "_source_file" in columns
    has_timestamp = "_ingestion_timestamp" in columns

    schema_results.append((
        table,
        len(columns),
        has_run_id,
        has_source_file,
        has_timestamp
    ))

schema_report = spark.createDataFrame(
    schema_results,
    [
        "table_name",
        "column_count",
        "has_ingestion_run_id",
        "has_source_file",
        "has_ingestion_timestamp"
    ]
)

display(schema_report)


# COMMAND ----------


# ============================================================
# STEP 7: BRONZE DATA VALIDATION
# ============================================================

for table in bronze_tables:

    full_table_name = f"anvesh.default.{table}"

    record_count = spark.table(
        full_table_name
    ).count()

    print(f"{table}: {record_count:,} records")


# COMMAND ----------


# ============================================================
# STEP 8: BRONZE MONITORING SUMMARY
# ============================================================

monitoring_results = []

for table in bronze_tables:

    df = spark.table(f"anvesh.default.{table}")

    record_count = df.count()

    monitoring_results.append((
        table,
        record_count,
        "SUCCESS" if record_count > 0 else "EMPTY",
        "_ingestion_run_id" in df.columns,
        "_source_file" in df.columns,
        "_ingestion_timestamp" in df.columns
    ))

monitoring_df = spark.createDataFrame(
    monitoring_results,
    [
        "table_name",
        "record_count",
        "table_status",
        "has_run_id",
        "has_source_file",
        "has_ingestion_timestamp"
    ]
)

display(monitoring_df)


# COMMAND ----------


# ============================================================
# STEP 9: BRONZE PIPELINE HEALTH METRICS
# ============================================================

total_datasets = monitoring_df.count()

successful_datasets = (
    monitoring_df
    .filter(F.col("table_status") == "SUCCESS")
    .count()
)

failed_datasets = total_datasets - successful_datasets

total_records = monitoring_df.agg(
    F.sum("record_count")
).first()[0]

success_rate = (
    successful_datasets / total_datasets
) * 100 if total_datasets else 0

metadata_compliant = monitoring_df.filter(
    F.col("has_run_id") &
    F.col("has_source_file") &
    F.col("has_ingestion_timestamp")
).count()

metadata_compliance_rate = (
    metadata_compliant / total_datasets
) * 100 if total_datasets else 0

print("========== BRONZE PIPELINE HEALTH ==========")
print(f"Total datasets: {total_datasets}")
print(f"Successful datasets: {successful_datasets}")
print(f"Failed datasets: {failed_datasets}")
print(f"Total records: {total_records:,}")
print(f"Dataset success rate: {success_rate:.2f}%")
print(f"Metadata compliance: {metadata_compliance_rate:.2f}%")
print("============================================")


# COMMAND ----------


# ============================================================
# STEP 10: HISTORICAL PIPELINE HEALTH REPORT
# ============================================================

historical_health_df = (
    bronze_audit_df
    .groupBy("run_id")
    .agg(
        F.count("*").alias("dataset_executions"),
        F.sum(
            F.when(F.col("status") == "SUCCESS", 1)
             .otherwise(0)
        ).alias("successful_executions"),
        F.sum(
            F.when(F.col("status") != "SUCCESS", 1)
             .otherwise(0)
        ).alias("failed_executions"),
        F.sum("record_count").alias("total_records"),
        F.min("started_at").alias("started_at"),
        F.max("completed_at").alias("completed_at")
    )
    .withColumn(
        "success_rate",
        F.round(
            F.col("successful_executions") /
            F.col("dataset_executions") * 100,
            2
        )
    )
    .withColumn(
        "pipeline_status",
        F.when(
            F.col("failed_executions") > 0,
            "FAILED"
        ).otherwise("SUCCESS")
    )
    .orderBy(F.col("completed_at").desc())
)

display(historical_health_df)


# COMMAND ----------


# ============================================================
# STEP 11: PIPELINE EXECUTION PERFORMANCE CHART
# ============================================================

display(
    historical_health_df.select(
        "run_id",
        "successful_executions",
        "failed_executions"
    )
)


# COMMAND ----------


# ============================================================
# STEP 12: CONSOLIDATED PIPELINE HEALTH REPORT
# ============================================================

print("=" * 60)
print("INDIA URBAN MOBILITY - PIPELINE HEALTH REPORT")
print("=" * 60)

print("\n1. CURRENT BRONZE TABLE HEALTH")
print("-" * 40)

display(monitoring_df)

print("\n2. HISTORICAL PIPELINE EXECUTIONS")
print("-" * 40)

display(historical_health_df)

print("\n3. CURRENT BRONZE HEALTH METRICS")
print("-" * 40)

display(health_metrics_df)


# COMMAND ----------


# ============================================================
# STEP 13: FINAL BRONZE HEALTH METRICS
# ============================================================

bronze_final_metrics = monitoring_df.agg(
    F.count("*").alias("total_tables"),

    F.sum(
        F.when(F.col("table_status") == "SUCCESS", 1).otherwise(0)
    ).alias("successful_tables"),

    F.sum(
        F.when(F.col("table_status") != "SUCCESS", 1).otherwise(0)
    ).alias("failed_tables"),

    F.sum("record_count").alias("total_records"),

    F.sum(
        F.when(
            F.col("has_run_id") &
            F.col("has_source_file") &
            F.col("has_ingestion_timestamp"),
            1
        ).otherwise(0)
    ).alias("metadata_compliant_tables")
)

display(bronze_final_metrics)


# COMMAND ----------


# ============================================================
# STEP 14: SAVE BRONZE MONITORING SNAPSHOT
# ============================================================

snapshot_df = (
    monitoring_df
    .withColumn("snapshot_timestamp", F.current_timestamp())
    .withColumn("pipeline_name", F.lit("india_urban_mobility_pipeline"))
)

(
    snapshot_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.bronze_monitoring_history")
)

print("Bronze monitoring snapshot saved successfully.")

display(
    spark.table("anvesh.default.bronze_monitoring_history")
    .orderBy(F.col("snapshot_timestamp").desc())
)


# COMMAND ----------


snapshot_history = spark.table(
    "anvesh.default.bronze_monitoring_history"
)

print("Total monitoring snapshots:", snapshot_history.count())

display(
    snapshot_history.select(
        "table_name",
        "record_count",
        "table_status",
        "snapshot_timestamp",
        "pipeline_name"
    ).orderBy("table_name")
)


# COMMAND ----------

# Capture another monitoring snapshot
new_snapshot_df = (
    monitoring_df
    .withColumn("snapshot_timestamp", F.current_timestamp())
    .withColumn(
        "pipeline_name",
        F.lit("india_urban_mobility_pipeline")
    )
)

new_snapshot_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("anvesh.default.bronze_monitoring_history")

print(
    "Total snapshot records:",
    spark.table(
        "anvesh.default.bronze_monitoring_history"
    ).count()
)

# COMMAND ----------

# ============================================================
# STEP 17: BRONZE MONITORING TREND ANALYSIS
# ============================================================

monitoring_history = spark.table(
    "anvesh.default.bronze_monitoring_history"
)

trend_report = (
    monitoring_history
    .groupBy("snapshot_timestamp")
    .agg(
        F.count("*").alias("total_tables"),
        F.sum(
            F.when(
                F.col("table_status") == "SUCCESS", 1
            ).otherwise(0)
        ).alias("successful_tables"),
        F.sum(
            F.when(
                F.col("table_status") != "SUCCESS", 1
            ).otherwise(0)
        ).alias("failed_tables"),
        F.sum("record_count").alias("total_records")
    )
    .withColumn(
        "health_percentage",
        F.round(
            F.col("successful_tables") /
            F.col("total_tables") * 100,
            2
        )
    )
    .orderBy("snapshot_timestamp")
)

display(trend_report)

# COMMAND ----------

# ============================================================
# STEP 18: BRONZE HEALTH ALERT CHECK
# ============================================================

failed_tables_df = monitoring_df.filter(
    (F.col("table_status") != "SUCCESS") |
    (~F.col("has_run_id")) |
    (~F.col("has_source_file")) |
    (~F.col("has_ingestion_timestamp"))
)

failed_count = failed_tables_df.count()

if failed_count > 0:
    print(f"ALERT: {failed_count} Bronze tables need attention.")
    display(failed_tables_df)
else:
    print("HEALTHY: All Bronze tables passed monitoring checks.")

# COMMAND ----------

# ============================================================
# STEP 19: REUSABLE BRONZE MONITORING FUNCTION
# ============================================================

def run_bronze_monitoring():

    current_df = spark.table("anvesh.default.bronze_monitoring_history")

    latest_timestamp = current_df.agg(
        F.max("snapshot_timestamp")
    ).first()[0]

    latest_snapshot = current_df.filter(
        F.col("snapshot_timestamp") == latest_timestamp
    )

    total_tables = latest_snapshot.count()

    healthy_tables = latest_snapshot.filter(
        (F.col("table_status") == "SUCCESS") &
        F.col("has_run_id") &
        F.col("has_source_file") &
        F.col("has_ingestion_timestamp")
    ).count()

    failed_tables = total_tables - healthy_tables

    total_records = latest_snapshot.agg(
        F.sum("record_count")
    ).first()[0]

    health_percentage = (
        round((healthy_tables / total_tables) * 100, 2)
        if total_tables else 0
    )

    print("=" * 50)
    print("BRONZE PIPELINE MONITORING SUMMARY")
    print("=" * 50)
    print(f"Total tables       : {total_tables}")
    print(f"Healthy tables     : {healthy_tables}")
    print(f"Failed tables      : {failed_tables}")
    print(f"Total records      : {total_records}")
    print(f"Health percentage  : {health_percentage}%")
    print(f"Latest snapshot    : {latest_timestamp}")
    print("=" * 50)

    return {
        "total_tables": total_tables,
        "healthy_tables": healthy_tables,
        "failed_tables": failed_tables,
        "total_records": total_records,
        "health_percentage": health_percentage
    }


bronze_health_summary = run_bronze_monitoring()

# COMMAND ----------

