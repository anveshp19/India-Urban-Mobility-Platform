# Databricks notebook source
# ============================================================
# INDIA URBAN MOBILITY PLATFORM
# PIPELINE OBSERVABILITY DASHBOARD
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timezone

# Project configuration
CATALOG = "anvesh"
SCHEMA = "default"

PIPELINE_NAME = "india_urban_mobility_pipeline"

# Table references
BRONZE_AUDIT_TABLE = f"{CATALOG}.{SCHEMA}.bronze_ingestion_audit"

MONITORING_HISTORY_TABLE = (
    f"{CATALOG}.{SCHEMA}.bronze_monitoring_history"
)

print("=" * 60)
print("INDIA URBAN MOBILITY PLATFORM")
print("PIPELINE OBSERVABILITY DASHBOARD")
print("=" * 60)

print("Catalog:", CATALOG)
print("Schema:", SCHEMA)
print("Pipeline:", PIPELINE_NAME)

print("Dashboard initialization successful.")

# COMMAND ----------

# ============================================================
# STEP 2: LOAD DASHBOARD DATA SOURCES
# ============================================================

dashboard_bronze_health = spark.table(
    "anvesh.default.bronze_monitoring_history"
)

dashboard_audit = spark.table(
    "anvesh.default.bronze_ingestion_audit"
)

dashboard_snapshots = spark.table(
    "anvesh.default.bronze_monitoring_history"
)

print("DASHBOARD DATA SOURCE VALIDATION")
print("-" * 45)

print("Bronze monitoring records:", dashboard_bronze_health.count())
print("Ingestion audit records:", dashboard_audit.count())
print("Monitoring snapshot records:", dashboard_snapshots.count())

print("\nData sources loaded successfully.")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timezone

print("Imports initialized.")

# COMMAND ----------

dashboard_audit = spark.table(
    "anvesh.default.bronze_ingestion_audit"
)

dashboard_snapshots = spark.table(
    "anvesh.default.bronze_monitoring_history"
)

print("Audit records:", dashboard_audit.count())
print("Snapshot records:", dashboard_snapshots.count())

# COMMAND ----------

dashboard_run_summary = (
    dashboard_audit
    .groupBy("run_id")
    .agg(
        F.sum(
            F.when(F.col("status") == "SUCCESS", 1).otherwise(0)
        ).alias("successful_executions"),

        F.sum(
            F.when(F.col("status") != "SUCCESS", 1).otherwise(0)
        ).alias("failed_executions")
    )
    .withColumn(
        "pipeline_status",
        F.when(F.col("failed_executions") > 0, "FAILED")
         .otherwise("SUCCESS")
    )
)

display(dashboard_run_summary)

# COMMAND ----------


#============================================================
# STEP 3: DASHBOARD KPI METRICS
# ============================================================

# Get the latest monitoring snapshot
latest_snapshot_timestamp = (
    dashboard_snapshots
    .agg(F.max("snapshot_timestamp"))
    .first()[0]
)

latest_bronze_snapshot = dashboard_snapshots.filter(
    F.col("snapshot_timestamp") == latest_snapshot_timestamp
)

# Current Bronze metrics
bronze_kpis = latest_bronze_snapshot.agg(
    F.count("*").alias("total_tables"),

    F.sum(
        F.when(
            (F.col("table_status") == "SUCCESS") &
            F.col("has_run_id") &
            F.col("has_source_file") &
            F.col("has_ingestion_timestamp"),
            1
        ).otherwise(0)
    ).alias("healthy_tables"),

    F.sum("record_count").alias("total_records")
).first()

total_tables = bronze_kpis["total_tables"]
healthy_tables = bronze_kpis["healthy_tables"] or 0
total_records = bronze_kpis["total_records"] or 0

failed_tables = total_tables - healthy_tables

bronze_health_percentage = (
    round(healthy_tables / total_tables * 100, 2)
    if total_tables else 0
)

# Historical pipeline run metrics
historical_run_count = dashboard_audit.select(
    "run_id"
).distinct().count()

failed_run_count = (
    dashboard_run_summary
    .filter(F.col("pipeline_status") == "FAILED")
    .count()
)

successful_run_count = (
    dashboard_run_summary
    .filter(F.col("pipeline_status") == "SUCCESS")
    .count()
)

# Print dashboard KPIs
print("=" * 55)
print("PIPELINE OBSERVABILITY - DASHBOARD KPIs")
print("=" * 55)

print("\nCURRENT BRONZE HEALTH")
print("Total tables:", total_tables)
print("Healthy tables:", healthy_tables)
print("Failed tables:", failed_tables)
print("Total records:", total_records)
print("Health percentage:", bronze_health_percentage, "%")

print("\nHISTORICAL PIPELINE EXECUTIONS")
print("Total pipeline runs:", historical_run_count)
print("Successful runs:", successful_run_count)
print("Failed runs:", failed_run_count)

print("=" * 55)


# COMMAND ----------


# ============================================================
# STEP 4: HISTORICAL PIPELINE EXECUTION REPORT
# ============================================================

dashboard_execution_report = (
    dashboard_run_summary
    .withColumn(
        "success_rate",
        F.round(
            F.col("successful_executions") /
            (
                F.col("successful_executions") +
                F.col("failed_executions")
            ) * 100,
            2
        )
    )
    .orderBy(F.desc("successful_executions"))
)

display(dashboard_execution_report)


# COMMAND ----------


# ============================================================
# STEP 5: INGESTION FAILURE ANALYSIS
# ============================================================

dashboard_failures = (
    dashboard_audit
    .filter(F.col("status") != "SUCCESS")
    .select(
        "run_id",
        "source_file",
        "target_table",
        "status",
        "error_message",
        "started_at",
        "completed_at"
    )
    .orderBy(F.col("started_at").desc())
)

print("TOTAL FAILED AUDIT EXECUTIONS:", dashboard_failures.count())

display(dashboard_failures)


# COMMAND ----------


# ============================================================
# STEP 6: FAILURE REASON CATEGORIZATION
# ============================================================

failure_categories = (
    dashboard_failures
    .withColumn(
        "failure_category",
        F.when(
            F.col("error_message").contains("DELTA_METADATA_MISMATCH"),
            "DELTA_SCHEMA_MISMATCH"
        )
        .when(
            F.col("error_message").contains("PERMISSION"),
            "PERMISSION_ERROR"
        )
        .when(
            F.col("error_message").contains("FileNotFound"),
            "FILE_NOT_FOUND"
        )
        .otherwise("OTHER_FAILURE")
    )
)

failure_summary = (
    failure_categories
    .groupBy("failure_category")
    .agg(
        F.count("*").alias("failure_count")
    )
    .orderBy(F.desc("failure_count"))
)

display(failure_summary)


# COMMAND ----------


# ============================================================
# STEP 7: FAILURE ANALYSIS DETAIL REPORT
# ============================================================

failure_detail_report = (
    failure_categories
    .groupBy(
        "run_id",
        "target_table",
        "source_file",
        "failure_category"
    )
    .agg(
        F.count("*").alias("failed_attempts")
    )
    .orderBy(
        F.desc("failed_attempts"),
        "target_table"
    )
)

display(failure_detail_report)

print(
    "Total failure detail groups:",
    failure_detail_report.count()
)


# COMMAND ----------


# ============================================================
# STEP 8: DATASET-LEVEL MONITORING REPORT
# ============================================================

dataset_monitoring_report = (
    latest_bronze_snapshot
    .select(
        "table_name",
        "record_count",
        "table_status",
        "has_run_id",
        "has_source_file",
        "has_ingestion_timestamp"
    )
    .withColumn(
        "metadata_status",
        F.when(
            F.col("has_run_id") &
            F.col("has_source_file") &
            F.col("has_ingestion_timestamp"),
            "COMPLIANT"
        ).otherwise("NON_COMPLIANT")
    )
    .orderBy("table_name")
)

display(dataset_monitoring_report)

print(
    "Total monitored datasets:",
    dataset_monitoring_report.count()
)


# COMMAND ----------


# ============================================================
# STEP 9: PIPELINE EXECUTION VISUALIZATION
# ============================================================

execution_chart_data = (
    dashboard_execution_report
    .select(
        "run_id",
        "successful_executions",
        "failed_executions"
    )
)

display(execution_chart_data)


# COMMAND ----------


# ============================================================
# STEP 10: BRONZE DATASET RECORD COUNT VISUALIZATION
# ============================================================

bronze_dataset_chart = (
    dataset_monitoring_report
    .select(
        "table_name",
        "record_count"
    )
    .orderBy(F.desc("record_count"))
)

display(bronze_dataset_chart)


# COMMAND ----------


# ============================================================
# STEP 11: MONITORING TREND VISUALIZATION
# ============================================================

monitoring_trend_chart = (
    dashboard_snapshots
    .groupBy("snapshot_timestamp")
    .agg(
        F.count("*").alias("total_tables"),

        F.sum(
            F.when(
                F.col("table_status") == "SUCCESS", 1
            ).otherwise(0)
        ).alias("healthy_tables"),

        F.sum("record_count").alias("total_records")
    )
    .withColumn(
        "health_percentage",
        F.round(
            F.col("healthy_tables") /
            F.col("total_tables") * 100,
            2
        )
    )
    .orderBy("snapshot_timestamp")
)

display(monitoring_trend_chart)


# COMMAND ----------


# ============================================================
# STEP 12: FINAL OBSERVABILITY DASHBOARD SUMMARY
# ============================================================

print("=" * 65)
print("       INDIA URBAN MOBILITY PLATFORM")
print("       PIPELINE OBSERVABILITY DASHBOARD")
print("=" * 65)

print("\n[1] CURRENT BRONZE HEALTH")
print("-" * 45)
print("Total datasets       :", total_tables)
print("Healthy datasets     :", healthy_tables)
print("Failed datasets      :", failed_tables)
print("Total records        :", total_records)
print("Health percentage    :", bronze_health_percentage, "%")

print("\n[2] HISTORICAL PIPELINE EXECUTIONS")
print("-" * 45)
print("Total pipeline runs  :", historical_run_count)
print("Successful runs      :", successful_run_count)
print("Failed runs          :", failed_run_count)

print("\n[3] FAILURE ANALYSIS")
print("-" * 45)
print("Failed audit records :", dashboard_failures.count())

display(failure_summary)

print("\n[4] DATASET MONITORING")
print("-" * 45)
display(dataset_monitoring_report)

print("\n[5] MONITORING HISTORY")
print("-" * 45)
display(monitoring_trend_chart)

print("\n" + "=" * 65)
print("       DASHBOARD SUMMARY GENERATED")
print("=" * 65)


# COMMAND ----------

# ============================================================
# STEP 13: FINAL DASHBOARD VALIDATION
# ============================================================

validation_results = [
    ("Bronze datasets", total_tables == 10),
    ("Healthy Bronze datasets", healthy_tables == 10),
    ("Bronze record count", total_records == 70478),
    ("Historical pipeline runs", historical_run_count == 3),
    ("Successful historical runs", successful_run_count == 1),
    ("Failed historical runs", failed_run_count == 2),
    ("Failure audit records", dashboard_failures.count() == 19),
    ("Failure detail groups", failure_detail_report.count() == 10),
    ("Monitoring snapshots", dashboard_snapshots.count() == 20)
]

passed = sum(1 for _, result in validation_results if result)
failed = len(validation_results) - passed

for check_name, result in validation_results:
    print(f"{'PASS' if result else 'FAIL'} | {check_name}")

print("-" * 55)
print("Total checks:", len(validation_results))
print("Passed:", passed)
print("Failed:", failed)

print(
    "FINAL STATUS:",
    "SUCCESS" if failed == 0 else "REQUIRES ATTENTION"
)

# COMMAND ----------

