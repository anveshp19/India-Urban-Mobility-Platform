# Databricks notebook source

# ============================================================
# INDIA URBAN MOBILITY PLATFORM
# MODULE 14: PRODUCTION DATA QUALITY & TESTING
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timezone
import uuid

# Project configuration
CATALOG = "anvesh"
SCHEMA = "default"

PIPELINE_NAME = "india_urban_mobility_pipeline"

MODULE_NAME = "production_data_quality_testing"

# Database configuration
DATABASE = f"{CATALOG}.{SCHEMA}"

# Pipeline execution ID
TEST_RUN_ID = str(uuid.uuid4())

print("=" * 60)
print("INDIA URBAN MOBILITY PLATFORM")
print("MODULE 14: PRODUCTION DATA QUALITY & TESTING")
print("=" * 60)

print("Catalog:", CATALOG)
print("Schema:", SCHEMA)
print("Pipeline:", PIPELINE_NAME)
print("Module:", MODULE_NAME)
print("Test Run ID:", TEST_RUN_ID)

print("\nProduction testing environment initialized.")


# COMMAND ----------


# ============================================================
# STEP 2: PRODUCTION PROJECT CONFIGURATION
# ============================================================

# Bronze tables
BRONZE_TABLES = [
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

# Silver tables
SILVER_TABLES = [
    "silver_agency",
    "silver_calendar",
    "silver_fare_attributes",
    "silver_fare_rules",
    "silver_feed_info",
    "silver_routes",
    "silver_shapes",
    "silver_stop_times",
    "silver_stops",
    "silver_trips"
]

# Gold tables
GOLD_TABLES = [
    "gold_dim_station",
    "gold_dim_route",
    "gold_dim_date",
    "gold_fact_trip",
    "gold_fact_stop_schedule",
    "gold_route_summary",
    "gold_station_summary",
    "gold_fare_summary"
]

# Fully qualified table names
def get_table_name(table_name):
    return f"{CATALOG}.{SCHEMA}.{table_name}"

print("=" * 55)
print("PRODUCTION DATA QUALITY CONFIGURATION")
print("=" * 55)

print("Bronze tables:", len(BRONZE_TABLES))
print("Silver tables:", len(SILVER_TABLES))
print("Gold tables:", len(GOLD_TABLES))

print("Total configured tables:",
      len(BRONZE_TABLES) + len(SILVER_TABLES) + len(GOLD_TABLES))

print("\nSample table:", get_table_name("bronze_stops"))


# COMMAND ----------


# ============================================================
# STEP 3: REUSABLE TABLE VALIDATION FRAMEWORK
# ============================================================

def validate_table_exists(table_name):

    full_table_name = get_table_name(table_name)

    try:
        df = spark.table(full_table_name)

        record_count = df.count()
        column_count = len(df.columns)

        if record_count > 0:
            status = "PASS"
            message = "Table exists and contains records"
        else:
            status = "FAIL"
            message = "Table exists but contains no records"

        return {
            "table_name": full_table_name,
            "record_count": record_count,
            "column_count": column_count,
            "status": status,
            "message": message
        }

    except Exception as e:

        return {
            "table_name": full_table_name,
            "record_count": 0,
            "column_count": 0,
            "status": "ERROR",
            "message": str(e)[:500]
        }


# Test the function
test_result = validate_table_exists("bronze_stops")

print("TABLE VALIDATION RESULT")
print("-" * 45)

for key, value in test_result.items():
    print(f"{key}: {value}")


# COMMAND ----------


# ============================================================
# STEP 4: VALIDATE ALL 28 PROJECT TABLES
# ============================================================

ALL_TABLES = (
    BRONZE_TABLES +
    SILVER_TABLES +
    GOLD_TABLES
)

validation_results = []

for table_name in ALL_TABLES:

    result = validate_table_exists(table_name)

    validation_results.append(result)

# Convert results into a Spark DataFrame
table_validation_df = spark.createDataFrame(validation_results)

display(
    table_validation_df.orderBy("table_name")
)

# Summary
total_tables = len(validation_results)

passed_tables = sum(
    1 for result in validation_results
    if result["status"] == "PASS"
)

failed_tables = sum(
    1 for result in validation_results
    if result["status"] == "FAIL"
)

error_tables = sum(
    1 for result in validation_results
    if result["status"] == "ERROR"
)

print("=" * 50)
print("PROJECT TABLE VALIDATION SUMMARY")
print("=" * 50)
print("Total tables:", total_tables)
print("Passed:", passed_tables)
print("Failed:", failed_tables)
print("Errors:", error_tables)
print("=" * 50)


# COMMAND ----------


# ============================================================
# STEP 5: COLUMN-LEVEL DATA QUALITY VALIDATION
# ============================================================

REQUIRED_COLUMNS = {
    "bronze_stops": [
        "stop_id",
        "stop_name"
    ],

    "bronze_routes": [
        "route_id",
        "route_type"
    ],

    "bronze_trips": [
        "trip_id",
        "route_id",
        "service_id"
    ],

    "bronze_stop_times": [
        "trip_id",
        "stop_id",
        "stop_sequence"
    ]
}


def validate_required_columns(table_name, required_columns):

    full_table_name = get_table_name(table_name)
    df = spark.table(full_table_name)

    existing_columns = set(df.columns)

    results = []

    for column_name in required_columns:

        if column_name not in existing_columns:

            results.append({
                "table_name": table_name,
                "column_name": column_name,
                "null_count": None,
                "status": "MISSING_COLUMN"
            })

        else:

            null_count = df.filter(
                F.col(column_name).isNull()
            ).count()

            results.append({
                "table_name": table_name,
                "column_name": column_name,
                "null_count": null_count,
                "status": "PASS" if null_count == 0 else "FAIL"
            })

    return results


# Execute column-level validations
column_quality_results = []

for table_name, columns in REQUIRED_COLUMNS.items():

    results = validate_required_columns(
        table_name,
        columns
    )

    column_quality_results.extend(results)


column_quality_df = spark.createDataFrame(
    column_quality_results
)

display(column_quality_df.orderBy("table_name", "column_name"))


# COMMAND ----------


# ============================================================
# STEP 6: DUPLICATE RECORD VALIDATION
# ============================================================

PRIMARY_KEYS = {
    "bronze_stops": ["stop_id"],
    "bronze_routes": ["route_id"],
    "bronze_trips": ["trip_id"],
    "bronze_stop_times": ["trip_id", "stop_sequence"]
}


def validate_duplicates(table_name, key_columns):

    full_table_name = get_table_name(table_name)
    df = spark.table(full_table_name)

    duplicate_count = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    return {
        "table_name": table_name,
        "key_columns": ", ".join(key_columns),
        "duplicate_groups": duplicate_count,
        "status": "PASS" if duplicate_count == 0 else "FAIL"
    }


duplicate_results = []

for table_name, keys in PRIMARY_KEYS.items():

    result = validate_duplicates(table_name, keys)
    duplicate_results.append(result)


duplicate_validation_df = spark.createDataFrame(
    duplicate_results
)

display(duplicate_validation_df)


# COMMAND ----------


# ============================================================
# STEP 7: REFERENTIAL INTEGRITY VALIDATION
# ============================================================

def validate_foreign_key(
    child_table,
    child_column,
    parent_table,
    parent_column
):

    child_df = spark.table(get_table_name(child_table))
    parent_df = spark.table(get_table_name(parent_table))

    parent_keys = (
        parent_df
        .select(F.col(parent_column).alias("parent_key"))
        .filter(F.col("parent_key").isNotNull())
        .distinct()
    )

    orphan_count = (
        child_df
        .filter(F.col(child_column).isNotNull())
        .join(
            parent_keys,
            F.col(child_column) == F.col("parent_key"),
            "left_anti"
        )
        .count()
    )

    return {
        "child_table": child_table,
        "child_column": child_column,
        "parent_table": parent_table,
        "parent_column": parent_column,
        "orphan_count": orphan_count,
        "status": "PASS" if orphan_count == 0 else "FAIL"
    }


FOREIGN_KEY_RULES = [
    ("bronze_trips", "route_id", "bronze_routes", "route_id"),
    ("bronze_trips", "service_id", "bronze_calendar", "service_id"),
    ("bronze_stop_times", "trip_id", "bronze_trips", "trip_id"),
    ("bronze_stop_times", "stop_id", "bronze_stops", "stop_id")
]

referential_results = []

for rule in FOREIGN_KEY_RULES:

    result = validate_foreign_key(*rule)
    referential_results.append(result)


referential_validation_df = spark.createDataFrame(
    referential_results
)

display(referential_validation_df)


# COMMAND ----------


# ============================================================
# STEP 8: BUSINESS RULE VALIDATION
# ============================================================

def validate_business_rule(
    table_name,
    rule_name,
    invalid_condition
):

    df = spark.table(get_table_name(table_name))

    invalid_count = df.filter(invalid_condition).count()

    return {
        "table_name": table_name,
        "rule_name": rule_name,
        "invalid_records": invalid_count,
        "status": "PASS" if invalid_count == 0 else "FAIL"
    }


business_rule_results = []

# Rule 1: Valid GTFS route type
business_rule_results.append(
    validate_business_rule(
        "bronze_routes",
        "route_type must be between 0 and 7",
        (F.col("route_type") < 0) | (F.col("route_type") > 7)
    )
)

# Rule 2: Stop sequence must be positive
business_rule_results.append(
    validate_business_rule(
        "bronze_stop_times",
        "stop_sequence must be greater than zero",
        F.col("stop_sequence") <= 0
    )
)

# Rule 3: Valid latitude
business_rule_results.append(
    validate_business_rule(
        "bronze_stops",
        "stop_lat must be between -90 and 90",
        (F.col("stop_lat") < -90) | (F.col("stop_lat") > 90)
    )
)

# Rule 4: Valid longitude
business_rule_results.append(
    validate_business_rule(
        "bronze_stops",
        "stop_lon must be between -180 and 180",
        (F.col("stop_lon") < -180) | (F.col("stop_lon") > 180)
    )
)

# Rule 5: Arrival time cannot be null
business_rule_results.append(
    validate_business_rule(
        "bronze_stop_times",
        "arrival_time must not be null",
        F.col("arrival_time").isNull()
    )
)

# Rule 6: Departure time cannot be null
business_rule_results.append(
    validate_business_rule(
        "bronze_stop_times",
        "departure_time must not be null",
        F.col("departure_time").isNull()
    )
)

business_rule_df = spark.createDataFrame(
    business_rule_results
)

display(business_rule_df)


# COMMAND ----------


# ============================================================
# STEP 9: CONSOLIDATED DATA QUALITY TEST REPORT
# ============================================================

# Normalize table validation results
table_tests = table_validation_df.select(
    F.lit("TABLE_VALIDATION").alias("test_category"),
    F.col("table_name"),
    F.col("status"),
    F.col("message").alias("details")
)

# Normalize required-column results
column_tests = column_quality_df.select(
    F.lit("COLUMN_VALIDATION").alias("test_category"),
    F.col("table_name"),
    F.col("status"),
    F.concat(
        F.lit("Column: "),
        F.col("column_name"),
        F.lit(" | Null count: "),
        F.col("null_count").cast("string")
    ).alias("details")
)

# Normalize duplicate results
duplicate_tests = duplicate_validation_df.select(
    F.lit("DUPLICATE_VALIDATION").alias("test_category"),
    F.col("table_name"),
    F.col("status"),
    F.concat(
        F.lit("Keys: "),
        F.col("key_columns"),
        F.lit(" | Duplicate groups: "),
        F.col("duplicate_groups").cast("string")
    ).alias("details")
)

# Normalize referential integrity results
referential_tests = referential_validation_df.select(
    F.lit("REFERENTIAL_INTEGRITY").alias("test_category"),
    F.col("child_table").alias("table_name"),
    F.col("status"),
    F.concat(
        F.col("child_column"),
        F.lit(" -> "),
        F.col("parent_table"),
        F.lit("."),
        F.col("parent_column"),
        F.lit(" | Orphans: "),
        F.col("orphan_count").cast("string")
    ).alias("details")
)

# Normalize business rule results
business_tests = business_rule_df.select(
    F.lit("BUSINESS_RULE").alias("test_category"),
    F.col("table_name"),
    F.col("status"),
    F.concat(
        F.col("rule_name"),
        F.lit(" | Invalid records: "),
        F.col("invalid_records").cast("string")
    ).alias("details")
)

# Combine all test categories
all_quality_tests = (
    table_tests
    .unionByName(column_tests)
    .unionByName(duplicate_tests)
    .unionByName(referential_tests)
    .unionByName(business_tests)
)

display(all_quality_tests.orderBy("test_category", "table_name"))

# Overall summary
all_quality_tests.groupBy("test_category", "status") \
    .count() \
    .orderBy("test_category", "status") \
    .show(truncate=False)


# COMMAND ----------


# ============================================================
# STEP 10: AUTOMATED DATA QUALITY TEST ENGINE
# ============================================================

def execute_data_quality_tests():

    test_results = []

    # Execute all validation categories
    validation_datasets = {
        "TABLE_VALIDATION": table_validation_df,
        "COLUMN_VALIDATION": column_quality_df,
        "DUPLICATE_VALIDATION": duplicate_validation_df,
        "REFERENTIAL_INTEGRITY": referential_validation_df,
        "BUSINESS_RULE": business_rule_df
    }

    for category, validation_df in validation_datasets.items():

        category_results = (
            validation_df
            .groupBy("status")
            .count()
            .collect()
        )

        for row in category_results:

            test_results.append({
                "test_category": category,
                "status": row["status"],
                "test_count": row["count"]
            })

    results_df = spark.createDataFrame(test_results)

    summary = results_df.agg(
        F.sum("test_count").alias("total_checks"),

        F.sum(
            F.when(
                F.col("status") == "PASS",
                F.col("test_count")
            ).otherwise(0)
        ).alias("passed_checks"),

        F.sum(
            F.when(
                F.col("status") != "PASS",
                F.col("test_count")
            ).otherwise(0)
        ).alias("failed_checks")
    ).first()

    total_checks = summary["total_checks"] or 0
    passed_checks = summary["passed_checks"] or 0
    failed_checks = summary["failed_checks"] or 0

    overall_status = (
        "PASS"
        if total_checks > 0 and failed_checks == 0
        else "FAIL"
    )

    print("=" * 55)
    print("AUTOMATED DATA QUALITY TEST REPORT")
    print("=" * 55)

    print("Total checks:", total_checks)
    print("Passed checks:", passed_checks)
    print("Failed checks:", failed_checks)
    print("Overall status:", overall_status)

    print("=" * 55)

    return results_df, {
        "total_checks": total_checks,
        "passed_checks": passed_checks,
        "failed_checks": failed_checks,
        "overall_status": overall_status
    }


automated_test_results, test_summary = (
    execute_data_quality_tests()
)

display(automated_test_results)


# COMMAND ----------


# ============================================================
# STEP 11: DATA QUALITY AUDIT TABLE
# ============================================================

AUDIT_TABLE = "anvesh.default.data_quality_test_audit"

# Add execution metadata to test results
audit_results_df = (
    all_quality_tests
    .withColumn("test_run_id", F.lit(TEST_RUN_ID))
    .withColumn("pipeline_name", F.lit(PIPELINE_NAME))
    .withColumn("module_name", F.lit(MODULE_NAME))
    .withColumn("execution_timestamp", F.current_timestamp())
)

# Persist results in Delta Lake
(
    audit_results_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(AUDIT_TABLE)
)

print("Data quality audit records saved successfully.")

# Verify saved records for this execution
saved_audit_df = spark.table(AUDIT_TABLE).filter(
    F.col("test_run_id") == TEST_RUN_ID
)

print("Saved audit records:", saved_audit_df.count())

display(
    saved_audit_df.orderBy("test_category", "table_name")
)


# COMMAND ----------


# ============================================================
# STEP 12: PRODUCTION DATA QUALITY GATE
# ============================================================

def validate_data_quality_gate(test_run_id):

    audit_df = spark.table(
        "anvesh.default.data_quality_test_audit"
    ).filter(
        F.col("test_run_id") == test_run_id
    )

    total_checks = audit_df.count()

    failed_checks = audit_df.filter(
        F.col("status") != "PASS"
    ).count()

    passed_checks = total_checks - failed_checks

    if total_checks == 0:
        gate_status = "BLOCKED"
        message = "No test results available"

    elif failed_checks > 0:
        gate_status = "BLOCKED"
        message = "Data quality failures detected"

    else:
        gate_status = "APPROVED"
        message = "All data quality checks passed"

    print("=" * 55)
    print("PRODUCTION DATA QUALITY GATE")
    print("=" * 55)

    print("Test Run ID:", test_run_id)
    print("Total checks:", total_checks)
    print("Passed checks:", passed_checks)
    print("Failed checks:", failed_checks)
    print("Gate status:", gate_status)
    print("Message:", message)

    print("=" * 55)

    return gate_status


quality_gate_status = validate_data_quality_gate(
    TEST_RUN_ID
)


# COMMAND ----------


# ============================================================
# STEP 13: QUALITY GATE FAILURE SIMULATION
# ============================================================

# Create an isolated test run ID
SIMULATED_RUN_ID = str(uuid.uuid4())

# Create one simulated failed test
simulated_failure = [{
    "test_category": "SIMULATED_TEST",
    "table_name": "bronze_stops",
    "status": "FAIL",
    "details": "Simulated failure for quality gate testing",
    "test_run_id": SIMULATED_RUN_ID,
    "pipeline_name": PIPELINE_NAME,
    "module_name": MODULE_NAME,
    "execution_timestamp": datetime.now(timezone.utc)
}]

simulated_failure_df = spark.createDataFrame(
    simulated_failure,
    schema=spark.table(
        "anvesh.default.data_quality_test_audit"
    ).schema
)

# Persist only the simulated test record
(
    simulated_failure_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("anvesh.default.data_quality_test_audit")
)

print("Simulated failure record saved.")

# Test the quality gate
simulated_gate_status = validate_data_quality_gate(
    SIMULATED_RUN_ID
)

print("Expected status: BLOCKED")
print("Actual status:", simulated_gate_status)


# COMMAND ----------


# ============================================================
# STEP 14: NO-RESULTS QUALITY GATE TEST
# ============================================================

EMPTY_RUN_ID = str(uuid.uuid4())

empty_gate_status = validate_data_quality_gate(
    EMPTY_RUN_ID
)

print("Expected status: BLOCKED")
print("Actual status:", empty_gate_status)

assert empty_gate_status == "BLOCKED", (
    "Quality gate incorrectly approved a run without test results"
)

print("\nNO-RESULTS TEST PASSED")


# COMMAND ----------


# ============================================================
# STEP 15: FINAL PRODUCTION TESTING REPORT
# ============================================================

audit_table = spark.table(
    "anvesh.default.data_quality_test_audit"
)

# Retrieve actual test runs
run_summary = (
    audit_table
    .groupBy("test_run_id")
    .agg(
        F.count("*").alias("total_checks"),

        F.sum(
            F.when(F.col("status") == "PASS", 1)
             .otherwise(0)
        ).alias("passed_checks"),

        F.sum(
            F.when(F.col("status") != "PASS", 1)
             .otherwise(0)
        ).alias("failed_checks"),

        F.max("execution_timestamp").alias("last_execution")
    )
    .withColumn(
        "test_status",
        F.when(
            F.col("failed_checks") == 0,
            "PASS"
        ).otherwise("FAIL")
    )
)

# Current actual test execution
current_run = run_summary.filter(
    F.col("test_run_id") == TEST_RUN_ID
)

print("=" * 60)
print("PRODUCTION DATA QUALITY TESTING REPORT")
print("=" * 60)

display(current_run)

print("\nVALIDATION CATEGORY SUMMARY")

display(
    all_quality_tests
    .groupBy("test_category", "status")
    .count()
    .orderBy("test_category", "status")
)

print("\nQUALITY GATE SCENARIO RESULTS")

print("Positive test:", quality_gate_status)
print("Negative test:", simulated_gate_status)
print("Empty-result test:", empty_gate_status)

print("\n" + "=" * 60)
print("PRODUCTION TESTING REPORT GENERATED")
print("=" * 60)


# COMMAND ----------

