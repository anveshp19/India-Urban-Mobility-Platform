# Databricks notebook source
# INDIA URBAN MOBILITY PLATFORM

# GOLD LAYER - DATA QUALITY VALIDATION

## Objective
Validate Gold layer tables before making data available for analytics.

## Validation Scope
1. Table existence
2. Primary key uniqueness
3. NULL checks
4. Referential integrity
5. Source-to-target reconciliation

## Catalog
anvesh.default

# COMMAND ----------

from pyspark.sql.functions import col

expected_gold_tables = [
    "gold_dim_station",
    "gold_dim_route",
    "gold_dim_date",
    "gold_fact_trip",
    "gold_fact_stop_schedule",
    "gold_route_summary",
    "gold_station_summary",
    "gold_fare_summary"
]

existing_tables = {
    row["tableName"]
    for row in spark.sql("SHOW TABLES IN anvesh.default").collect()
}

for table_name in expected_gold_tables:
    if table_name in existing_tables:
        print(f"PASS: {table_name} exists")
    else:
        print(f"FAIL: {table_name} is missing")

# COMMAND ----------

from pyspark.sql.functions import col

def validate_primary_key(table_name, primary_key):

    df = spark.table(f"anvesh.default.{table_name}")

    total_records = df.count()

    unique_records = (
        df.select(primary_key)
        .distinct()
        .count()
    )

    null_records = (
        df.filter(col(primary_key).isNull())
        .count()
    )

    duplicate_records = total_records - unique_records

    status = (
        "PASS"
        if duplicate_records == 0 and null_records == 0
        else "FAIL"
    )

    print(f"\nTable: {table_name}")
    print(f"Total Records: {total_records}")
    print(f"Duplicate Records: {duplicate_records}")
    print(f"Null Primary Keys: {null_records}")
    print(f"Status: {status}")

    return status

# COMMAND ----------

primary_key_checks = {
    "gold_dim_station": "station_id",
    "gold_dim_route": "route_id",
    "gold_dim_date": "date_key",
    "gold_fact_trip": "trip_id",
    "gold_fact_stop_schedule": "trip_id"
}

validation_results = {}

for table_name, primary_key in primary_key_checks.items():

    validation_results[table_name] = validate_primary_key(
        table_name,
        primary_key
    )

# COMMAND ----------

from pyspark.sql.functions import col

schedule_df = spark.table("anvesh.default.gold_fact_stop_schedule")

total_records = schedule_df.count()

unique_records = (
    schedule_df
    .select("trip_id", "stop_sequence")
    .distinct()
    .count()
)

null_records = (
    schedule_df
    .filter(
        col("trip_id").isNull() |
        col("stop_sequence").isNull()
    )
    .count()
)

duplicate_records = total_records - unique_records

status = (
    "PASS"
    if duplicate_records == 0 and null_records == 0
    else "FAIL"
)

print("Table: gold_fact_stop_schedule")
print(f"Total Records: {total_records}")
print(f"Duplicate Composite Keys: {duplicate_records}")
print(f"Null Composite Keys: {null_records}")
print(f"Status: {status}")

# COMMAND ----------

from pyspark.sql.functions import col

def validate_composite_key(table_name, primary_keys):

    df = spark.table(f"anvesh.default.{table_name}")

    total_records = df.count()

    unique_records = (
        df.select(*primary_keys)
        .distinct()
        .count()
    )

    null_condition = None

    for key in primary_keys:
        condition = col(key).isNull()
        null_condition = (
            condition if null_condition is None
            else null_condition | condition
        )

    null_records = df.filter(null_condition).count()

    duplicate_records = total_records - unique_records

    status = (
        "PASS"
        if duplicate_records == 0 and null_records == 0
        else "FAIL"
    )

    print(f"\nTable: {table_name}")
    print(f"Total Records: {total_records}")
    print(f"Duplicate Keys: {duplicate_records}")
    print(f"Null Keys: {null_records}")
    print(f"Status: {status}")

    return status

# COMMAND ----------

remaining_checks = {
    "gold_route_summary": ["route_id"],
    "gold_station_summary": ["station_id"],
    "gold_fare_summary": [
        "origin_id",
        "destination_id",
        "fare_id"
    ]
}

remaining_results = {}

for table_name, keys in remaining_checks.items():

    remaining_results[table_name] = validate_composite_key(
        table_name,
        keys
    )

# COMMAND ----------

from pyspark.sql.functions import col

null_checks = {
    "gold_dim_station": [
        "station_id",
        "station_name"
    ],
    "gold_dim_route": [
        "route_id",
        "route_short_name"
    ],
    "gold_dim_date": [
        "date_key",
        "full_date"
    ],
    "gold_fact_trip": [
        "trip_id",
        "route_id",
        "service_id"
    ],
    "gold_fact_stop_schedule": [
        "trip_id",
        "route_id",
        "stop_id",
        "stop_sequence"
    ],
    "gold_route_summary": [
        "route_id",
        "total_trips"
    ],
    "gold_station_summary": [
        "station_id",
        "total_stop_events"
    ],
    "gold_fare_summary": [
        "origin_id",
        "destination_id",
        "fare_id",
        "price"
    ]
}

for table_name, columns in null_checks.items():

    df = spark.table(f"anvesh.default.{table_name}")

    print(f"\nTable: {table_name}")

    for column_name in columns:

        null_count = df.filter(
            col(column_name).isNull()
        ).count()

        print(f"{column_name}: {null_count}")

# COMMAND ----------

from pyspark.sql.functions import col

referential_checks = [
    {
        "name": "Trip to Route",
        "source": "gold_fact_trip",
        "source_key": "route_id",
        "target": "gold_dim_route",
        "target_key": "route_id"
    },
    {
        "name": "Stop Schedule to Trip",
        "source": "gold_fact_stop_schedule",
        "source_key": "trip_id",
        "target": "gold_fact_trip",
        "target_key": "trip_id"
    },
    {
        "name": "Stop Schedule to Station",
        "source": "gold_fact_stop_schedule",
        "source_key": "stop_id",
        "target": "silver_stops",
        "target_key": "stop_id"
    },
    {
        "name": "Route Summary to Route",
        "source": "gold_route_summary",
        "source_key": "route_id",
        "target": "gold_dim_route",
        "target_key": "route_id"
    },
    {
        "name": "Station Summary to Station",
        "source": "gold_station_summary",
        "source_key": "station_id",
        "target": "gold_dim_station",
        "target_key": "station_id"
    }
]

for check in referential_checks:

    source_df = spark.table(
        f"anvesh.default.{check['source']}"
    )

    target_df = spark.table(
        f"anvesh.default.{check['target']}"
    )

    orphan_records = source_df.join(
        target_df.select(
            col(check["target_key"]).alias("reference_key")
        ),
        source_df[check["source_key"]] ==
        col("reference_key"),
        "left_anti"
    )

    orphan_count = orphan_records.count()

    status = "PASS" if orphan_count == 0 else "FAIL"

    print(f"\nCheck: {check['name']}")
    print(f"Orphan Records: {orphan_count}")
    print(f"Status: {status}")

# COMMAND ----------

reconciliation_checks = [
    ("silver_stops", "gold_dim_station", 57),
    ("silver_routes", "gold_dim_route", 3),
    ("silver_calendar", "gold_dim_date", 1279),
    ("silver_trips", "gold_fact_trip", 2820),
    ("silver_stop_times", "gold_fact_stop_schedule", 61236),
    ("silver_routes", "gold_route_summary", 3),
    ("gold_dim_station", "gold_station_summary", 57),
    ("silver_fare_rules", "gold_fare_summary", 3249)
]

for source_table, target_table, expected_count in reconciliation_checks:

    source_count = spark.table(
        f"anvesh.default.{source_table}"
    ).count()

    target_count = spark.table(
        f"anvesh.default.{target_table}"
    ).count()

    status = (
        "PASS"
        if target_count == expected_count
        else "FAIL"
    )

    print(f"\nSource: {source_table}")
    print(f"Target: {target_table}")
    print(f"Source Records: {source_count}")
    print(f"Target Records: {target_count}")
    print(f"Expected Gold Records: {expected_count}")
    print(f"Status: {status}")

# COMMAND ----------

