# Databricks notebook source
# INDIA URBAN MOBILITY PLATFORM

# GOLD LAYER - SILVER TO GOLD

## Project: Hyderabad Metro GTFS Analytics

### Objectives
- Create business-ready analytical datasets.
- Build dimension and fact tables.
- Implement aggregations and business metrics.
- Store Gold datasets as Delta tables.
- Prepare data for Power BI dashboards.

### Source
Unity Catalog: anvesh.default

### Target
Unity Catalog: anvesh.default

# COMMAND ----------

silver_stops = spark.table(
    "anvesh.default.silver_stops"
)

print("Silver Stops:", silver_stops.count())

silver_stops.groupBy(
    "location_type"
).count().orderBy(
    "location_type"
).show()

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp

gold_dim_station = (
    silver_stops
    .filter(col("location_type") == 1)
    .select(
        trim(col("stop_id")).alias("station_id"),
        trim(col("stop_name")).alias("station_name"),
        col("stop_lat"),
        col("stop_lon"),
        col("zone_id")
    )
    .withColumn("created_at", current_timestamp())
)

# COMMAND ----------

print("Total Stations:", gold_dim_station.count())

print(
    "Duplicate Station IDs:",
    gold_dim_station.count()
    - gold_dim_station.select("station_id").distinct().count()
)

print("Null Station IDs:",
      gold_dim_station.filter(col("station_id").isNull()).count())

print("Null Station Names:",
      gold_dim_station.filter(col("station_name").isNull()).count())

# COMMAND ----------

gold_dim_station.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.gold_dim_station")

# COMMAND ----------

saved_station = spark.table("anvesh.default.gold_dim_station")

print("Saved Gold Station Count:", saved_station.count())

saved_station.printSchema()

# COMMAND ----------

silver_routes = spark.table(
    "anvesh.default.silver_routes"
)

print("Total Routes:", silver_routes.count())

silver_routes.printSchema()

silver_routes.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp

gold_dim_route = (
    silver_routes
    .select(
        trim(col("route_id")).alias("route_id"),
        trim(col("agency_id")).alias("agency_id"),
        trim(col("route_short_name")).alias("route_short_name"),
        trim(col("route_long_name")).alias("route_long_name"),
        col("route_type").cast("integer").alias("route_type"),
        trim(col("route_text_color")).alias("route_text_color"),
        col("route_sort_order").cast("integer").alias("route_sort_order")
    )
    .withColumn("created_at", current_timestamp())
)

# COMMAND ----------

print("Total Routes:", gold_dim_route.count())

print(
    "Duplicate Route IDs:",
    gold_dim_route.count()
    - gold_dim_route.select("route_id").distinct().count()
)

print(
    "Null Route IDs:",
    gold_dim_route.filter(col("route_id").isNull()).count()
)

print(
    "Null Route Names:",
    gold_dim_route.filter(col("route_long_name").isNull()).count()
)

gold_dim_route.show(truncate=False)

# COMMAND ----------

gold_dim_route.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.gold_dim_route")

# COMMAND ----------

saved_route = spark.table("anvesh.default.gold_dim_route")

print("Saved Gold Route Count:", saved_route.count())

saved_route.show(truncate=False)

# COMMAND ----------

silver_calendar = spark.table(
    "anvesh.default.silver_calendar"
)

print("Calendar Records:", silver_calendar.count())

silver_calendar.printSchema()

silver_calendar.show(truncate=False)

# COMMAND ----------

silver_calendar.select(
    "service_id",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
    "start_date",
    "end_date"
).orderBy("service_id").show(truncate=False)


# COMMAND ----------

silver_calendar.selectExpr(
    "min(start_date) as min_start_date",
    "max(end_date) as max_end_date"
).show()

# COMMAND ----------

from pyspark.sql.functions import expr

from pyspark.sql.functions import (
    col,
    sequence,
    explode,
    year,
    month,
    quarter,
    dayofmonth,
    dayofweek,
    date_format,
    when,
    lit,
    current_timestamp
)

gold_dim_date = (
    silver_calendar
    .select(
        explode(
            sequence(
                col("start_date"),
                col("end_date"),
                expr("INTERVAL 1 DAY")
            )
        ).alias("full_date")
    )
    .distinct()
    .select(
        col("full_date"),
        date_format("full_date", "yyyyMMdd").cast("integer").alias("date_key"),
        year("full_date").alias("year"),
        quarter("full_date").alias("quarter"),
        month("full_date").alias("month"),
        date_format("full_date", "MMMM").alias("month_name"),
        dayofmonth("full_date").alias("day"),
        dayofweek("full_date").alias("day_of_week"),
        date_format("full_date", "EEEE").alias("day_name"),
        when(dayofweek("full_date").isin(1, 7), lit("Weekend"))
        .otherwise(lit("Weekday"))
        .alias("day_type")
    )
    .withColumn("created_at", current_timestamp())
)

# COMMAND ----------

print("Total Dates:", gold_dim_date.count())

print(
    "Duplicate Date Keys:",
    gold_dim_date.count()
    - gold_dim_date.select("date_key").distinct().count()
)

print(
    "Null Date Keys:",
    gold_dim_date.filter(col("date_key").isNull()).count()
)

gold_dim_date.orderBy("full_date").show(10, truncate=False)

gold_dim_date.orderBy(col("full_date").desc()).show(5, truncate=False)

# COMMAND ----------

gold_dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.gold_dim_date")

# COMMAND ----------

saved_date = spark.table("anvesh.default.gold_dim_date")

print("Saved Gold Date Count:", saved_date.count())

saved_date.orderBy("full_date").show(5, truncate=False)

saved_date.orderBy(col("full_date").desc()).show(5, truncate=False)

# COMMAND ----------

silver_trips = spark.table(
    "anvesh.default.silver_trips"
)

print("Total Trips:", silver_trips.count())

silver_trips.printSchema()

silver_trips.show(10, truncate=False)

# COMMAND ----------

# Check whether every trip has a matching Gold route
orphan_route_count = (
    silver_trips
    .join(
        spark.table("anvesh.default.gold_dim_route")
        .select("route_id"),
        on="route_id",
        how="left_anti"
    )
    .count()
)

# Check whether every trip has a matching Silver service
orphan_service_count = (
    silver_trips
    .join(
        silver_calendar.select("service_id"),
        on="service_id",
        how="left_anti"
    )
    .count()
)

print("Trips without matching routes:", orphan_route_count)
print("Trips without matching services:", orphan_service_count)

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp

gold_fact_trip = (
    silver_trips
    .select(
        trim(col("trip_id")).alias("trip_id"),
        trim(col("route_id")).alias("route_id"),
        trim(col("service_id")).alias("service_id"),
        col("direction_id").cast("integer").alias("direction_id"),
        trim(col("trip_headsign")).alias("trip_headsign"),
        col("block_id"),
        col("shape_id")
    )
    .withColumn("created_at", current_timestamp())
)

# COMMAND ----------

print("Total Trips:", gold_fact_trip.count())

print(
    "Duplicate Trip IDs:",
    gold_fact_trip.count()
    - gold_fact_trip.select("trip_id").distinct().count()
)

print(
    "Null Trip IDs:",
    gold_fact_trip.filter(col("trip_id").isNull()).count()
)

print(
    "Null Route IDs:",
    gold_fact_trip.filter(col("route_id").isNull()).count()
)

gold_fact_trip.show(10, truncate=False)

# COMMAND ----------

gold_fact_trip.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.gold_fact_trip")

# COMMAND ----------

saved_trip = spark.table("anvesh.default.gold_fact_trip")

print("Saved Gold Trip Count:", saved_trip.count())

saved_trip.groupBy("route_id") \
    .count() \
    .orderBy("route_id") \
    .show()

# COMMAND ----------

silver_stop_times = spark.table(
    "anvesh.default.silver_stop_times"
)

print("Total Stop Time Records:", silver_stop_times.count())

silver_stop_times.printSchema()

silver_stop_times.show(10, truncate=False)

# COMMAND ----------

# Check stop times referencing missing trips
orphan_trip_count = (
    silver_stop_times
    .join(
        saved_trip.select("trip_id"),
        on="trip_id",
        how="left_anti"
    )
    .count()
)

# Check stop times referencing missing stops
orphan_stop_count = (
    silver_stop_times
    .join(
        spark.table("anvesh.default.silver_stops")
        .select("stop_id"),
        on="stop_id",
        how="left_anti"
    )
    .count()
)

print("Stop times without matching trips:", orphan_trip_count)
print("Stop times without matching stops:", orphan_stop_count)

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp

gold_fact_stop_schedule = (
    silver_stop_times.alias("st")
    .join(
        saved_trip.select("trip_id", "route_id").alias("t"),
        on="trip_id",
        how="inner"
    )
    .select(
        col("st.trip_id"),
        col("t.route_id"),
        trim(col("st.stop_id")).alias("stop_id"),
        col("st.stop_sequence"),
        col("st.arrival_time"),
        col("st.departure_time"),
        col("st.timepoint"),
        col("st.shape_dist_traveled")
    )
    .withColumn("created_at", current_timestamp())
)

# COMMAND ----------

print(
    "Total Stop Schedule Records:",
    gold_fact_stop_schedule.count()
)

print(
    "Duplicate Trip-Stop Sequences:",
    gold_fact_stop_schedule.count()
    - gold_fact_stop_schedule
        .select("trip_id", "stop_sequence")
        .distinct()
        .count()
)

print(
    "Null Trip IDs:",
    gold_fact_stop_schedule.filter(col("trip_id").isNull()).count()
)

print(
    "Null Stop IDs:",
    gold_fact_stop_schedule.filter(col("stop_id").isNull()).count()
)

gold_fact_stop_schedule.show(10, truncate=False)

# COMMAND ----------

gold_fact_stop_schedule.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.gold_fact_stop_schedule")

# COMMAND ----------

saved_stop_schedule = spark.table(
    "anvesh.default.gold_fact_stop_schedule"
)

print(
    "Saved Stop Schedule Records:",
    saved_stop_schedule.count()
)

saved_stop_schedule.groupBy("route_id") \
    .count() \
    .orderBy("route_id") \
    .show()

# COMMAND ----------

from pyspark.sql.functions import (
    countDistinct,
    count,
    round,
    current_timestamp
)

trip_metrics = (
    saved_trip
    .groupBy("route_id")
    .agg(
        countDistinct("trip_id").alias("total_trips")
    )
)

stop_metrics = (
    saved_stop_schedule
    .groupBy("route_id")
    .agg(
        count("*").alias("total_stop_events")
    )
)

gold_route_summary = (
    trip_metrics
    .join(stop_metrics, on="route_id", how="inner")
    .withColumn(
        "avg_stops_per_trip",
        round(
            col("total_stop_events") / col("total_trips"),
            2
        )
    )
    .withColumn("created_at", current_timestamp())
)

# COMMAND ----------

gold_route_summary.orderBy("route_id").show(truncate=False)

# COMMAND ----------

print("Total Routes:", gold_route_summary.count())

print(
    "Duplicate Route IDs:",
    gold_route_summary.count()
    - gold_route_summary.select("route_id").distinct().count()
)

print(
    "Null Route IDs:",
    gold_route_summary.filter(col("route_id").isNull()).count()
)

print(
    "Null Total Trips:",
    gold_route_summary.filter(col("total_trips").isNull()).count()
)

# COMMAND ----------

gold_route_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.gold_route_summary")

# COMMAND ----------

saved_route_summary = spark.table(
    "anvesh.default.gold_route_summary"
)

print("Saved Route Summary Count:", saved_route_summary.count())

saved_route_summary.orderBy("route_id").show(truncate=False)

# COMMAND ----------

saved_stops = spark.table(
    "anvesh.default.silver_stops"
)

print("Total Stops:", saved_stops.count())

print("Total Stop Schedule Records:", saved_stop_schedule.count())

# COMMAND ----------

from pyspark.sql.functions import (
    count,
    countDistinct,
    current_timestamp
)

gold_station_summary = (
    saved_stop_schedule
    .groupBy("stop_id")
    .agg(
        count("*").alias("total_stop_events"),
        countDistinct("trip_id").alias("total_trips"),
        countDistinct("route_id").alias("total_routes")
    )
    .join(
        saved_stops.select(
            "stop_id",
            "stop_name",
            "location_type",
            "stop_lat",
            "stop_lon"
        ),
        on="stop_id",
        how="left"
    )
    .withColumn("created_at", current_timestamp())
)

# COMMAND ----------

print("Total Summary Records:", gold_station_summary.count())

gold_station_summary.orderBy(
    col("total_stop_events").desc()
).show(10, truncate=False)

# COMMAND ----------

gold_station_summary.groupBy(
    "location_type"
).count().show()

saved_stops.select(
    "stop_id",
    "stop_name",
    "location_type",
    "parent_station"
).filter(
    col("location_type") == 0
).show(15, truncate=False)

# COMMAND ----------

from pyspark.sql.functions import (
    count,
    countDistinct,
    col,
    current_timestamp
)

# Aggregate platform activity at parent station level
station_metrics = (
    saved_stop_schedule.alias("schedule")
    .join(
        saved_stops.alias("stops"),
        col("schedule.stop_id") == col("stops.stop_id"),
        "inner"
    )
    .groupBy(
        col("stops.parent_station").alias("station_id")
    )
    .agg(
        count("*").alias("total_stop_events"),
        countDistinct("schedule.trip_id").alias("total_trips"),
        countDistinct("schedule.route_id").alias("total_routes")
    )
)

# Join metrics to the actual station dimension
gold_station_summary = (
    spark.table("anvesh.default.gold_dim_station")
    .join(
        station_metrics,
        on="station_id",
        how="left"
    )
    .withColumn(
        "total_stop_events",
        col("total_stop_events")
    )
    .withColumn("created_at", current_timestamp())
)

# COMMAND ----------

print("Total Station Records:", gold_station_summary.count())

gold_station_summary.orderBy(
    col("total_stop_events").desc()
).show(15, truncate=False)

# COMMAND ----------

print("Total Stations:", gold_station_summary.count())

print(
    "Duplicate Station IDs:",
    gold_station_summary.count()
    - gold_station_summary.select("station_id").distinct().count()
)

print(
    "Stations with Null Stop Events:",
    gold_station_summary.filter(
        col("total_stop_events").isNull()
    ).count()
)

print(
    "Total Stop Events:",
    gold_station_summary.agg(
        {"total_stop_events": "sum"}
    ).first()[0]
)

gold_station_summary.orderBy(
    col("total_stop_events").desc()
).show(15, truncate=False)

# COMMAND ----------

gold_station_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.gold_station_summary")

# COMMAND ----------

saved_station_summary = spark.table(
    "anvesh.default.gold_station_summary"
)

print("Saved Station Summary:", saved_station_summary.count())

saved_station_summary.orderBy(
    col("total_stop_events").desc()
).show(10, truncate=False)

# COMMAND ----------

gold_station_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.gold_station_summary")

# COMMAND ----------



# COMMAND ----------

saved_station_summary = spark.table(
    "anvesh.default.gold_station_summary"
)

print("Saved Station Summary:", saved_station_summary.count())

# COMMAND ----------

silver_fare_attributes = spark.table(
    "anvesh.default.silver_fare_attributes"
)

silver_fare_rules = spark.table(
    "anvesh.default.silver_fare_rules"
)

print("Fare Attributes:", silver_fare_attributes.count())
print("Fare Rules:", silver_fare_rules.count())

silver_fare_attributes.printSchema()
silver_fare_attributes.show(truncate=False)

silver_fare_rules.printSchema()
silver_fare_rules.show(10, truncate=False)

# COMMAND ----------

silver_fare_attributes.show(
    20,
    truncate=False
)

# COMMAND ----------

silver_fare_rules.printSchema()

silver_fare_rules.show(
    15,
    truncate=False
)

# COMMAND ----------

silver_fare_attributes.show(
    20,
    truncate=False
)

# COMMAND ----------

orphan_fare_count = (
    silver_fare_rules
    .join(
        silver_fare_attributes.select("fare_id"),
        on="fare_id",
        how="left_anti"
    )
    .count()
)

print("Fare rules without matching fare attributes:", orphan_fare_count)

# COMMAND ----------

silver_fare_rules.select(
    "fare_id"
).distinct().orderBy(
    "fare_id"
).show(truncate=False)

# COMMAND ----------

print("Fare rules without matching fare attributes:", orphan_fare_count)

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp

gold_fare_summary = (
    silver_fare_rules.alias("r")
    .join(
        silver_fare_attributes.alias("a"),
        on="fare_id",
        how="inner"
    )
    .select(
        trim(col("r.origin_id")).alias("origin_id"),
        trim(col("r.destination_id")).alias("destination_id"),
        col("r.fare_id"),
        col("a.price"),
        col("a.currency_type"),
        col("a.payment_method"),
        col("a.transfers"),
        col("a.agency_id")
    )
    .withColumn("created_at", current_timestamp())
)

# COMMAND ----------

print("Total Fare Records:", gold_fare_summary.count())

print(
    "Duplicate Fare Combinations:",
    gold_fare_summary.count()
    - gold_fare_summary.select(
        "origin_id",
        "destination_id",
        "fare_id"
    ).distinct().count()
)

print(
    "Null Fare IDs:",
    gold_fare_summary.filter(
        col("fare_id").isNull()
    ).count()
)

gold_fare_summary.show(15, truncate=False)

# COMMAND ----------

gold_fare_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.gold_fare_summary")

# COMMAND ----------

saved_fare_summary = spark.table(
    "anvesh.default.gold_fare_summary"
)

print("Saved Fare Summary:", saved_fare_summary.count())

saved_fare_summary.groupBy(
    "price",
    "currency_type"
).count().orderBy("price").show()

# COMMAND ----------

gold_tables = spark.sql("""
    SHOW TABLES IN anvesh.default
""")

gold_tables.filter(
    col("tableName").startswith("gold_")
).show(truncate=False)

# COMMAND ----------

# Dimension table checks

print("STATION DIMENSION")
print("Total:", spark.table("anvesh.default.gold_dim_station").count())
print("Unique IDs:",
      spark.table("anvesh.default.gold_dim_station")
      .select("station_id").distinct().count())

print("\nROUTE DIMENSION")
print("Total:", spark.table("anvesh.default.gold_dim_route").count())
print("Unique IDs:",
      spark.table("anvesh.default.gold_dim_route")
      .select("route_id").distinct().count())

print("\nDATE DIMENSION")
print("Total:", spark.table("anvesh.default.gold_dim_date").count())
print("Unique IDs:",
      spark.table("anvesh.default.gold_dim_date")
      .select("date_key").distinct().count())

# COMMAND ----------

# Load Gold fact tables
gold_trip = spark.table("anvesh.default.gold_fact_trip")

gold_stop_schedule = spark.table(
    "anvesh.default.gold_fact_stop_schedule"
)

# Trip fact validation
print("GOLD FACT TRIP")
print("Total Records:", gold_trip.count())
print(
    "Unique Trip IDs:",
    gold_trip.select("trip_id").distinct().count()
)
print(
    "Null Trip IDs:",
    gold_trip.filter(col("trip_id").isNull()).count()
)

# Stop schedule fact validation
print("\nGOLD FACT STOP SCHEDULE")
print("Total Records:", gold_stop_schedule.count())

print(
    "Unique Trip-Stop Sequences:",
    gold_stop_schedule
    .select("trip_id", "stop_sequence")
    .distinct()
    .count()
)

print(
    "Null Stop IDs:",
    gold_stop_schedule.filter(col("stop_id").isNull()).count()
)

# COMMAND ----------

gold_route_summary = spark.table(
    "anvesh.default.gold_route_summary"
)

gold_station_summary = spark.table(
    "anvesh.default.gold_station_summary"
)

gold_fare_summary = spark.table(
    "anvesh.default.gold_fare_summary"
)

print("GOLD ROUTE SUMMARY")
print("Records:", gold_route_summary.count())
print(
    "Unique routes:",
    gold_route_summary.select("route_id").distinct().count()
)

print("\nGOLD STATION SUMMARY")
print("Records:", gold_station_summary.count())
print(
    "Unique stations:",
    gold_station_summary.select("station_id").distinct().count()
)

print("\nGOLD FARE SUMMARY")
print("Records:", gold_fare_summary.count())
print(
    "Unique fare combinations:",
    gold_fare_summary.select(
        "origin_id",
        "destination_id",
        "fare_id"
    ).distinct().count()
)

# COMMAND ----------

# Load source and target tables
checks = [
    (
        "Trips",
        spark.table("anvesh.default.silver_trips").count(),
        spark.table("anvesh.default.gold_fact_trip").count()
    ),
    (
        "Stop Times",
        spark.table("anvesh.default.silver_stop_times").count(),
        spark.table("anvesh.default.gold_fact_stop_schedule").count()
    ),
    (
        "Stations",
        spark.table("anvesh.default.gold_dim_station").count(),
        spark.table("anvesh.default.gold_station_summary").count()
    ),
    (
        "Routes",
        spark.table("anvesh.default.gold_dim_route").count(),
        spark.table("anvesh.default.gold_route_summary").count()
    ),
    (
        "Fare Rules",
        spark.table("anvesh.default.silver_fare_rules").count(),
        spark.table("anvesh.default.gold_fare_summary").count()
    )
]

print("SILVER-TO-GOLD RECONCILIATION")

for name, source_count, target_count in checks:
    status = "PASS" if source_count == target_count else "CHECK"

    print(
        f"{name}: Source={source_count}, "
        f"Target={target_count}, Status={status}"
    )

# COMMAND ----------

