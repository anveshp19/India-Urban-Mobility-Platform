# Databricks notebook source
print("India Urban Mobility Platform")
print("Silver Layer - Day 1")

print("Spark Version:",spark.version)

# COMMAND ----------

print("Spark Session:",spark)

# COMMAND ----------

bronze_path="/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/stops.txt"

stops_df=(
    spark.read
    .option("header",True)
    .option("inferSchema",True)
    .csv(bronze_path)
)

display(stops_df)

# COMMAND ----------

print("Total Records:",stops_df.count())

print("Schema:")
stops_df.printSchema()

print("First 5 Records:")

display(stops_df.limit(5))

# COMMAND ----------

bronze_path = "/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/stops.txt"

stops_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(bronze_path)
)

print("Total Records:", stops_df.count())

# COMMAND ----------

print(type(stops_df))

# COMMAND ----------

stops_df.select("stop_name").show(10, truncate=False)

# COMMAND ----------

stops_df.select("stop_name").distinct().show(20, truncate=False)

# COMMAND ----------

unique_stations=stops_df.select("stop_name").distinct().count()
print("Unique Stop Names:",unique_stations)

# COMMAND ----------

stops_df.groupBy("location_type").count().orderBy("location_type").show()

# COMMAND ----------

stops_df.select(
    "stop_id",
    "stop_name",
    "location_type",
    "parent_station"
).show(20, truncate=False)

# COMMAND ----------

stations_df=stops_df.filter(
    stops_df.location_type==1
)

print("Total Stations:",stations_df.count())

display(
    stations_df.select(
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "location_type"
    )
)

# COMMAND ----------

platforms_df=stops_df.filter(
    stops_df.location_type==0
)

print("Total Platforms:",platforms_df.count())

display(platforms_df)

# COMMAND ----------

from pyspark.sql.functions import col

platforms = platforms_df.alias("p")
stations = stations_df.alias("s")

orphan_platforms = platforms.join(
    stations,
    col("p.parent_station") == col("s.stop_id"),
    "left_anti"
)

print("Orphan Platforms:", orphan_platforms.count())

display(orphan_platforms)

# COMMAND ----------

entrances_df=stops_df.filter(
    col("location_type")==2
)

entrances=entrances_df.alias("e")
stations=stations_df.alias("s")

orphan_entrances=entrances.join(
    stations,
    col("e.parent_station")==col("s.stop_id"),
    "left_anti"
)

print("Total Entrances:",entrances_df.count())
print("Orphan Entrances:",orphan_entrances.count())

display(orphan_entrances)

# COMMAND ----------

from pyspark.sql.functions import col,count

duplicate_stop_ids=(
    stops_df
    .groupBy("stop_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count")>1)
)

print("Duplicate Stop IDs:",duplicate_stop_ids.count())
display(duplicate_stop_ids)

# COMMAND ----------

from pyspark.sql.functions import col,sum,when

null_check=stops_df.select(
    *[
        sum(
            when(col(column).isNull(),1).otherwise(0)
        ).alias(column)
        for column in stops_df.columns
    ]
)

display(null_check)
null_check.show()

# COMMAND ----------

from pyspark.sql.functions import col,sum,when

display(
    stops_df
    .groupBy("location_type")
    .agg(
        sum(when(col("parent_station").isNull(),1).otherwise(0))
            .alias("null_parent_station"),
        sum(when(col("platform_code").isNull(),1).otherwise(0))
            .alias("null_platform_code"),
        sum(when(col("zone_id").isNull(),1).otherwise(0))
            .alias("null_zone_id")

    )
    .orderBy("location_type")
)

# COMMAND ----------

from pyspark.sql.functions import col

invalid_coordinates = stops_df.filter(
    (col("stop_lat") < -90) |
    (col("stop_lat") > 90) |
    (col("stop_lon") < -180) |
    (col("stop_lon") > 180) |
    col("stop_lat").isNull() |
    col("stop_lon").isNull()
)

print("Total Records:", stops_df.count())
print("Invalid Coordinates:", invalid_coordinates.count())

display(invalid_coordinates)

# COMMAND ----------

from pyspark.sql.functions import count

duplicate_coordinates = (
    stops_df
    .groupBy("stop_lat", "stop_lon")
    .agg(
        count("*").alias("record_count")
    )
    .filter(col("record_count") > 1)
    .orderBy(col("record_count").desc())
)

print("Duplicate Coordinate Groups:", duplicate_coordinates.count())

display(duplicate_coordinates)

# COMMAND ----------

from pyspark.sql.functions import col, count

duplicate_coordinate_details = (
    stops_df
    .join(
        duplicate_coordinates.select("stop_lat", "stop_lon"),
        on=["stop_lat", "stop_lon"],
        how="inner"
    )
    .select(
        "stop_id",
        "stop_name",
        "location_type",
        "parent_station",
        "stop_lat",
        "stop_lon"
    )
    .orderBy("stop_lat", "stop_lon", "location_type")
)

display(duplicate_coordinate_details)

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    current_timestamp
)

silver_stops_df = (
    stops_df
    .select(
        trim(col("stop_id")).alias("stop_id"),
        trim(col("stop_name")).alias("stop_name"),
        col("stop_lat").cast("double").alias("stop_lat"),
        col("stop_lon").cast("double").alias("stop_lon"),
        col("zone_id"),
        col("location_type").cast("integer").alias("location_type"),
        col("parent_station"),
        col("platform_code")
    )
    .withColumn("processed_at", current_timestamp())
)

print("Silver Stops Count:", silver_stops_df.count())

display(silver_stops_df)

# COMMAND ----------

from pyspark.sql.functions import col, count

# Check duplicate stop IDs
silver_duplicates = (
    silver_stops_df
    .groupBy("stop_id")
    .agg(count("*").alias("count"))
    .filter(col("count") > 1)
)

# Check missing mandatory fields
silver_nulls = silver_stops_df.filter(
    col("stop_id").isNull() |
    col("stop_name").isNull() |
    col("stop_lat").isNull() |
    col("stop_lon").isNull()
)

print("Silver Records:", silver_stops_df.count())
print("Duplicate IDs:", silver_duplicates.count())
print("Records with mandatory NULLs:", silver_nulls.count())

# COMMAND ----------

silver_table = "anvesh.default.silver_stops"

(
    silver_stops_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table)
)

print(f"Successfully created Delta table: {silver_table}")

# COMMAND ----------

saved_silver_df = spark.table("anvesh.default.silver_stops")

print("Saved Silver Records:", saved_silver_df.count())

display(saved_silver_df)

# COMMAND ----------

print("Current Catalog:", spark.catalog.currentCatalog())
print("Current Database:", spark.catalog.currentDatabase())

display(
    spark.sql("SHOW TABLES IN anvesh.default")
)

# COMMAND ----------

spark.sql("SHOW TABLES IN workspace.default").show(truncate=False)

# COMMAND ----------

spark.sql("SHOW TABLES IN anvesh.default").show(truncate=False)

# COMMAND ----------

spark.sql("""
DESCRIBE DETAIL anvesh.default.silver_stops
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
DESCRIBE HISTORY anvesh.default.silver_stops
""").show(truncate=False)

# COMMAND ----------

version_0_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .table("anvesh.default.silver_stops")
)

print("Version 0 Records:", version_0_df.count())

display(version_0_df)

# COMMAND ----------

version_1_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 1)
    .table("anvesh.default.silver_stops")
)

print("Version 1 Records:", version_1_df.count())

# COMMAND ----------

routes_path="/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/routes.txt"

routes_df=(
    spark.read
    .option("header",True)
    .option("inferSchema",True)
    .csv(routes_path)
)

print("Total Routes:",routes_df.count())

routes_df.printSchema()

display(routes_df)

# COMMAND ----------

from pyspark.sql.functions import col,count

duplicate_routes=(
    routes_df
    .groupBy("route_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count")>1)
)

invalid_routes=routes_df.filter(
    col("route_id").isNull() |
    col("agency_id").isNull() |
    col("route_short_name").isNull() 
)

print("Total Routes:",routes_df.count())

print("Duplicate Route:",duplicate_routes.count())

print("Invalid Records:",invalid_routes.count())

# COMMAND ----------

from pyspark.sql.functions import col,trim,current_timestamp

silver_routes_df=(
    routes_df
    .select(
        trim(col("route_id")).alias("route_id"),
        trim(col("agency_id")).alias("agency_id"),
        trim(col("route_short_name")).alias("route_short_name"),
        trim(col("route_long_name")).alias("route_long_name"),
        col("route_type").cast("integer").alias("route_type"),
        col("route_text_color"),
        col("route_sort_order").cast("integer").alias("route_sort_order")
    )

    .withColumn("processed_at",current_timestamp())
)

display(silver_routes_df)

# COMMAND ----------

silver_routes_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.silver_routes")

# COMMAND ----------

saved_routes_df = spark.table("anvesh.default.silver_routes")

print("Silver Routes Count:", saved_routes_df.count())

display(saved_routes_df)

# COMMAND ----------

trips_path = "dbfs:/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/trips.txt"

trips_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(trips_path)
)

print("Total Trips:", trips_df.count())
trips_df.printSchema()
display(trips_df)

# COMMAND ----------

from pyspark.sql.functions import col, count

duplicate_trips = (
    trips_df
    .groupBy("trip_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

invalid_trips = trips_df.filter(
    col("trip_id").isNull() |
    col("route_id").isNull() |
    col("service_id").isNull()
)

print("Total Trips:", trips_df.count())
print("Duplicate Trip IDs:", duplicate_trips.count())
print("Invalid Records:", invalid_trips.count())

# COMMAND ----------

from pyspark.sql.functions import col,trim,current_timestamp

silver_trips_df=(
    trips_df
    .select(
        trim(col("service_id")).alias("service_id"),
        trim(col("route_id")).alias("route_id"),
        trim(col("trip_id")).alias("trip_id"),
        col("direction_id").cast("integer").alias("direction_id"),
        trim(col("trip_headsign")).alias("trip_headsign"),
        col("block_id"),
        col("shape_id")

    )
    .withColumn("processed_at",current_timestamp())
)

display(silver_trips_df)

# COMMAND ----------

silver_trips_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.silver_trips")

# COMMAND ----------

saved_trips_df = spark.table("anvesh.default.silver_trips")

print("Silver Trips Count:", saved_trips_df.count())

# COMMAND ----------

stop_times_path = "/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/stop_times.txt"

stop_times_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(stop_times_path)
)

print("Total Stop Times:", stop_times_df.count())

stop_times_df.printSchema()

display(stop_times_df)

# COMMAND ----------

from pyspark.sql.functions import col, count

duplicate_stop_times = (
    stop_times_df
    .groupBy("trip_id", "stop_sequence")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

invalid_stop_times = stop_times_df.filter(
    col("trip_id").isNull() |
    col("stop_id").isNull() |
    col("stop_sequence").isNull() |
    col("arrival_time").isNull() |
    col("departure_time").isNull()
)

print("Total Stop Times:", stop_times_df.count())
print("Duplicate Trip-Sequence Records:", duplicate_stop_times.count())
print("Invalid Records:", invalid_stop_times.count())

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp

silver_stop_times_df = (
    stop_times_df
    .select(
        trim(col("trip_id")).alias("trip_id"),
        col("stop_sequence").cast("integer").alias("stop_sequence"),
        trim(col("stop_id")).alias("stop_id"),
        col("arrival_time"),
        col("departure_time"),
        col("timepoint").cast("integer").alias("timepoint"),
        col("shape_dist_traveled").cast("integer").alias("shape_dist_traveled")
    )
    .withColumn("processed_at", current_timestamp())
)

print("Silver Stop Times:", silver_stop_times_df.count())
display(silver_stop_times_df.limit(10))

# COMMAND ----------

silver_stop_times_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.silver_stop_times")

# COMMAND ----------

saved_stop_times_df = spark.table("anvesh.default.silver_stop_times")

print("Silver Stop Times Count:", saved_stop_times_df.count())

# COMMAND ----------

calendar_path = "/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/calendar.txt"

calendar_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(calendar_path)
)

print("Total Calendar Records:", calendar_df.count())
calendar_df.printSchema()
display(calendar_df)

# COMMAND ----------

from pyspark.sql.functions import col, count

duplicate_services = (
    calendar_df
    .groupBy("service_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

invalid_calendar = calendar_df.filter(
    col("service_id").isNull() |
    col("start_date").isNull() |
    col("end_date").isNull()
)

print("Total Calendar Records:", calendar_df.count())
print("Duplicate Service IDs:", duplicate_services.count())
print("Invalid Records:", invalid_calendar.count())

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, to_date, current_timestamp
)

silver_calendar_df = (
    calendar_df
    .select(
        trim(col("service_id")).alias("service_id"),
        col("monday").cast("integer").alias("monday"),
        col("tuesday").cast("integer").alias("tuesday"),
        col("wednesday").cast("integer").alias("wednesday"),
        col("thursday").cast("integer").alias("thursday"),
        col("friday").cast("integer").alias("friday"),
        col("saturday").cast("integer").alias("saturday"),
        col("sunday").cast("integer").alias("sunday"),
        to_date(col("start_date").cast("string"), "yyyyMMdd").alias("start_date"),
        to_date(col("end_date").cast("string"), "yyyyMMdd").alias("end_date")
    )
    .withColumn("processed_at", current_timestamp())
)

display(silver_calendar_df)

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, to_date, current_timestamp
)

silver_calendar_df = (
    calendar_df
    .select(
        trim(col("service_id")).alias("service_id"),
        col("monday").cast("integer").alias("monday"),
        col("tuesday").cast("integer").alias("tuesday"),
        col("wednesday").cast("integer").alias("wednesday"),
        col("thursday").cast("integer").alias("thursday"),
        col("friday").cast("integer").alias("friday"),
        col("saturday").cast("integer").alias("saturday"),
        col("sunday").cast("integer").alias("sunday"),
        to_date(col("start_date").cast("string"), "yyyyMMdd").alias("start_date"),
        to_date(col("end_date").cast("string"), "yyyyMMdd").alias("end_date")
    )
    .withColumn("processed_at", current_timestamp())
)

display(silver_calendar_df)

# COMMAND ----------

silver_calendar_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.silver_calendar")

# COMMAND ----------

saved_calendar_df = spark.table("anvesh.default.silver_calendar")

print("Silver Calendar Count:", saved_calendar_df.count())

display(saved_calendar_df)

# COMMAND ----------

agency_path="/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/agency.txt"

agency_df=(
    spark.read
    .option("header",True)
    .option("inferSchema",True)
    .csv(agency_path)
)

print("Total Agency Records:",agency_df.count())
agency_df.printSchema()
display(agency_df)

# COMMAND ----------

from pyspark.sql.functions import col,count

duplicate_agencies=(
    agency_df
    .groupBy("agency_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count")>1)
)

invalid_agencies=agency_df.filter(
    col("agency_id").isNull() |
    col("agency_name").isNull() |
    col("agency_url").isNull() |
    col("agency_timezone").isNull()
)

print("Total Agency Records:",agency_df.count())
print("Duplicate Agency IDs:",duplicate_agencies.count())
print("Invalid Records:",invalid_agencies.count())

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp

silver_agency_df = (
    agency_df
    .select(
        trim(col("agency_id")).alias("agency_id"),
        trim(col("agency_name")).alias("agency_name"),
        trim(col("agency_url")).alias("agency_url"),
        trim(col("agency_timezone")).alias("agency_timezone"),
        col("agency_lang"),
        col("agency_fare_url"),
        col("agency_email"),
        col("agency_phone")
    )
    .withColumn("processed_at", current_timestamp())
)

display(silver_agency_df)

# COMMAND ----------

silver_agency_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.silver_agency")

# COMMAND ----------

saved_agency_df = spark.table("anvesh.default.silver_agency")

print("Silver Agency Count:", saved_agency_df.count())

# COMMAND ----------

shapes_path="/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/shapes.txt"

shapes_df=(
    spark.read
    .option("header",True)
    .option("inferSchema",True)
    .csv(shapes_path)
)

print("Total Shapes Records:",shapes_df.count())
shapes_df.printSchema()
display(shapes_df.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, count

duplicate_shape_points = (
    shapes_df
    .groupBy("shape_id", "shape_pt_sequence")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

invalid_shapes = shapes_df.filter(
    col("shape_id").isNull() |
    col("shape_pt_lat").isNull() |
    col("shape_pt_lon").isNull() |
    col("shape_pt_sequence").isNull() |
    (col("shape_pt_lat") < -90) |
    (col("shape_pt_lat") > 90) |
    (col("shape_pt_lon") < -180) |
    (col("shape_pt_lon") > 180)
)

print("Total Shape Records:", shapes_df.count())
print("Duplicate Shape Points:", duplicate_shape_points.count())
print("Invalid Records:", invalid_shapes.count())

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp

silver_shapes_df = (
    shapes_df
    .select(
        trim(col("shape_id")).alias("shape_id"),
        col("shape_pt_lat").cast("double").alias("shape_pt_lat"),
        col("shape_pt_lon").cast("double").alias("shape_pt_lon"),
        col("shape_pt_sequence").cast("integer").alias("shape_pt_sequence"),
        col("shape_dist_traveled").cast("integer").alias("shape_dist_traveled")
    )
    .withColumn("processed_at", current_timestamp())
)

print("Silver Shapes Count:", silver_shapes_df.count())
display(silver_shapes_df.limit(10))

# COMMAND ----------

silver_shapes_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.silver_shapes")


# COMMAND ----------

saved_shapes_df = spark.table("anvesh.default.silver_shapes")

print("Silver Shapes Count:", saved_shapes_df.count())

# COMMAND ----------

fare_attributes_path = "/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/fare_attributes.txt"

fare_attributes_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(fare_attributes_path)
)

print("Total Fare Attributes:", fare_attributes_df.count())
fare_attributes_df.printSchema()
display(fare_attributes_df)

# COMMAND ----------

from pyspark.sql.functions import col, count

duplicate_fares = (
    fare_attributes_df
    .groupBy("fare_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

invalid_fares = fare_attributes_df.filter(
    col("fare_id").isNull() |
    col("price").isNull() |
    col("currency_type").isNull() |
    col("payment_method").isNull()
)

print("Total Fare Attributes:", fare_attributes_df.count())
print("Duplicate Fare IDs:", duplicate_fares.count())
print("Invalid Records:", invalid_fares.count())

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp

silver_fare_attributes_df = (
    fare_attributes_df
    .select(
        trim(col("fare_id")).alias("fare_id"),
        col("price").cast("decimal(10,2)").alias("price"),
        trim(col("currency_type")).alias("currency_type"),
        col("payment_method").cast("integer").alias("payment_method"),
        col("transfers"),
        col("agency_id")
    )
    .withColumn("processed_at", current_timestamp())
)

print("Silver Fare Attributes:", silver_fare_attributes_df.count())
display(silver_fare_attributes_df)

# COMMAND ----------

silver_fare_attributes_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.silver_fare_attributes")

# COMMAND ----------

saved_fare_attributes_df = spark.table(
    "anvesh.default.silver_fare_attributes"
)

print(
    "Silver Fare Attributes Count:",
    saved_fare_attributes_df.count()
)

# COMMAND ----------

fare_rules_path = "/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/fare_rules.txt"

fare_rules_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(fare_rules_path)
)

print("Total Fare Rules:", fare_rules_df.count())
fare_rules_df.printSchema()
display(fare_rules_df)

# COMMAND ----------

from pyspark.sql.functions import col, count

duplicate_fare_rules = (
    fare_rules_df
    .groupBy("fare_id", "origin_id", "destination_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

invalid_fare_rules = fare_rules_df.filter(
    col("fare_id").isNull() |
    col("origin_id").isNull() |
    col("destination_id").isNull()
)

print("Total Fare Rules:", fare_rules_df.count())
print("Duplicate Fare Rules:", duplicate_fare_rules.count())
print("Invalid Records:", invalid_fare_rules.count())

# COMMAND ----------

from pyspark.sql.functions import col, trim, current_timestamp

silver_fare_rules_df = (
    fare_rules_df
    .select(
        trim(col("origin_id")).alias("origin_id"),
        trim(col("destination_id")).alias("destination_id"),
        trim(col("fare_id")).alias("fare_id")
    )
    .withColumn("processed_at", current_timestamp())
)

print("Silver Fare Rules:", silver_fare_rules_df.count())
display(silver_fare_rules_df.limit(10))

# COMMAND ----------

silver_fare_rules_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.silver_fare_rules")

# COMMAND ----------

saved_fare_rules_df = spark.table(
    "anvesh.default.silver_fare_rules"
)

print("Silver Fare Rules Count:", saved_fare_rules_df.count())

# COMMAND ----------

feed_info_path = "/Volumes/anvesh/default/test/bronze/gtfs/hyderabad_metro/feed_info.txt"

feed_info_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(feed_info_path)
)

print("Total Feed Info Records:", feed_info_df.count())
feed_info_df.printSchema()
display(feed_info_df)

# COMMAND ----------

from pyspark.sql.functions import col, count

duplicate_feed_info = (
    feed_info_df
    .groupBy("feed_publisher_name", "feed_lang")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

invalid_feed_info = feed_info_df.filter(
    col("feed_publisher_name").isNull() |
    col("feed_publisher_url").isNull() |
    col("feed_lang").isNull()
)

print("Total Feed Info Records:", feed_info_df.count())
print("Duplicate Feed Info Records:", duplicate_feed_info.count())
print("Invalid Records:", invalid_feed_info.count())

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, to_date, current_timestamp
)

silver_feed_info_df = (
    feed_info_df
    .select(
        trim(col("feed_publisher_name")).alias("feed_publisher_name"),
        trim(col("feed_publisher_url")).alias("feed_publisher_url"),
        trim(col("feed_lang")).alias("feed_lang"),
        col("feed_contact_url"),
        to_date(
            col("feed_start_date").cast("string"),
            "yyyyMMdd"
        ).alias("feed_start_date"),
        to_date(
            col("feed_end_date").cast("string"),
            "yyyyMMdd"
        ).alias("feed_end_date")
    )
    .withColumn("processed_at", current_timestamp())
)

print("Silver Feed Info:", silver_feed_info_df.count())
silver_feed_info_df.printSchema()
display(silver_feed_info_df)

# COMMAND ----------

silver_feed_info_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("anvesh.default.silver_feed_info")

# COMMAND ----------

saved_feed_info_df = spark.table("anvesh.default.silver_feed_info")

print("Silver Feed Info Count:", saved_feed_info_df.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN anvesh.default;

# COMMAND ----------

from pyspark.sql.functions import col

trips_df = spark.table("anvesh.default.silver_trips")
routes_df = spark.table("anvesh.default.silver_routes")

orphan_trips = trips_df.join(
    routes_df.select("route_id"),
    on="route_id",
    how="left_anti"
)

print("Total Trips:", trips_df.count())
print("Trips without matching Routes:", orphan_trips.count())

display(orphan_trips)

# COMMAND ----------

calendar_df = spark.table("anvesh.default.silver_calendar")

orphan_services = trips_df.join(
    calendar_df.select("service_id"),
    on="service_id",
    how="left_anti"
)

print("Total Trips:", trips_df.count())
print("Trips without matching Calendar Services:", orphan_services.count())

display(orphan_services)

# COMMAND ----------

stop_times_df = spark.table("anvesh.default.silver_stop_times")

orphan_stop_times = stop_times_df.join(
    trips_df.select("trip_id"),
    on="trip_id",
    how="left_anti"
)

print("Total Stop Times:", stop_times_df.count())
print("Stop Times without matching Trips:", orphan_stop_times.count())

display(orphan_stop_times)

# COMMAND ----------

stops_df = spark.table("anvesh.default.silver_stops")

orphan_stop_ids = stop_times_df.join(
    stops_df.select("stop_id"),
    on="stop_id",
    how="left_anti"
)

print("Total Stop Times:", stop_times_df.count())
print("Stop Times without matching Stops:", orphan_stop_ids.count())

display(orphan_stop_ids)

# COMMAND ----------

shapes_df = spark.table("anvesh.default.silver_shapes")

orphan_shapes = trips_df.join(
    shapes_df.select("shape_id").distinct(),
    on="shape_id",
    how="left_anti"
)

print("Total Trips:", trips_df.count())
print("Trips without matching Shapes:", orphan_shapes.count())

display(orphan_shapes)

# COMMAND ----------

fare_rules_df = spark.table("anvesh.default.silver_fare_rules")
fare_attributes_df = spark.table("anvesh.default.silver_fare_attributes")

orphan_fare_rules = fare_rules_df.join(
    fare_attributes_df.select("fare_id"),
    on="fare_id",
    how="left_anti"
)

print("Total Fare Rules:", fare_rules_df.count())
print("Fare Rules without matching Fare Attributes:", orphan_fare_rules.count())

display(orphan_fare_rules)

# COMMAND ----------

silver_tables = [
    "silver_stops",
    "silver_routes",
    "silver_trips",
    "silver_stop_times",
    "silver_calendar",
    "silver_agency",
    "silver_shapes",
    "silver_fare_attributes",
    "silver_fare_rules",
    "silver_feed_info"
]

for table in silver_tables:
    df = spark.table(f"anvesh.default.{table}")
    print(f"{table}: {df.count()} records")

# COMMAND ----------

