# Databricks notebook source
# ============================================
# INDIA URBAN MOBILITY PLATFORM
# END-TO-END PIPELINE ORCHESTRATION
# ============================================

from datetime import datetime, timezone
import uuid

PIPELINE_NAME = "india_urban_mobility_pipeline"

CATALOG = "anvesh"
SCHEMA = "default"

PIPELINE_RUN_ID = str(uuid.uuid4())

NOTEBOOKS = {
    "bronze_ingestion": "00_bronze_ingestion",
    "bronze_quality": "04_bronze_quality_validation",
    "bronze_to_silver": "01_bronze_to_silver_stops",
    "silver_to_gold": "02_silver_to_gold",
    "gold_quality": "03_gold_quality_validation"
}

print("Pipeline:", PIPELINE_NAME)
print("Run ID:", PIPELINE_RUN_ID)
print("Started at:", datetime.now(timezone.utc))
print("Configured notebooks:")

for task, notebook in NOTEBOOKS.items():
    print(f"{task}: {notebook}")

# COMMAND ----------

# Get current notebook workspace path

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

current_path = context.notebookPath().get()

print("Current notebook path:")
print(current_path)

print("\nExpected notebook folder:")
print(current_path.rsplit("/", 1)[0])

print("\nConfigured notebook names:")
for task, notebook in NOTEBOOKS.items():
    print(f"{task}: {notebook}")

# COMMAND ----------

WORKSPACE_FOLDER = "/Users/anveshp222002@gmail.com"

NOTEBOOK_PATHS = {
    "bronze_ingestion": f"{WORKSPACE_FOLDER}/00_bronze_ingestion",
    "bronze_quality": f"{WORKSPACE_FOLDER}/04_bronze_quality_validation",
    "bronze_to_silver": f"{WORKSPACE_FOLDER}/01_bronze_to_silver_stops",
    "silver_to_gold": f"{WORKSPACE_FOLDER}/02_silver_to_gold",
    "gold_quality": f"{WORKSPACE_FOLDER}/03_gold_quality_validation"
}

for task, path in NOTEBOOK_PATHS.items():
    print(f"{task}: {path}")

# COMMAND ----------

