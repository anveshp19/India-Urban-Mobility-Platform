
import csv
import json
import logging
import uuid

from datetime import datetime, timezone
from pathlib import Path


# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SOURCE_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "gtfs"
    / "hyderabad_metro"
    / "extracted"
)

BRONZE_DIR = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "gtfs"
    / "hyderabad_metro"
)

MANIFEST_DIR = (
    PROJECT_ROOT
    / "data"
    / "metadata"
    / "ingestion_runs"
)

GTFS_FILES = [
    "agency.txt",
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar.txt",
]


# --------------------------------------------------
# 2. LOGGING CONFIGURATION
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# 3. UTILITY FUNCTION
# --------------------------------------------------

def get_current_timestamp():
    """Return current UTC timestamp."""

    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------
# 4. BRONZE FILE INGESTION
# --------------------------------------------------

def ingest_file(filename, ingestion_time):
    """Ingest one GTFS file into Bronze storage."""

    source_file = SOURCE_DIR / filename

    table_name = Path(filename).stem

    target_dir = BRONZE_DIR / table_name

    if not source_file.exists():
        raise FileNotFoundError(
            f"Source file not found: {source_file}"
        )

    target_dir.mkdir(parents=True, exist_ok=True)

    temp_file = target_dir / "data.csv.tmp"

    target_file = target_dir / "data.csv"

    record_count = 0

    try:
        with source_file.open(
            mode="r",
            encoding="utf-8-sig",
            newline="",
        ) as source:

            reader = csv.DictReader(source)

            if not reader.fieldnames:
                raise ValueError(
                    f"Missing CSV header: {filename}"
                )

            output_columns = reader.fieldnames + [
                "_source_file",
                "_source_system",
                "_ingestion_timestamp",
            ]

            with temp_file.open(
                mode="w",
                encoding="utf-8",
                newline="",
            ) as target:

                writer = csv.DictWriter(
                    target,
                    fieldnames=output_columns,
                )

                writer.writeheader()

                for row in reader:

                    row["_source_file"] = filename
                    row["_source_system"] = "HMRL_GTFS"
                    row["_ingestion_timestamp"] = ingestion_time

                    writer.writerow(row)

                    record_count += 1

        temp_file.replace(target_file)

        logger.info(
            "%s ingested successfully | Records: %d",
            filename,
            record_count,
        )

        return {
            "file_name": filename,
            "table_name": table_name,
            "record_count": record_count,
            "status": "SUCCESS",
        }

    except Exception:

        temp_file.unlink(missing_ok=True)

        raise


# --------------------------------------------------
# 5. MANIFEST GENERATION
# --------------------------------------------------

def write_manifest(manifest):
    """Save one JSON manifest for each pipeline run."""

    MANIFEST_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    run_id = manifest["run_id"]

    manifest_file = MANIFEST_DIR / f"{run_id}.json"

    temp_file = MANIFEST_DIR / f"{run_id}.json.tmp"

    try:
        with temp_file.open(
            mode="w",
            encoding="utf-8",
        ) as file:

            json.dump(
                manifest,
                file,
                indent=4,
            )

        temp_file.replace(manifest_file)

        logger.info(
            "Ingestion manifest saved: %s",
            manifest_file,
        )

    except Exception:

        temp_file.unlink(missing_ok=True)

        raise


# --------------------------------------------------
# 6. PIPELINE EXECUTION
# --------------------------------------------------

def run_bronze_ingestion():

    run_id = str(uuid.uuid4())

    started_at = get_current_timestamp()

    logger.info("Starting Bronze ingestion pipeline")

    logger.info("Run ID: %s", run_id)

    file_details = []

    total_records = 0

    status = "SUCCESS"

    error_message = None

    try:

        if not SOURCE_DIR.exists():
            raise FileNotFoundError(
                f"Source directory not found: {SOURCE_DIR}"
            )

        for filename in GTFS_FILES:

            result = ingest_file(
                filename,
                started_at,
            )

            file_details.append(result)

            total_records += result["record_count"]

        logger.info(
            "All GTFS files ingested successfully"
        )

    except Exception as error:

        status = "FAILED"

        error_message = str(error)

        logger.exception(
            "Bronze ingestion pipeline failed"
        )

        raise

    finally:

        completed_at = get_current_timestamp()

        manifest = {
            "pipeline_name": "hyderabad_metro_gtfs_bronze",
            "run_id": run_id,
            "source_system": "HMRL_GTFS",
            "status": status,
            "started_at": started_at,
            "completed_at": completed_at,
            "files_processed": len(file_details),
            "total_records": total_records,
            "file_details": file_details,
            "error_message": error_message,
        }

        write_manifest(manifest)

        logger.info(
            "Pipeline status: %s | Total records: %d",
            status,
            total_records,
        )


# --------------------------------------------------
# 7. MAIN ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":

    run_bronze_ingestion()
