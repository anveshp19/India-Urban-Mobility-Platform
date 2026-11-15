
import csv
import logging
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Extracted GTFS source directory
GTFS_SOURCE_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "gtfs"
    / "hyderabad_metro"
    / "extracted"
)

# Required GTFS files and expected columns
REQUIRED_FILES = {
    "agency.txt": [
        "agency_id",
        "agency_name",
        "agency_url",
        "agency_timezone",
    ],
    "routes.txt": [
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_type",
    ],
    "stops.txt": [
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
    ],
    "trips.txt": [
        "service_id",
        "route_id",
        "trip_id",
    ],
    "stop_times.txt": [
        "trip_id",
        "stop_sequence",
        "stop_id",
        "arrival_time",
        "departure_time",
    ],
    "calendar.txt": [
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ],
}

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def validate_gtfs_files(source_dir: Path) -> None:
    """Validate required GTFS files and their CSV headers."""

    if not source_dir.exists():
        raise FileNotFoundError(
            f"GTFS source directory not found: {source_dir}"
        )

    logger.info("Starting GTFS ingestion validation")

    for filename, expected_columns in REQUIRED_FILES.items():

        file_path = source_dir / filename

        if not file_path.exists():
            raise FileNotFoundError(
                f"Required GTFS file missing: {filename}"
            )

        with file_path.open(
            mode="r",
            encoding="utf-8-sig",
            newline="",
        ) as file:

            reader = csv.DictReader(file)

            actual_columns = reader.fieldnames or []

            missing_columns = [
                column
                for column in expected_columns
                if column not in actual_columns
            ]

            if missing_columns:
                raise ValueError(
                    f"{filename} is missing columns: {missing_columns}"
                )

            record_count = sum(1 for _ in reader)

            logger.info(
                "%s validated successfully | Records: %d",
                filename,
                record_count,
            )

    logger.info(
        "GTFS ingestion validation completed successfully"
    )


if __name__ == "__main__":

    try:
        validate_gtfs_files(GTFS_SOURCE_DIR)

    except Exception:
        logger.exception("GTFS ingestion validation failed")
        raise
