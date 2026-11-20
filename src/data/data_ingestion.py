"""
data_ingestion.py

First step of the pipeline: clean the raw NYC yellow taxi trip files.

What this script does:
1. Reads each raw monthly CSV file in small pieces (chunks), because
   every file is almost 2 GB and is too big to load at once.
2. Removes rows with bad values (GPS errors, strange fares, strange distances).
3. Keeps only the columns we need later: pickup time and pickup location.
4. Saves all cleaned rows into one CSV file inside data/interim.
"""

import logging
from pathlib import Path

import pandas as pd

# Print progress messages with a time stamp while the script is running
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("data_ingestion")

# ----------------------------------------------------------------------
# Settings
# ----------------------------------------------------------------------

# The raw files we want to clean (January, February and March 2016)
RAW_FILE_NAMES = [
    "yellow_tripdata_2016-01.csv",
    "yellow_tripdata_2016-02.csv",
    "yellow_tripdata_2016-03.csv",
]

# How many rows we read from a file at one time
CHUNK_SIZE = 500000

# Only these columns are read from the raw files. All other columns are ignored.
COLUMNS_TO_READ = [
    "tpep_pickup_datetime",
    "pickup_latitude",
    "pickup_longitude",
    "dropoff_latitude",
    "dropoff_longitude",
    "trip_distance",
    "fare_amount",
]

# These are the only columns we save in the cleaned file
COLUMNS_TO_KEEP = [
    "tpep_pickup_datetime",
    "pickup_latitude",
    "pickup_longitude",
]

# A box around New York City. Points outside this box are treated as GPS errors.
MIN_LATITUDE = 40.60
MAX_LATITUDE = 40.85
MIN_LONGITUDE = -74.05
MAX_LONGITUDE = -73.70

# Allowed range for the fare (in dollars) and the trip distance (in miles)
MIN_FARE = 0.50
MAX_FARE = 81.0
MIN_DISTANCE = 0.25
MAX_DISTANCE = 24.43


# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------

def keep_between(data, column_name, low, high):
    """Keep only the rows where the column value is between low and high (both included)."""
    is_inside = (data[column_name] >= low) & (data[column_name] <= high)
    return data[is_inside]


def clean_chunk(chunk):
    """Remove bad rows from one chunk and keep only the columns we need."""

    # 1. The pickup point and the dropoff point must both be inside the NYC box
    chunk = keep_between(chunk, "pickup_latitude", MIN_LATITUDE, MAX_LATITUDE)
    chunk = keep_between(chunk, "pickup_longitude", MIN_LONGITUDE, MAX_LONGITUDE)
    chunk = keep_between(chunk, "dropoff_latitude", MIN_LATITUDE, MAX_LATITUDE)
    chunk = keep_between(chunk, "dropoff_longitude", MIN_LONGITUDE, MAX_LONGITUDE)

    # 2. The fare and the trip distance must be in the allowed range
    chunk = keep_between(chunk, "fare_amount", MIN_FARE, MAX_FARE)
    chunk = keep_between(chunk, "trip_distance", MIN_DISTANCE, MAX_DISTANCE)

    # 3. Keep only the columns we need in later steps
    chunk = chunk[COLUMNS_TO_KEEP]

    return chunk


# ----------------------------------------------------------------------
# Main program
# ----------------------------------------------------------------------

def main():
    # This file lives in <project>/src/data/, so we go up two levels to find the project folder
    root_path = Path(__file__).resolve().parent.parent.parent
    raw_folder = root_path / "data" / "raw"
    output_path = root_path / "data" / "interim" / "df_without_outliers.csv"

    # Make sure the output folder exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Delete the old output file, so new rows are never added on top of old results
    if output_path.exists():
        output_path.unlink()
        logger.info("Old output file deleted")

    total_rows_read = 0
    total_rows_kept = 0

    # Column names must be written only once, at the top of the output file
    header_written = False

    for file_name in RAW_FILE_NAMES:
        file_path = raw_folder / file_name
        logger.info(f"Reading {file_name}")

        # Counters for this one file
        file_rows_read = 0
        file_rows_kept = 0
        chunk_number = 0

        # With chunksize, read_csv gives us the file piece by piece
        chunk_reader = pd.read_csv(
            file_path,
            usecols=COLUMNS_TO_READ,
            parse_dates=["tpep_pickup_datetime"],
            chunksize=CHUNK_SIZE,
        )

        for chunk in chunk_reader:
            chunk_number = chunk_number + 1

            # Clean this chunk and count how many rows we had before and after
            rows_before = len(chunk)
            cleaned_chunk = clean_chunk(chunk)
            rows_after = len(cleaned_chunk)

            file_rows_read = file_rows_read + rows_before
            file_rows_kept = file_rows_kept + rows_after

            # Save the cleaned chunk. The first chunk creates the file (with column names),
            # every later chunk is added at the end of the file.
            if header_written:
                cleaned_chunk.to_csv(output_path, mode="a", header=False, index=False)
            else:
                cleaned_chunk.to_csv(output_path, mode="w", header=True, index=False)
                header_written = True

            # Print a short progress message after every 10 chunks
            if chunk_number % 10 == 0:
                logger.info(f"  {file_name}: {file_rows_read:,} rows read so far")

        logger.info(f"Finished {file_name}: kept {file_rows_kept:,} of {file_rows_read:,} rows")

        total_rows_read = total_rows_read + file_rows_read
        total_rows_kept = total_rows_kept + file_rows_kept

    # Final summary
    kept_percent = 100 * total_rows_kept / total_rows_read
    logger.info(
        f"All files done: kept {total_rows_kept:,} of {total_rows_read:,} rows ({kept_percent:.1f} percent)"
    )
    logger.info(f"Cleaned data saved to {output_path}")


# This runs main() only when we start this file directly (python src/data/data_ingestion.py)
if __name__ == "__main__":
    main()