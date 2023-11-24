
"""Query wxm data at remote IPFS parquet files."""

import logging
import polars as pl

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Read from parquet files in a data directory
#
# Schema is as follows:
#
# device_id (varchar)
# timestamp (bigint)
# temperature (double)
# humidity (double)
# precipitation_accumulated (double)
# wind_speed (double)
# wind_gust (double)
# wind_direction (double)
# illuminance (double)
# solar_irradiance (double)
# fo_uv (double)
# uv_index (double)
# precipitation_rate (double)
# pressure (double)
# model (varchar)


# Create a dataframe from remote parquet files
def get_df(remote_files: list[str]) -> pl.DataFrame:
    logging.info("Creating dataframe from remote (IPFS)...")

    if not remote_files:
        raise ValueError("No remote parquet files provided.")

    try:
        df = pl.scan_parquet(remote_files).collect()
        return df
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


# Query the min and max timestamp values from the dataframe
def query_timestamp_range(df: pl.DataFrame) -> (int, int):
    if df is not None and not df.is_empty():
        try:
            min_max_value = df.select([
                pl.col("timestamp").min().alias("min_timestamp"),
                pl.col("timestamp").max().alias("max_timestamp")
            ])
            result = min_max_value
            # Extract the min and max values
            min_timestamp = result[0, "min_timestamp"]
            max_timestamp = result[0, "max_timestamp"]

            return (min_timestamp, max_timestamp)
        except Exception as e:
            logging.error(f"Error in query_timestamp_range: {e}")
            raise
    else:
        logging.error("DataFrame is None or empty")
        raise ValueError("DataFrame is None or empty")


# Query averages across all columns for time range (except device_id, timestamp, model)
def query_average_all(df: pl.DataFrame, start: int, end: int) -> pl.DataFrame:
    if df is not None and not df.is_empty():
        try:
            # List of columns to calculate the average
            columns = ["temperature", "humidity", "precipitation_accumulated",
                       "wind_speed", "wind_gust", "wind_direction",
                       "illuminance", "solar_irradiance", "fo_uv",
                       "uv_index", "precipitation_rate", "pressure"]
            # Prepare selection expressions
            selection = [(pl.col(col).mean().alias(col)) for col in columns]
            # Apply the selection
            averages = df.filter(
                (pl.col("timestamp") > start) & (pl.col("timestamp") < end)
            ).select(selection)

            return averages
        except Exception as e:
            logging.error(f"Error in query_average_all: {e}")
            raise
    else:
        logging.error("DataFrame is None or empty")
        raise ValueError("DataFrame is None or empty")


# Query number of unique `device_id` entries
def query_num_unique_devices(df: pl.DataFrame) -> int:
    if df is not None and not df.is_empty():
        try:
            unique_devices = df.select(pl.col("device_id").unique())

            return unique_devices.shape[0]
        except Exception as e:
            logging.error(f"Error in query_num_unique_devices: {e}")
            raise
    else:
        logging.error("DataFrame is None or empty")
        raise ValueError("DataFrame is None or empty")


# Query number of unique `model` entries
def query_num_unique_models(df: pl.DataFrame) -> int:
    if df is not None and not df.is_empty():
        try:
            unique_models = df.select(pl.col("model").unique())

            return unique_models.shape[0]
        except Exception as e:
            logging.error(f"Error in query_num_unique_models: {e}")
            raise
    else:
        logging.error("DataFrame is None or empty")
        raise ValueError("DataFrame is None or empty")


# Query total precipitation for time range
def query_agg_precipitation_acc(df: pl.DataFrame, start: int, end: int) -> float:
    if df is not None and not df.is_empty():
        try:
            total_precipitation = df.select(
                pl.col("precipitation_accumulated")
                .where(
                    (pl.col("timestamp") > start) &
                    (pl.col("timestamp") < end)
                )
                .sum()
                .alias("total_precipitation")
            )

            return total_precipitation[0, "total_precipitation"]
        except Exception as e:
            logging.error(f"Error in query_agg_precipitation_acc: {e}")
            raise
    else:
        logging.error("DataFrame is None or empty")
        raise ValueError("DataFrame is None or empty")


# Query all rows from the dataframe (for testing)
def query_all_limit_n(df: pl.DataFrame, n: int) -> pl.DataFrame:
    if df is not None and not df.is_empty():
        try:
            limit = df.head(n)

            return limit
        except Exception as e:
            logging.error(f"Error in query_all_limit_n: {e}")
            raise
    else:
        logging.error("DataFrame is None or empty")
        raise ValueError("DataFrame is None or empty")
