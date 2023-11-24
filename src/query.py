
"""Query wxm data at remote IPFS parquet files using polars."""

import polars as pl

from utils import format_unix, err, log_info

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
    if not remote_files:
        err("No remote parquet files provided",
            ValueError("Invalid input"), ValueError)

    try:
        df = pl.scan_parquet(remote_files).collect()
        return df
    except Exception as e:
        err("Error in get_df", e)


# Get the timestamp range from the dataframe, or use the provided start and end
# Note: timestamp range for Oct 15-21 data: 1697328000000 to 1697932798895
def get_timerange(df: pl.DataFrame, start: int, end: int) -> (int, int):
    # If no start or end time was provided, default to the min/max of the dataset
    if start is None or end is None:
        # Get timestamp range from data
        min_time, max_time = query_timestamp_range(df)
        if start is None:
            start = min_time
        if end is None:
            end = max_time
    log_info(
        f"Query range: {format_unix(start)} to {format_unix(end)}")
    return (start, end)


# Execute all queries (below) on the dataframe for a given time range
def execute_queries(df: pl.DataFrame, start: int, end: int):
    # Query for averages across all columns (except device_id, timestamp,
    # model). Also, query total precipitation, number of unique devices, and
    # number of unique models.
    try:
        averages = query_average_all(df, start, end)
        total_precipitation = query_agg_precipitation_acc(df, start, end)
        num_devices = query_num_unique_devices(df)
        num_models = query_num_unique_models(df)

        return (averages, total_precipitation, num_devices, num_models)
    except Exception as e:
        err("Error in execute_queries", e)


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
            err("Error in query_timestamp_range", e)
    else:
        err("DataFrame is None or empty", ValueError("Invalid input"), ValueError)


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
            err("Error in query_average_all", e)
    else:
        err("DataFrame is None or empty", ValueError("Invalid input"), ValueError)


# Query number of unique `device_id` entries
def query_num_unique_devices(df: pl.DataFrame) -> int:
    if df is not None and not df.is_empty():
        try:
            unique_devices = df.select(pl.col("device_id").unique())

            return unique_devices.shape[0]
        except Exception as e:
            err("Error in query_num_unique_devices", e)
    else:
        err("DataFrame is None or empty", ValueError("Invalid input"), ValueError)


# Query number of unique `model` entries
def query_num_unique_models(df: pl.DataFrame) -> int:
    if df is not None and not df.is_empty():
        try:
            unique_models = df.select(pl.col("model").unique())

            return unique_models.shape[0]
        except Exception as e:
            err("Error in query_num_unique_models", e)
    else:
        err("DataFrame is None or empty", ValueError("Invalid input"), ValueError)


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
            err("Error in query_agg_precipitation_acc", e)
    else:
        err("DataFrame is None or empty", ValueError("Invalid input"), ValueError)


# Query all rows from the dataframe (for testing)
def query_all_limit_n(df: pl.DataFrame, n: int) -> pl.DataFrame:
    if df is not None and not df.is_empty():
        try:
            limit = df.head(n)
            return limit
        except Exception as e:
            err("Error in query_all_limit_n", e)
    else:
        err("DataFrame is None or empty", ValueError("Invalid input"), ValueError)
