
"""Query wxm data at remote IPFS parquet files using polars."""

import polars as pl

from utils import err

"""
Read from parquet files on IPFS

Schema is as follows:

device_id (varchar)
timestamp (bigint)
temperature (double)
humidity (double)
precipitation_accumulated (double)
wind_speed (double)
wind_gust (double)
wind_direction (double)
illuminance (double)
solar_irradiance (double)
fo_uv (double)
uv_index (double)
precipitation_rate (double)
pressure (double)
model (varchar)
name (varchar)
cell_id (varchar)
lat (double)
lon (double)
"""


def get_df(remote_files: list[str]) -> pl.DataFrame:
    """
    Create a dataframe from remote parquet files.

    Args:
        remote_files (list[str]): The list of remote parquet files.

    Returns:
        pl.DataFrame: A Polars DataFrame from the remote parquet files.
    """
    if not remote_files:
        err("No remote parquet files provided",
            ValueError("Invalid input"), ValueError)

    try:
        df = pl.scan_parquet(
            source=remote_files,
            cache=True,
            retries=3).collect()
        return df
    except Exception as e:
        err("Error in get_df", e)


def query_timestamp_range(df: pl.DataFrame) -> (int, int):
    """
    Returns the min and max timestamp values from the provided DataFrame.

    Args:
        df (pl.DataFrame): The DataFrame to query.

    Returns:
        (int, int): A tuple containing the min and max timestamp values.
    """
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


def query_average_all(df: pl.DataFrame, start: int, end: int) -> pl.DataFrame:
    """
    Returns the average values for all columns in the provided DataFrame. This
    excludes device_id, timestamp, model, name, cell_id, lat, and lon.

    Args:
        df (pl.DataFrame): The DataFrame to query.
        start (int): The start of the query time range.
        end (int): The end of the query time range.

    Returns:
        pl.DataFrame: A DataFrame containing the averages for all columns.
    """
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
            if start is None or end is None:
                averages = df.select(selection)
            elif start is None and end is not None:
                averages = df.filter(
                    (pl.col("timestamp") > start)
                ).select(selection)
            else:
                averages = df.filter(
                    (pl.col("timestamp") > start) & (pl.col("timestamp") < end)
                ).select(selection)

            return averages
        except Exception as e:
            err("Error in query_average_all", e)
    else:
        err("DataFrame is None or empty", ValueError("Invalid input"), ValueError)


def query_num_unique_devices(df: pl.DataFrame, start: int, end: int) -> int:
    """
    Returns the number of unique device_id entries in the provided DataFrame.

    Args:
        df (pl.DataFrame): The DataFrame to query.
        start (int): The start of the query time range.
        end (int): The end of the query time range.

    Returns:
        int: The number of unique device_id entries.
    """
    if df is not None and not df.is_empty():
        try:
            selection = pl.col("device_id").unique().alias("num_devices")
            if start is None or end is None:
                unique_devices = df.select(selection)
            elif start is None and end is not None:
                unique_devices = df.filter(
                    (pl.col("timestamp") > start)
                ).select(selection)
            else:
                unique_devices = df.filter(
                    (pl.col("timestamp") > start) & (pl.col("timestamp") < end)
                ).select(selection)

            return len(unique_devices)
        except Exception as e:
            err("Error in query_num_unique_devices", e)
    else:
        err("DataFrame is None or empty", ValueError("Invalid input"), ValueError)


def query_mode_cell_id(df: pl.DataFrame, start: int, end: int) -> int:
    """
    Returns the most common cell_id value in the provided DataFrame.

    Args:
        df (pl.DataFrame): The DataFrame to query.
        start (int): The start of the query time range.
        end (int): The end of the query time range.

    Returns:
        int: The most common cell_id value.
    """
    if df is not None and not df.is_empty():
        try:
            selection = pl.col("cell_id").drop_nulls(
            ).mode().first().alias("cell_mode")

            if start is None or end is None:
                cell_mode = df.select(selection)
            elif start is None and end is not None:
                cell_mode = df.filter(
                    (pl.col("timestamp") > start)
                ).select(selection)
            else:
                cell_mode = df.filter(
                    (pl.col("timestamp") > start) & (pl.col("timestamp") < end)
                ).select(selection)

            return cell_mode['cell_mode'][0]
        except Exception as e:
            err("Error in query_mode_cell_id", e)
    else:
        err("DataFrame is None or empty", ValueError("Invalid input"), ValueError)


def query_agg_precipitation_acc(df: pl.DataFrame, start: int, end: int) -> float:
    """
    Returns the total precipitation value for the provided DataFrame.

    Args:
        df (pl.DataFrame): The DataFrame to query.
        start (int): The start of the query time range.
        end (int): The end of the query time range.

    Returns:
        float: The total precipitation value.
    """
    if df is not None and not df.is_empty():
        try:
            selection = pl.col(
                "precipitation_accumulated").sum().alias("total_precipitation")

            if start is None or end is None:
                total_precipitation = df.select(selection)
            elif start is None and end is not None:
                total_precipitation = df.filter(
                    (pl.col("timestamp") > start)
                ).select(selection)
            else:
                total_precipitation = df.filter(
                    (pl.col("timestamp") > start) & (pl.col("timestamp") < end)
                ).select(selection)

            return total_precipitation['total_precipitation'][0]
        except Exception as e:
            err("Error in query_agg_precipitation_acc", e)
    else:
        err("DataFrame is None or empty", ValueError("Invalid input"), ValueError)


def query_all_limit_n(df: pl.DataFrame, n: int) -> pl.DataFrame:
    """
    Returns the first 'n' rows of the provided DataFrame.

    Args:
        df (pl.DataFrame): The DataFrame to query.
        n (int): The number of rows to return.

    Returns:
        pl.DataFrame: A DataFrame containing the first 'n' rows of 'df'.
    """
    if df is not None and not df.is_empty():
        try:
            limit = df.head(n)
            return limit
        except Exception as e:
            err("Error in query_all_limit_n", e)
    else:
        err("DataFrame is None or empty", ValueError("Invalid input"), ValueError)
