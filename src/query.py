"""Query wxm data at remote IPFS parquet files using polars."""

from polars import DataFrame, LazyFrame, col, lit, scan_parquet
from polars.exceptions import ComputeError

from utils import err, log_warn

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


def create_lazyframe(remote_files: list[str], max_retries: int = 3) -> LazyFrame:
    """
    Create a dataframe from remote parquet files.

    Parameters
    ----------
        remote_files (list[str]): The list of remote parquet files.
        max_retries (int): The maximum number of retries. Defaults to 3.

    Returns
    -------
        LazyFrame: A Polars LazyFrame from the remote parquet files.

    Raises
    ------
        Exception: If there is an error creating the dataframe.
    """
    if not remote_files:
        err("No remote parquet files provided", ValueError("Invalid input"), ValueError)

    attempt = 0  # Use retry logic in case of `operation timed out`
    while attempt < max_retries:
        try:
            # Use a LazyFrame for *significantly* faster performance & lower
            # memory usage than collecting into a DataFrame
            lf = scan_parquet(source=remote_files, cache=True, retries=3)

            return lf
        except ComputeError as e:
            error_message = str(e)
            if "operation timed out" in error_message:
                attempt += 1
                log_warn(
                    f"Attempt {attempt}/{max_retries} failed: request timed out. Retrying..."
                )
            elif "429 Too Many Requests" in error_message:
                # Catch 429 errors (consider adding retry logic here)
                err(
                    "Error in create_dataframe: 429 Too Many Requests",
                    ComputeError("IPFS rate limit exceeded"),
                    ComputeError,
                )
            else:
                # For any other ComputeError, don't retry and just raise the error
                err(
                    f"Error in create_dataframe: {error_message}",
                    ComputeError("Polars request failed"),
                    ComputeError,
                )
        except Exception as e:
            # For any non-ComputeError exceptions, handle them immediately
            err("Error in create_dataframe", e)

    # Throw if all retries failed
    err(
        "Error in create_dataframe: All retries failed",
        ComputeError("Polars timeout"),
        ComputeError,
    )


def execute_queries(
    lf: LazyFrame, start: int | None, end: int | None, max_retries: int = 3
) -> DataFrame:
    """
    Execute all queries and return the results as a DataFrame.

    Parameters
    ----------
        lf (LazyFrame): The LazyFrame to query.
        start (int): The start of the query time range (can be None).
        end (int): The end of the query time range (can be None).
        max_retries (int): The maximum number of retries. Defaults to 3.

    Returns
    -------
        DataFrame: The results of the queries.

    Raises
    ------
        Exception: If there is an error executing the queries.
    """
    # Define the columns to calculate the averages; this excludes the device_id,
    # timestamp, model, name, cell_id and lat/long.
    columns = [
        "temperature",
        "humidity",
        "precipitation_accumulated",
        "wind_speed",
        "wind_gust",
        "wind_direction",
        "illuminance",
        "solar_irradiance",
        "fo_uv",
        "uv_index",
        "precipitation_rate",
        "pressure",
    ]
    average_selection = [(col(column).mean().alias(column)) for column in columns]

    # Apply conditional filtering based on 'start' and 'end'
    if start is not None and end is not None:
        filter_condition = (col("timestamp") > start) & (col("timestamp") < end)
    elif start is not None:
        filter_condition = col("timestamp") > start
    elif end is not None:
        filter_condition = col("timestamp") < end
    else:
        filter_condition = lit(True)

    # Query & execute the averages across all columns. Also, query total
    # precipitation, number of unique devices, most common cell_id, and the min
    # and max timestamps for the range.
    attempt = 0  # Use retry logic in case of `operation timed out`
    while attempt < max_retries:
        try:
            combined_query = lf.filter(filter_condition).select(
                [
                    col("timestamp").min().alias("range_start"),
                    col("timestamp").max().alias("range_end"),
                    col("device_id").n_unique().alias("number_of_devices"),
                    col("cell_id").mode().first().alias("cell_id_mode"),
                    col("precipitation_accumulated").sum().alias("total_precipitation"),
                ]
                + average_selection
            )
            result = combined_query.collect()

            return result
        except ComputeError as e:
            error_message = str(e)
            if "operation timed out" in error_message:
                attempt += 1
                log_warn(
                    f"Attempt {attempt}/{max_retries} failed: request timed out. Retrying..."
                )
            elif "429 Too Many Requests" in error_message:
                # Catch 429 errors (consider adding retry logic here)
                err(
                    "Error in execute_queries: 429 Too Many Requests",
                    ComputeError("IPFS rate limit exceeded"),
                    ComputeError,
                )
            else:
                # For any other ComputeError, don't retry and just raise the error
                err(
                    f"Error in execute_queries: {error_message}",
                    ComputeError("Polars request failed"),
                    ComputeError,
                )
        except Exception as e:
            err("Error in exec_all", e)

    # Throw if all retries failed
    err(
        "Error in execute_queries: All retries failed",
        ComputeError("Polars timeout"),
        ComputeError,
    )


def query_all_limit_n(lf: LazyFrame, n: int) -> DataFrame:
    """
    Returns the first 'n' rows of the provided DataFrame. Used for testing.

    Parameters
    ----------
        lf (LazyFrame): The LazyFrame to query.
        n (int): The number of rows to return.

    Returns
    -------
        LazyFrame: A DataFrame containing the first 'n' rows of 'df'.

    Raises
    ------
        Exception: If there is an error querying the first 'n' rows.
    """
    if lf is not None:
        try:
            limit = lf.head(n)
            return limit
        except Exception as e:
            err("Error in query_all_limit_n", e)
    else:
        err("No LazyFrame argument", ValueError("Invalid input"), ValueError)
