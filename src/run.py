"""Set up a dataframe for remote files, execute queries, and write to CSV and markdown."""

import os
import textwrap

from polars import DataFrame, LazyFrame, Series, concat

from fetch import get_basin_deals, get_basin_pubs, get_basin_urls
from query import execute_queries, create_lazyframe
from utils import (
    err,
    format_df_to_markdown,
    format_unix_ms,
    get_current_date,
    wrap_task,
)


def prepare_data() -> LazyFrame:
    """
    Prepare dataframe querying by first getting Basin for publications, deals,
    and remote parquet files, returning a dataframe of the IPFS data.

    Returns
    -------
        LazyFrame: The LazyFrame of IPFS data.

    Raises
    ------
        Exception: If there is an error getting the publications, deals, or
            remote parquet files.
    """
    # Get publications for `xm_data` namespace creator
    pubs = wrap_task(
        lambda: get_basin_pubs("0xfc7C55c4A9e30A4e23f0e48bd5C1e4a865dA06C5"),
        "Getting publications...",
    )
    # Filter for only `xm_data` namespace (get rid of testing-only data)
    active_pubs = [item for item in pubs if item.startswith("xm_data")]
    # Get deals for each publication, also inserting the `namespace.publication`
    # into the returned objects (used in forming URL path for IPFS requests)
    deals = wrap_task(
        lambda: get_basin_deals(active_pubs), "Getting deals for publications..."
    )
    urls = wrap_task(lambda: get_basin_urls(deals), "Forming remote URLs for deals...")
    # Create a LazyFrame from the remote parquet files at the IPFS URLs
    lf = wrap_task(
        lambda: create_lazyframe(urls), "Preparing LazyFrame from remote files..."
    )

    return lf


def run(lf: LazyFrame, start: int | None, end: int | None) -> None:
    """
    Execute queries and write results to files.

    Parameters
    ----------
        lf (LazyFrame): The LazyFrame to query.
        start (int): The start of the query time range (can be None).
        end (int): The end of the query time range (can be None).

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error executing the queries or writing the
            results.
    """
    # Execute queries and get the results as a DataFrame
    # Also, set `start` and `end` if None
    (exec_df, start, end) = wrap_task(
        lambda: execute_queries(lf, start, end), "Executing queries..."
    )
    # Prepare the data and write to files
    wrap_task(
        lambda: write_files(exec_df),
        "Writing results to files...",
    )


def write_files(df: DataFrame, start: int, end: int) -> None:
    """
    Write the run's dataframe results to a csv file for history and markdown
    for current state.

    Parameters
    -------
        df (DataFrame): The run's DataFrame results.
        start (int): The start of the query time range.
        end (int): The end of the query time range.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the results.
    """
    cwd = os.getcwd()
    try:
        prepared = prepare_output(df, start, end)
        write_history_csv(prepared, cwd)
        write_markdown(prepared, cwd)
    except Exception as e:
        err("Error in write_results", e)


def prepare_output(df: DataFrame) -> DataFrame:
    """
    Prepare the DataFrame with the run date and start/end query time range; used
    when writing to files.

    Parameters
    ----------
        df (DataFrame): The run's full DataFrame results.
        start (int): The start of the query time range.
        end (int): The end of the query time range.

    Returns
    -------
        LazyFrame: The full dataframe with run info.
    """
    current_datetime = get_current_date()
    # Include run data qnd query ranges
    run_info = DataFrame(
        [
            Series("job_date", [current_datetime]),
        ]
    )
    df_with_run_info = concat([run_info, df], how="horizontal")

    return df_with_run_info


def write_history_csv(df: DataFrame, cwd: str) -> None:
    """
    Append the run's DataFrame results to a csv file.

    Parameters
    ----------
        df (DataFrame): The run's DataFrame results.
        cwd (str): The current working directory.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the results.
    """
    try:
        history_file = os.path.join(cwd, "history.csv")
        # Check if the file exists—if so, append to it; otherwise, create it
        if os.path.exists(history_file):
            # Open the file in append mode and write without header
            with open(history_file, "a") as f:
                df.write_csv(f, include_header=False)
        else:
            # If the file doesn't exist, create it and write with header
            df.write_csv(history_file)
    except Exception as e:
        err("Error in write_history_csv", e)
        raise


def write_markdown(df: DataFrame, cwd: str) -> None:
    """
    Overwrite the run's DataFrame results to a markdown file.

    Parameters
    ----------
        df (DataFrame): The run's DataFrame results.
        cwd (str): The current working directory.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the results.
    """
    try:
        markdown_file = os.path.join(cwd, "Data.md")
        # Get job date and the start/end of the query range; needed before we
        # get the markdown table variation of the DataFrame
        job_date = df["job_date"][0]
        range_start = df["range_start"][0]
        range_end = df["range_end"][0]
        # Get the DataFrame as markdown tables
        markdown_tables = format_df_to_markdown(df)

        # Write the markdown table to the file with metadata
        with open(markdown_file, "w") as md_file:
            # Format time range
            start_formatted = format_unix_ms(range_start)
            end_formatted = format_unix_ms(range_end)
            # Write to the file
            md_file.write(f"# Data\n\n")
            md_file.write(
                f"_Generated on **{job_date}** for data in range **{start_formatted}** to **{end_formatted}**._\n"
            )
            md_content = textwrap.dedent(
                """
                The schema for the raw data is as follows:

                - `device_id` (varchar): Unique identifier for the device.
                - `timestamp` (bigint): Timestamp (unix milliseconds).
                - `temperature` (double): Temperature (Celsius).
                - `humidity` (double): Relative humidity reading (%).
                - `precipitation_accumulated` (double): Total precipitation (millimeters).
                - `wind_speed` (double): Wind speed (meters per second).
                - `wind_gust` (double): Wind gust (meters per second).
                - `wind_direction` (double): Wind direction (degrees).
                - `illuminance` (double): Illuminance (lux).
                - `solar_irradiance` (double): Solar irradiance (watts per square meter).
                - `fo_uv` (double): UV-related index value.
                - `uv_index` (double): UV index.
                - `precipitation_rate` (double): Precipitation rate (millimeters per hour).
                - `pressure` (double): Pressure (HectoPascals).
                - `model` (varchar): Model of the device (either WXM WS1000 or WXM WS2000).
                - `name` (varchar): Name of the device.
                - `cell_id` (varchar): Cell ID of the device.
                - `lat` (double): Latitude of the cell.
                - `lon` (double): Longitude of the cell.

                Most of the columns above are included in the mean calculations, and there are three additional columns for aggregates:

                - `total_precipitation` (double): Total `precipitation_accumulated` (millimeters).
                - `num_devices` (int): Count of unique `device_id` values.
                - `cell_mode` (varchar): Most common `cell_id` value.

                And three additional columns for run metadata:

                - `job_date` (varchar): Date the job was run.
                - `range_start` (bigint): Start of the query range (unix milliseconds).\n
                """
            )
            md_file.write(md_content)
            md_file.write(f"## Averages & cumulative metrics\n\n")
            md_file.write(markdown_tables)
    except Exception as e:
        err("Error in write_markdown", e)
        raise
