"""Set up an in-memory database for remote files, execute queries, and write to CSV and markdown."""

import sys
from contextlib import redirect_stdout
from io import StringIO
from math import ceil
from pathlib import Path
from textwrap import dedent

from duckdb import DuckDBPyConnection
from polars import Config, DataFrame, Series, concat

from fetch import (
    check_deals_cache,
    extract_deals,
    get_basin_deals,
    get_basin_pubs,
    write_deals_cache,
)
from query import create_database, execute_queries
from utils import err, format_unix_ms, get_current_date, log_info, wrap_task


def prepare_data(root: Path) -> DuckDBPyConnection | None:
    """
    Prepare an in-memory database for querying by first getting Basin for
    publications, deals, CAR files, and extracted parquet files.

    Parameters
    ----------
        root (Path): The root directory of the project.

    Returns
    -------
        DuckDBPyConnection | None: The in-memory database connection, or None if
            there are no new deals to process.


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
    namespace = "xm_data"
    active_pubs = [item for item in pubs if item.startswith(namespace)]

    # Get deals for each publication, also inserting the `namespace.publication`
    # into the returned objects (used in forming URL path for IPFS requests)
    deals = wrap_task(
        lambda: get_basin_deals(active_pubs), "Getting deals for publications..."
    )
    num_deals = len(deals)
    log_info(f"Number of deals found: {num_deals}")

    # Check if there is a cache file; if so, read it and filter out deals that
    # have already been downloaded
    cache_file = root / "cache.json"
    new_deals = check_deals_cache(deals, cache_file)
    # Exit early if no new deals exist
    log_info(f"Number of new deals: {len(new_deals)}")
    if len(new_deals) == 0:
        log_info("No new deals found, exiting...")
        return None

    # Write the updated deals to the cache file
    write_deals_cache(deals, cache_file)

    # Set up the directory for the extracted parquet files
    data_dir = root / "inputs"
    if not Path.exists(data_dir):
        Path.mkdir(data_dir)
    # Retrieve deals & extract the parquet files from each CAR file
    wrap_task(
        lambda: extract_deals(new_deals, data_dir), "Extracting data from deals..."
    )
    # Create a in-memory database from the parquet files
    db = wrap_task(
        lambda: create_database(data_dir),
        "Creating database with parquet files...",
    )

    return db


def run(db: DuckDBPyConnection, root: Path, start: int | None, end: int | None) -> None:
    """
    Execute queries and write results to files.

    Parameters
    ----------
        db (DuckDBPyConnection): The database to query.
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
    # Execute queries and get the results as a polars DataFrame
    # Also, set `start` and `end` time dynamically, if None
    df = wrap_task(lambda: execute_queries(db, start, end), "Executing queries...")
    # Prepare the data and write to files
    wrap_task(
        lambda: write_files(df, root),
        "Writing results to files...",
    )


def write_files(df: DataFrame, root: Path) -> None:
    """
    Write the run's dataframe results to a csv file for history and markdown
    for current state.

    Parameters
    -------
        df (DataFrame): The run's polars DataFrame results.
        start (int): The start of the query time range.
        end (int): The end of the query time range.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the results.
    """
    try:
        prepared = prepare_output(df)
        write_history_csv(prepared, root)
        write_markdown(prepared, root)
    except Exception as e:
        err("Error in write_results", e)


def prepare_output(df: DataFrame) -> DataFrame:
    """
    Prepare the DataFrame with the run date and start/end query time range; used
    when writing to files.

    Parameters
    ----------
        df (DataFrame): The run's polars DataFrame results.
        start (int): The start of the query time range.
        end (int): The end of the query time range.

    Returns
    -------
        DataFrame: The full polars DataFrame with run info.
    """
    current_datetime = get_current_date()
    # Include run data qnd query ranges
    run_info = DataFrame([Series("job_date", [current_datetime])])
    df_with_run_info = concat([run_info, df], how="horizontal")

    return df_with_run_info


def write_history_csv(df: DataFrame, root: Path) -> None:
    """
    Append the run's DataFrame results to a csv file.

    Parameters
    ----------
        df (DataFrame): The run's polars DataFrame results.
        root (Path): The root directory for the program.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the results.
    """
    try:
        history_file = Path(root) / "history.csv"
        # Check if the file exists—if so, append to it; otherwise, create it
        if Path.exists(history_file):
            # Open the file in append mode and write without header
            with open(history_file, "a") as csv_file:
                df.write_csv(csv_file, include_header=False)
        else:
            # If the file doesn't exist, create it and write with header
            df.write_csv(history_file)
    except Exception as e:
        err("Error in write_history_csv", e)
        raise


def write_markdown(df: DataFrame, root: Path) -> None:
    """
    Overwrite the run's DataFrame results to a markdown file.

    Parameters
    ----------
        df (DataFrame): The run's polars DataFrame results.
        root (Path): The root directory for the program.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the results.
    """
    try:
        markdown_file = Path(root) / "Data.md"
        # Get job date and the start/end of the query range; needed before we
        # get the markdown table variation of the DataFrame
        job_date = df["job_date"][0]
        range_start = df["range_start"][0]
        range_end = df["range_end"][0]
        # Get the DataFrame as markdown tables
        markdown_tables = convert_df_to_markdown(df)

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
            md_content = dedent(
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
                - `number_of_devices` (int): Count of unique `device_id` values.
                - `cell_id_mode` (varchar): Most common `cell_id` value.

                And three additional columns for run metadata:

                - `job_date` (varchar): Date the job was run.
                - `range_start` (bigint): Start of the query range (unix milliseconds).
                - `range_end` (bigint): End of the query range (unix milliseconds).\n
                """
            )
            md_file.write(md_content)
            md_file.write(f"## Averages & cumulative metrics\n\n")
            md_file.write(markdown_tables)
    except Exception as e:
        err("Error in write_markdown", e)
        raise


def convert_df_to_markdown(df: DataFrame) -> str:
    """
    Format a DataFrame to a markdown table, splitting into two tables of
    approximately equal widths.
    """
    split_at = ceil(df.width / 2)
    df1 = df.select(df.columns[:split_at])
    df2 = df.select(df.columns[split_at:])

    output = StringIO()
    with redirect_stdout(output):
        with Config(
            tbl_formatting="ASCII_MARKDOWN",
            tbl_hide_column_data_types=True,
            tbl_hide_dataframe_shape=True,
            set_tbl_width_chars=5000,  # Prevent line wrapping
            float_precision=3,  # Show 3 decimal points
            set_fmt_float="full",  # Show full float precision
            set_tbl_cols=-1,  # Show all columns
        ):
            # Replace underscores with spaces and capitalize column names
            df.columns = [col.replace("_", " ").capitalize() for col in df.columns]
            print(df1)
            print()
            print(df2)
    return output.getvalue()
