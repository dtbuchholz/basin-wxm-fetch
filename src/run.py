"""Fetch Basin wxm data, run queries on remote files, and store a history of results in local files."""

import os
import argparse
import io
import contextlib
from datetime import datetime as dt
import polars as pl

from fetch import get_basin_pubs_legacy, get_basin_deals, get_basin_urls
from query import get_df, execute_queries
from utils import format_unix, err, wrap_task


# Set up command line argument parsing, returning the start and end timestamps
def command_setup() -> (int, int):
    parser = argparse.ArgumentParser(
        description="Fetch Basin wxm data and run queries.")
    parser.add_argument("--start", type=int, default=None,
                        help="Start timestamp for data range in unix milliseconds")
    parser.add_argument("--end", type=int, default=None,
                        help="End timestamp for data range in unix milliseconds")

    # Parse the arguments for start and end time ranges, used in queries
    args = parser.parse_args()
    # Default to `None` and let the queries define the range as min and/or max
    # timestamp from data
    start = args.start
    end = args.end
    return (start, end)


# Query Basin for publications, deals, and remote parquet files, returning a
# dataframe of the IPFS data
def set_up_df() -> pl.DataFrame:
    # Get publications for wxm2 namespace creator (note: must use legacy
    # contract)
    pubs = wrap_task(lambda: get_basin_pubs_legacy(
        "0x64251043A35ab5D11f04111B8BdF7C03BE9cF0e7"), "Getting publications...")
    # Filter for only wxm2 namespace (gets rid of testing-only data)
    active_pubs = [item for item in pubs if item.startswith('wxm2')]

    # Get deals for each publication, also inserting the `namespace.publication`
    # into the returned objects (used in forming URL path for IPFS requests)
    deals = wrap_task(lambda: get_basin_deals(
        active_pubs), "Getting deals for publications...")
    urls = wrap_task(lambda: get_basin_urls(deals),
                     "Forming remote URLs for deals...")
    # Create a dataframe from the remote parquet files at the IPFS URLs
    df = wrap_task(lambda: get_df(urls),
                   "Creating dataframe from remote (IPFS)...")
    return df


# Execute queries and write results to files
def execute(df: pl.DataFrame, cwd: str, start: int, end: int) -> None:
    averages, total_precipitation, num_devices, num_models = wrap_task(lambda: execute_queries(
        df, start, end),
        "Executing queries...")
    df_final = format_df(averages, total_precipitation,
                         num_devices, num_models)
    wrap_task(lambda: write_results(df_final, cwd, start, end),
              "Writing results to files...")


# Format the dataframe with the run date, total precipitation, and number of devices and models
def format_df(averages: pl.DataFrame, total_precipitation: float, num_devices: int, num_models: int) -> pl.DataFrame:
    current_datetime = dt.now().strftime("%Y-%m-%d %H:%M:%S")
    df_with_run_date = averages.insert_column(0, pl.Series(
        "job_date", [current_datetime]))
    df = df_with_run_date.with_columns([
        pl.Series("total_precipitation", [total_precipitation]),
        pl.Series("num_devices", [num_devices]),
        pl.Series("num_models", [num_models])
    ])

    return df


# Write to a csv file for history and markdown for current state
def write_results(df: pl.DataFrame, cwd: str, start: int, end: int) -> None:
    write_history_csv(df, cwd)
    write_markdown(df, cwd, start, end)


# Append the run's dataframe results to a csv file
def write_history_csv(df: pl.DataFrame, cwd: str) -> None:
    try:
        history_file = os.path.join(cwd, "history.csv")
        # Check if the file exists—if so, append to it; otherwise, create it
        if os.path.exists(history_file):
            # Open the file in append mode and write without header
            with open(history_file, 'a') as f:
                df.write_csv(f, include_header=False)
        else:
            # If the file doesn't exist, create it and write with header
            df.write_csv(history_file)
    except Exception as e:
        err("Error in write_history_csv", e)
        raise


# Overwrite the run's dataframe results to a markdown file
def write_markdown(df: pl.DataFrame, cwd: str, start: int, end: int) -> None:
    # Overwrite the summary results to a markdown file, first converting
    # column names from snake case to sentence case
    try:
        markdown_file = os.path.join(cwd, "Data.md")

        # Capture stdout, which prints a markdown table of the dataframe
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with pl.Config(
                tbl_formatting="ASCII_MARKDOWN",
                tbl_hide_column_data_types=True,
                tbl_hide_dataframe_shape=True,
                set_tbl_width_chars=5000,  # Prevent line wrapping
                float_precision=3,  # Show 3 decimal points
                set_fmt_float="full",  # Show full float precision
                set_tbl_cols=-1  # Show all columns
            ):
                # Replace underscores with spaces and capitalize column names
                df.columns = [col.replace('_', ' ').capitalize()
                              for col in df.columns]
                print(df)
        markdown_table = output.getvalue()

        # Write the markdown table to the file with metadata
        with open(markdown_file, 'w') as md_file:
            # Format run date and time range
            current_date = dt.now().strftime("%Y-%m-%d")
            start_formatted = format_unix(start)
            end_formatted = format_unix(end)
            # Write to the file
            md_file.write(f"## Data\n\n")
            md_file.write(
                f"Generated on _{current_date}_ for data in range _{start_formatted}_ to _{end_formatted}_\n\n")
            md_file.write(markdown_table)
    except Exception as e:
        err("Error in write_markdown", e)
        raise
