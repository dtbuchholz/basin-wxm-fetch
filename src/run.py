"""Set up a dataframe for remote files, execute queries, and write to CSV and markdown."""

import contextlib
import io
import os

import polars as pl

from fetch import get_basin_deals, get_basin_pubs, get_basin_urls
from query import (get_df, query_average_all, query_agg_precipitation_acc,
                   query_mode_cell_id, query_num_unique_devices, query_timestamp_range)
from utils import err, get_current_date, format_unix_ms, wrap_task


# Query Basin for publications, deals, and remote parquet files, returning a
# dataframe of the IPFS data
def set_up_df() -> pl.DataFrame:
    # Get publications for wxm2 namespace creator (note: must use legacy
    # contract)
    pubs = wrap_task(lambda: get_basin_pubs(
        "0xfc7C55c4A9e30A4e23f0e48bd5C1e4a865dA06C5"), "Getting publications...")
    # Filter for only `xm_data` namespace (get rid of testing-only data)
    active_pubs = [item for item in pubs if item.startswith("xm_data")]
    # Get deals for each publication, also inserting the `namespace.publication`
    # into the returned objects (used in forming URL path for IPFS requests)
    deals = wrap_task(lambda: get_basin_deals(
        active_pubs), "Getting deals for publications...")
    urls = wrap_task(lambda: get_basin_urls(deals),
                     "Forming remote URLs for deals...")
    # Create a dataframe from the remote parquet files at the IPFS URLs
    df = wrap_task(lambda: get_df(urls),
                   "Creating dataframe from remote IPFS files...")

    return df


# Execute queries and write results to files
def execute(df: pl.DataFrame, cwd: str, start: int | None, end: int | None) -> None:
    (
        averages,
        total_precipitation,
        num_devices,
        cell_mode,
        start,
        end
    ) = wrap_task(lambda: execute_queries(df, start, end),
                  "Executing queries...")
    df_final = prepare_df(averages, total_precipitation,
                          num_devices, cell_mode, start, end)
    wrap_task(lambda: write_results(df_final, cwd),
              "Writing results to files...")


# Execute all queries on the dataframe for a given time range
def execute_queries(df: pl.DataFrame, start: int | None, end: int | None):
    # Query for averages across all columns (except device_id, timestamp,
    # model, name, cell_id and lat/long). Also, query total precipitation,
    # number of unique devices, and number of unique models.
    try:
        averages = query_average_all(df, start, end)
        total_precipitation = query_agg_precipitation_acc(df, start, end)
        num_devices = query_num_unique_devices(df, start, end)
        cell_mode = query_mode_cell_id(df, start, end)
        # When writing to files, if `start` or `end` are None, query the min/max
        # timestamp values in the dataframe
        if start is None or end is None:
            min, max = query_timestamp_range(df)
            start = min if start is None else start
            end = max if end is None else end

        return (averages, total_precipitation, num_devices, cell_mode, start, end)
    except Exception as e:
        err("Error in execute_queries", e)


# Format the dataframe with the run date, total precipitation, device count,
# cell mode, and start/end query time range—used when writing to files
def prepare_df(averages: pl.DataFrame, total_precipitation: float, num_devices: int, cell_mode: int, start: int, end: int) -> pl.DataFrame:
    current_datetime = get_current_date()
    # Include run data qnd query ranges
    run_info = pl.DataFrame([
        pl.Series("job_date", [current_datetime]),
        pl.Series("range_start", [start]),
        pl.Series("range_end", [end])
    ])
    averages_with_run_info = pl.concat([run_info, averages], how='horizontal')
    # Also include non-averaging data
    df = averages_with_run_info.with_columns([
        pl.Series("total_precipitation", [total_precipitation]),
        pl.Series("num_devices", [num_devices]),
        pl.Series("cell_mode", [cell_mode])
    ])

    return df

# Write to a csv file for history and markdown for current state


def write_results(df: pl.DataFrame, cwd: str) -> None:
    write_history_csv(df, cwd)
    write_markdown(df, cwd)


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
def write_markdown(df: pl.DataFrame, cwd: str) -> None:
    # Overwrite the summary results to a markdown file, first converting
    # column names from snake case to sentence case
    try:
        markdown_file = os.path.join(cwd, "Data.md")
        # Get job date and the start/end of the query range; needed before we
        # make formatting changes to the dataframe
        job_date = df['job_date'][0]
        range_start = df['range_start'][0]
        range_end = df['range_end'][0]

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
            # Format time range
            start_formatted = format_unix_ms(range_start)
            end_formatted = format_unix_ms(range_end)
            # Write to the file
            md_file.write(f"## Data\n\n")
            md_file.write(
                f"Generated on _{job_date}_ for data in range _{start_formatted}_ to _{end_formatted}_\n\n")
            md_file.write(markdown_table)
    except Exception as e:
        err("Error in write_markdown", e)
        raise
