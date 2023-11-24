"""Fetch Basin wxm data, run queries on remote files, and store a history of results in local files."""

import sys
import os
import argparse
import logging
import polars as pl
import io
import contextlib
from datetime import datetime as dt

from fetch import get_basin_pubs_legacy, get_basin_deals, get_basin_urls
from query import (get_df, query_agg_precipitation_acc, query_timestamp_range,
                   query_average_all, query_num_unique_devices, query_num_unique_models)

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def main():
    # Get the current working directory (used during file writes)
    cwd = os.getcwd()
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(
        description="Fetch Basin wxm data and run queries.")
    parser.add_argument("--start", type=int, default=None,
                        help="Start timestamp for data range")
    parser.add_argument("--end", type=int, default=None,
                        help="End timestamp for data range")

    # Parse the arguments for start and end time ranges, used in queries
    args = parser.parse_args()
    # Default to `None` and let the queries define the range as min and/or max
    # timestamp from data
    start = args.start
    end = args.end
    # Note: these are only used in queries; the `basin` command does have
    # support for timestamp ranges on deals, but it's not relevant in the
    # current wxm use case since each publication only has one deal

    # Fetch & query data from remote parquet files with a polars dataframe
    try:
        # Get publications for wxm2 namespace creator (note: must use legacy contract)
        pubs = get_basin_pubs_legacy(
            "0x64251043A35ab5D11f04111B8BdF7C03BE9cF0e7")
        # Filter for only wxm2 namespace (gets rid of testing-only data)
        active_pubs = [item for item in pubs if item.startswith('wxm2')]

        # Get deals for each publication, also inserting the `namespace.publication`
        # into the returned objects (used in forming URL path for IPFS requests)
        deals = get_basin_deals(active_pubs)
        urls = get_basin_urls(deals)
        # Create a dataframe from the remote parquet files at the IPFS URLs
        df = get_df(urls)

        # If no start or end time was provided, default to the min/max of the dataset
        # Note: timestamp range for Oct 15-21 data: 1697328000000 to 1697932798895
        if start is None or end is None:
            # Get timestamp range from data
            start_time, end_time = query_timestamp_range(df)
            if start is None:
                start = start_time
            if end is None:
                end = end_time
        logging.info(f"Timestamp range: {start} to {end}:")

        logging.info(f"Executing queries...")
        # Query for averages across all columns (except device_id, timestamp,
        # model). Also, query total precipitation, number of unique devices, and
        # number of unique models.
        averages = query_average_all(df, start, end)
        total_precipitation = query_agg_precipitation_acc(df, start, end)
        num_devices = query_num_unique_devices(df)
        num_models = query_num_unique_models(df)

        logging.info("Writing results to results files...")
        # Write the results to a csv file, appending to a history file if it
        # exists. Include the non-averaging queries and run time as well.
        current_datetime = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        df_with_run_date = averages.insert_column(0, pl.Series(
            "job_date", [current_datetime]))
        df_final = df_with_run_date.with_columns([
            pl.Series("total_precipitation", [total_precipitation]),
            pl.Series("num_devices", [num_devices]),
            pl.Series("num_models", [num_models])
        ])
        # Check if the file exists—if so, append to it; otherwise, create it
        history_file = os.path.join(cwd, "history.csv")
        if os.path.exists(history_file):
            # Open the file in append mode and write without header
            with open(history_file, 'a') as f:
                df_final.write_csv(f, include_header=False)
        else:
            # If the file doesn't exist, create it and write with header
            df_final.write_csv(history_file)

        # Overwrite the summary results to a markdown file, first converting
        # column names from snake case to sentence case
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
                df_final.columns = [col.replace('_', ' ').capitalize()
                                    for col in df_final.columns]
                print(df_final)
        markdown_table = output.getvalue()

        # Write the markdown table to the file with metadata
        with open(markdown_file, 'w') as md_file:
            # Format run date and time range
            current_date = current_datetime.split(' ')[0]
            start_formatted = dt.utcfromtimestamp(
                start / 1000).strftime("%Y-%m-%d %H:%M:%S")
            end_formatted = dt.utcfromtimestamp(
                end / 1000).strftime("%Y-%m-%d %H:%M:%S")
            # Write to the file
            md_file.write(f"## Data\n\n")
            md_file.write(
                f"Generated on _{current_date}_ for data in range _{start_formatted}_ to _{end_formatted}_\n\n")
            md_file.write(markdown_table)

    except RuntimeError as e:
        logging.error(f"Error occurred during run: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
