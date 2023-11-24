"""Fetch Basin wxm data, run queries on remote files, and store a history of results in local files."""

import sys
import os

from run import command_setup, set_up_df, execute
from query import get_timerange
from utils import log_err


def main():
    # Get the current working directory (used during file writes)
    cwd = os.getcwd()
    start, end = command_setup()
    # Note: these are only used in queries; the `basin` command does have
    # support for timestamp ranges on deals, but it's not relevant in the
    # current wxm use case since each publication only has one deal

    try:
        # Fetch & query data from remote parquet files, setting up a polars
        # dataframe
        df = set_up_df()
        # If no start or end time was provided, default to the min/max of the dataset
        start, end = get_timerange(df, start, end)

        # Query for averages across all columns (except device_id, timestamp,
        # model). Also, query total precipitation, number of unique devices, and
        # number of unique models. Then, write the results to a csv file,
        # appending to a history file if it exists. Include the non-averaging
        # queries and run time as well.
        execute(df, cwd, start, end)

    except RuntimeError as e:
        log_err(f"Error occurred during run: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
