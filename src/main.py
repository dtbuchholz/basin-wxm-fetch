"""Fetch Basin wxm data, run queries on remote files, and store a history of results in local files."""

import os
import sys

from config import command_setup
from run import execute, set_up_df
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

        # Query for averages across all columns (except device_id, timestamp,
        # model). Also, query total precipitation, number of unique devices, and
        # most common cell_id. Then, write the results to a csv file,
        # appending to a history file if it exists. Include the non-averaging
        # queries and run time as well.
        execute(df, cwd, start, end)

    except RuntimeError as e:
        log_err(f"Error occurred during run: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
