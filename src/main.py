import os
import sys

from config import command_setup
from run import execute, set_up_df
from utils import log_err


def main():
    """
    Entrypoint for the wx_data data CLI.
    - Fetches and queries data from remote parquet files.
    - Sets up a Polars DataFrame.
    - Executes queries for averages across all columns,
    - Executes queries for total precipitation, number of unique devices,
      and the most common cell_id.
    - Writes results to a CSV file, appending to a history file if it exists.
    """
    # Get the current working directory (used during file writes)
    cwd = os.getcwd()
    start, end = command_setup()
    # Note: the `start` and `end` args are only used in data range queries

    try:
        df = set_up_df()  # Fetch & query data from remote parquet files
        execute(df, cwd, start, end)  # Execute queries and write results

    except RuntimeError as e:
        log_err(f"Error occurred during run: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
