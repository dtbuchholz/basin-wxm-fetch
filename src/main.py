from pathlib import Path
from sys import exit

from config import command_setup
from run import prepare_data, run
from utils import log_err


def main():
    """
    Entrypoint for the wx_data data CLI.
    - Fetches/extracts remote CAR files on IPFS & downloads to local machine.
    - Checks if a local cache exists, and if so, checks if any new deals
      have been made since the last run, and if not, exits early.
    - Sets up a DuckDB in-memory database filled by parquet file data.
    - Executes queries for averages across all columns,
    - Executes queries for total precipitation, number of unique devices,
      and the most common cell_id.
    - Writes results to a CSV file, appending to a history file if it exists.
    - Writes results to a markdown file with the latest run information.
    """
    # Get the `start` and `end` (only used in data range queries, defaults None)
    start, end = command_setup()
    root = Path.cwd()  # Get the root directory of the project

    try:
        db = prepare_data(root)  # Fetch remote files & create DuckDB database
        # If no db is created, then no new deals were found after checking cache
        if db is not None:
            run(db, root, start, end)  # Execute queries and write results to files
        else:
            # All files were processed on a previous run, so we can exit early
            exit(0)

    except RuntimeError as e:
        log_err(f"Error occurred during runtime: {e}")
        exit(1)


if __name__ == "__main__":
    main()
