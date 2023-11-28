import sys

from config import command_setup
from run import prepare_data, run
from utils import log_err


def main():
    """
    Entrypoint for the wx_data data CLI.
    - Fetches and queries data from remote parquet files on IPFS/Filecoin.
    - Sets up a Polars LazyFrame.
    - Executes queries for averages across all columns,
    - Executes queries for total precipitation, number of unique devices,
      and the most common cell_id.
    - Writes results to a CSV file, appending to a history file if it exists.
    - Writes results to a markdown file with the latest run information.
    """
    # Get the `start` and `end` (only used in data range queries, default is None)
    start, end = command_setup()

    try:
        lf = prepare_data()  # Fetch remote files & create polars LazyFrame
        run(lf, start, end)  # Execute queries and write results to files

    except RuntimeError as e:
        log_err(f"Error occurred during run: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
