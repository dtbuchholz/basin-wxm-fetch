"""Configs & command setup for query start time, end time, and verbose logging."""

from argparse import ArgumentParser
from json import load
from typing import Tuple

# Define global setting for verbose traceback logging
log_traceback = True


def command_setup() -> Tuple[int | None, int | None]:
    """
    Sets up command line argument parsing, returning the start and end
    timestamps (defaults to None if not provided). Flags include:
    - `--start`: Start timestamp for data range in unix ms (e.g., 1700438400000)
    - `--end`: End timestamp for data range in unix ms (e.g., 1700783999000)
    - `--verbose`: Enable verbose error logging with tracebacks (default: true)

    Returns:
        (int | None, int | None): The start and end timestamps.
    """
    global log_traceback

    parser = ArgumentParser(description="Fetch wxm vault data and run queries.")
    parser.add_argument(
        "--start",
        type=int,
        default=None,
        help="Start unix timestamp for data range in (e.g., 1700438400)",
    )
    parser.add_argument(
        "--end",
        type=int,
        default=None,
        help="End unix timestamp for data range in (e.g., 1700783999)",
    )
    parser.add_argument(
        "--verbose",
        type=int,
        default=True,
        help="Enable verbose error logging with tracebacks (default: true)",
    )

    # Parse the arguments for start and end time query ranges; also verbose logging
    # Default to `None` and let the queries use full range if no start/end
    args = parser.parse_args()
    start, end, log_traceback = args.start, args.end, args.verbose

    return (start, end)


def get_vaults_config():
    """
    Read the vaults configuration from the `vaults.json` file.
    """
    with open("vaults-config.json", "r") as f:
        vaults_config = load(f)
        return vaults_config
