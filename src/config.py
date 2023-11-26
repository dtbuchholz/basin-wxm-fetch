"""Configs & command setup for query start time, end time, and verbose logging."""

import argparse

# Define global setting for verbose traceback logging
log_traceback = True


def command_setup() -> (int | None, int | None):
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

    parser = argparse.ArgumentParser(
        description="Fetch Basin wxm data and run queries.")
    parser.add_argument("--start", type=int, default=None,
                        help="Start timestamp for data range in unix ms (e.g., 1700438400000)")
    parser.add_argument("--end", type=int, default=None,
                        help="End timestamp for data range in unix ms (e.g., 1700783999000)")
    parser.add_argument("--verbose", type=int, default=True,
                        help="Enable verbose error logging with tracebacks (default: true)")

    # Parse the arguments for start and end time query ranges; also verbose logging
    # Default to `None` and let the queries use full range if no start/end
    args = parser.parse_args()
    start, end, log_traceback = args.start, args.end, args.verbose

    return (start, end)
