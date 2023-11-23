"""Fetch Basin wxm data and run queries."""

import argparse
from pathlib import Path
import logging

from fetch import get_basin_pubs_legacy, get_basin_deals, extract
from query import get_df, query_agg_precipitation_acc, query_timestamp_range, query_average_all, query_num_unique_devices, query_num_unique_models

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def main():
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
    # Note: these are only used in queries; the `basin` command does have
    # support for timestamp ranges on deals, but it's not relevant in the
    # current wxm use case since each publication only has one deal
    start = args.start
    end = args.end

    # Get publications for wxm2 namespace creator (note: must use legacy contract)
    pubs = get_basin_pubs_legacy("0x64251043A35ab5D11f04111B8BdF7C03BE9cF0e7")
    if len(pubs) == 0:
        print("No publications found")
        return

    # Filter for only wxm2 namespace (gets rid of testing-only data)
    active_pubs = [item for item in pubs if item.startswith('wxm2')]

    # Get deals for each publication
    deals = get_basin_deals(active_pubs)
    if len(deals) == 0:
        print("No deals found")
        return

    # Extract CAR files from deals into 'data' directory (creates if not exists)
    data_dir = Path("data")
    extract(deals, data_dir)
    # Read data from parquet files
    df = get_df(data_dir)
    # Note: timestamp range for Oct 15-21 data: 1697328000000 to 1697932798895
    if start is None or end is None:
        # Get timestamp range from data
        start_time, end_time = query_timestamp_range(get_df(data_dir))
        if start is None:
            start = start_time
        if end is None:
            end = end_time

    logging.info(f"Timestamp range {start} to {end}:")
    # Query for averages across all columns (except device_id, timestamp, model)
    averages, columns = query_average_all(df, start, end)
    # Format and print the result
    logging.info(f"Averages:")
    formatted_values = {
        col: f"{averages[0, col]:.16f}" for col in columns}
    for col, val in formatted_values.items():
        print(f"  {col}: {val}")
    print("Total precipitation accumulated")
    total_precipitation = query_agg_precipitation_acc(df, start, end)
    print(f"  {total_precipitation:.16f}")
    print("Number of unique devices")
    num_devices = query_num_unique_devices(df)
    print(f"  {num_devices}")
    print("Number of unique models")
    num_models = query_num_unique_models(df)
    print(f"  {num_models}")


if __name__ == "__main__":
    main()
