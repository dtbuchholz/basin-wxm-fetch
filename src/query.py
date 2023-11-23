
"""Query wxm data."""

import logging
import os
import traceback
import polars as pl

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Read from parquet files in a data directory
#
# Schema is as follows:
#
# device_id (varchar)
# timestamp (bigint)
# temperature (double)
# humidity (double)
# precipitation_accumulated (double)
# wind_speed (double)
# wind_gust (double)
# wind_direction (double)
# illuminance (double)
# solar_irradiance (double)
# fo_uv (double)
# uv_index (double)
# precipitation_rate (double)
# pressure (double)
# model (varchar)


def get_df(data_dir: str) -> pl.LazyFrame:
    # Check if 'data' directory exists
    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"No directory found at '{data_dir}'.")

    try:
        files = []
        for root, _, filenames in os.walk(data_dir):
            for filename in filenames:
                if filename.endswith('.parquet'):
                    file_path = os.path.join(root, filename)
                    files.append(file_path)

        if files:
            df = pl.scan_parquet(files)
            return df
        else:
            print(f"No parquet files found in '{data_dir}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()


def query_num_unique_devices(df: pl.LazyFrame):
    unique_devices = df.select(pl.col("device_id").unique()).collect()
    count = unique_devices.shape[0]

    return count


def query_num_unique_models(df: pl.LazyFrame):
    unique_devices = df.select(pl.col("model").unique()).collect()
    count = unique_devices.shape[0]

    return count


def query_agg_precipitation_acc(df: pl.LazyFrame, start: int, end: int):
    total_precipitation = df.select(
        pl.col("precipitation_accumulated")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .sum()
        .alias("total_precipitation")
    )

    value = total_precipitation.collect()[0, "total_precipitation"]

    return value


def query_all_limit_n(df: pl.LazyFrame, n: int):
    limit = df.head(n).collect()
    print(limit)
    return limit


def query_timestamp_range(df: pl.LazyFrame) -> (int, int):
    min_max_value = df.select([
        pl.col("timestamp").min().alias("min_timestamp"),
        pl.col("timestamp").max().alias("max_timestamp")
    ])
    result = min_max_value.collect()

    # Extract the min and max values
    min_timestamp = result[0, "min_timestamp"]
    max_timestamp = result[0, "max_timestamp"]

    return (min_timestamp, max_timestamp)


# Individual queries for each column—less efficient than query_average_all

def query_average_all(df: pl.LazyFrame, start: int, end: int) -> (pl.DataFrame, list[str]):
    # List of columns to calculate the average
    columns = ["temperature", "humidity", "precipitation_accumulated",
               "wind_speed", "wind_gust", "wind_direction",
               "illuminance", "solar_irradiance", "fo_uv",
               "uv_index", "precipitation_rate", "pressure"]

    # Prepare selection expressions
    selection = [(pl.col(col).mean().alias(col)) for col in columns]

    # Apply the selection
    averages = df.filter(
        (pl.col("timestamp") > start) & (pl.col("timestamp") < end)
    ).select(selection)

    # Collect the result
    result = averages.collect()

    return (result, columns)


def query_avg_temp(df: pl.LazyFrame, start: int, end: int):
    avg_precipitation = df.select(
        pl.col("temperature")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .mean()
        .alias("temperature")
    )

    value = avg_precipitation.collect()[0, "temperature"]

    return value


def query_avg_humidity(df: pl.LazyFrame, start: int, end: int):
    avg_precipitation = df.select(
        pl.col("humidity")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .mean()
        .alias("humidity")
    )
    value = avg_precipitation.collect()[0, "humidity"]

    return value


def query_avg_precipitation_acc(df: pl.LazyFrame, start: int, end: int):
    avg_precipitation = df.select(
        pl.col("precipitation_accumulated")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .mean()
        .alias("avg_precipitation")
    )

    value = avg_precipitation.collect()[0, "avg_precipitation"]

    return value


def query_avg_precipitation_rate(df: pl.LazyFrame, start: int, end: int):
    avg_precipitation = df.select(
        pl.col("precipitation_rate")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .mean()
        .alias("precipitation_rate")
    )

    value = avg_precipitation.collect()[0, "precipitation_rate"]

    return value


def query_avg_wind_speed(df: pl.LazyFrame, start: int, end: int):
    avg_precipitation = df.select(
        pl.col("wind_speed")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .mean()
        .alias("wind_speed")
    )

    value = avg_precipitation.collect()[0, "wind_speed"]

    return value


def query_avg_wind_gust(df: pl.LazyFrame, start: int, end: int):
    avg_precipitation = df.select(
        pl.col("wind_gust")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .mean()
        .alias("wind_gust")
    )

    value = avg_precipitation.collect()[0, "wind_gust"]

    return value


def query_avg_wind_direction(df: pl.LazyFrame, start: int, end: int):
    avg_precipitation = df.select(
        pl.col("wind_direction")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .mean()
        .alias("wind_direction")
    )

    value = avg_precipitation.collect()[0, "wind_direction"]

    return value


def query_avg_illuminance(df: pl.LazyFrame, start: int, end: int):
    avg_precipitation = df.select(
        pl.col("illuminance")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .mean()
        .alias("illuminance")
    )

    value = avg_precipitation.collect()[0, "illuminance"]

    return value


def query_avg_solar_irradiance(df: pl.LazyFrame, start: int, end: int):
    avg_precipitation = df.select(
        pl.col("solar_irradiance")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .mean()
        .alias("solar_irradiance")
    )

    value = avg_precipitation.collect()[0, "solar_irradiance"]

    return value


def query_avg_fo_uv(df: pl.LazyFrame, start: int, end: int):
    avg_precipitation = df.select(
        pl.col("fo_uv")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .mean()
        .alias("fo_uv")
    )

    value = avg_precipitation.collect()[0, "fo_uv"]

    return value


def query_avg_uv_index(df: pl.LazyFrame, start: int, end: int):
    avg_precipitation = df.select(
        pl.col("uv_index")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .mean()
        .alias("uv_index")
    )

    value = avg_precipitation.collect()[0, "uv_index"]

    return value


def query_avg_pressure(df: pl.LazyFrame, start: int, end: int):
    avg_precipitation = df.select(
        pl.col("uv_index")
        .where(
            (pl.col("timestamp") > start) &
            (pl.col("timestamp") < end)
        )
        .mean()
        .alias("uv_index")
    )

    value = avg_precipitation.collect()[0, "uv_index"]

    return value
