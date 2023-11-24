
"""Query wxm data."""

import logging
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


def get_df(remote_files: list[str]) -> pl.DataFrame:
    logging.info(f"Creating dataframe from remote (IPFS)...")
    try:
        if len(remote_files) > 0:
            df = pl.scan_parquet(remote_files).collect()
            return df
        else:
            logging.info(f"No remote parquet files could be retrieved.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")


def query_num_unique_devices(df: pl.DataFrame) -> int:
    if df is not None:
        unique_devices = df.select(pl.col("device_id").unique())
        count = unique_devices.shape[0]

        return count
    else:
        logging.info(f"Dataframe is empty")


def query_num_unique_models(df: pl.DataFrame) -> int:
    if df is not None:
        unique_devices = df.select(pl.col("model").unique())
        count = unique_devices.shape[0]

        return count
    else:
        logging.info(f"Dataframe is empty")


def query_agg_precipitation_acc(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        total_precipitation = df.select(
            pl.col("precipitation_accumulated")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .sum()
            .alias("total_precipitation")
        )

        value = total_precipitation[0, "total_precipitation"]

        return value
    else:
        logging.info(f"Dataframe is empty")


def query_all_limit_n(df: pl.DataFrame, n: int) -> int:
    if df is not None:
        limit = df.head(n)
        return limit
    else:
        logging.info(f"Dataframe is empty")


def query_timestamp_range(df: pl.DataFrame) -> (int, int):
    if df is not None:
        min_max_value = df.select([
            pl.col("timestamp").min().alias("min_timestamp"),
            pl.col("timestamp").max().alias("max_timestamp")
        ])
        result = min_max_value

        # Extract the min and max values
        min_timestamp = result[0, "min_timestamp"]
        max_timestamp = result[0, "max_timestamp"]

        return (min_timestamp, max_timestamp)
    else:
        logging.info(f"Dataframe is empty")


# Individual queries for each column—less efficient than query_average_all

def query_average_all(df: pl.DataFrame, start: int, end: int) -> (int, list[str]):
    if df is not None:
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
        result = averages

        return (result, columns)
    else:
        logging.info(f"Dataframe is empty")


def query_avg_temp(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        avg_precipitation = df.select(
            pl.col("temperature")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .mean()
            .alias("temperature")
        )

        value = avg_precipitation[0, "temperature"]

        return value
    else:
        logging.info(f"Dataframe is empty")


def query_avg_humidity(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        avg_precipitation = df.select(
            pl.col("humidity")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .mean()
            .alias("humidity")
        )
        value = avg_precipitation[0, "humidity"]

        return value
    else:
        logging.info(f"Dataframe is empty")


def query_avg_precipitation_acc(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        avg_precipitation = df.select(
            pl.col("precipitation_accumulated")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .mean()
            .alias("avg_precipitation")
        )

        value = avg_precipitation[0, "avg_precipitation"]

        return value
    else:
        logging.info(f"Dataframe is empty")


def query_avg_precipitation_rate(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        avg_precipitation = df.select(
            pl.col("precipitation_rate")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .mean()
            .alias("precipitation_rate")
        )

        value = avg_precipitation[0, "precipitation_rate"]

        return value
    else:
        logging.info(f"Dataframe is empty")


def query_avg_wind_speed(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        avg_precipitation = df.select(
            pl.col("wind_speed")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .mean()
            .alias("wind_speed")
        )

        value = avg_precipitation[0, "wind_speed"]

        return value
    else:
        logging.info(f"Dataframe is empty")


def query_avg_wind_gust(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        avg_precipitation = df.select(
            pl.col("wind_gust")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .mean()
            .alias("wind_gust")
        )

        value = avg_precipitation[0, "wind_gust"]

        return value
    else:
        logging.info(f"Dataframe is empty")


def query_avg_wind_direction(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        avg_precipitation = df.select(
            pl.col("wind_direction")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .mean()
            .alias("wind_direction")
        )

        value = avg_precipitation[0, "wind_direction"]

        return value
    else:
        logging.info(f"Dataframe is empty")


def query_avg_illuminance(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        avg_precipitation = df.select(
            pl.col("illuminance")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .mean()
            .alias("illuminance")
        )

        value = avg_precipitation[0, "illuminance"]

        return value
    else:
        logging.info(f"Dataframe is empty")


def query_avg_solar_irradiance(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        avg_precipitation = df.select(
            pl.col("solar_irradiance")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .mean()
            .alias("solar_irradiance")
        )

        value = avg_precipitation[0, "solar_irradiance"]

        return value
    else:
        logging.info(f"Dataframe is empty")


def query_avg_fo_uv(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        avg_precipitation = df.select(
            pl.col("fo_uv")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .mean()
            .alias("fo_uv")
        )

        value = avg_precipitation[0, "fo_uv"]

        return value
    else:
        logging.info(f"Dataframe is empty")


def query_avg_uv_index(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        avg_precipitation = df.select(
            pl.col("uv_index")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .mean()
            .alias("uv_index")
        )

        value = avg_precipitation[0, "uv_index"]

        return value
    else:
        logging.info(f"Dataframe is empty")


def query_avg_pressure(df: pl.DataFrame, start: int, end: int) -> int:
    if df is not None:
        avg_precipitation = df.select(
            pl.col("uv_index")
            .where(
                (pl.col("timestamp") > start) &
                (pl.col("timestamp") < end)
            )
            .mean()
            .alias("uv_index")
        )

        value = avg_precipitation[0, "uv_index"]

        return value
    else:
        logging.info(f"Dataframe is empty")
