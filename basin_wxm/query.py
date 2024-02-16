from pathlib import Path
from typing import Tuple

from duckdb import DuckDBPyConnection, connect
from pandas import DataFrame as PdDataFrame
from polars import DataFrame as PlDataFrame

from .utils import err


def create_database(data_dir: Path) -> DuckDBPyConnection:
    """
    Create a DuckDB connection and load data from local parquet files.

    Parameters
    ----------
        data_dir (Path): Path to directory containing parquet files.

    Returns
    -------
        db (DuckDBPyConnection): DuckDB connection object.

    Raises
    ------
        Exception: If there is an error creating the DuckDB connection.
    """
    db: DuckDBPyConnection
    try:
        db = connect()
        files = Path(data_dir) / "*.parquet"  # Read all parquet files in data directory
        db.execute(f"CREATE VIEW wxm_data AS SELECT * FROM read_parquet('{files}');")
    except Exception as e:
        err("Error creating DuckDB connection", e)
    return db


def execute_queries(
    db: DuckDBPyConnection, start: int | None, end: int | None
) -> PlDataFrame:
    """
    Execute all queries using DuckDB and return the results as polars DataFrame.

    Parameters
    ----------
        db (DuckDBPyConnection): DuckDB connection object.
        start (int | None): Start timestamp for time filtering.
        end (int | None): End timestamp for time filtering.

    Returns
    -------
        result (DataFrame): The result of the query.

    Raises
    ------
        Exception: If there is an error executing the queries.
    """
    # Set up columns for average calculations
    columns = [
        "temperature",
        "humidity",
        "precipitation_accumulated",
        "wind_speed",
        "wind_gust",
        "wind_direction",
        "illuminance",
        "solar_irradiance",
        "fo_uv",
        "uv_index",
        "precipitation_rate",
        "pressure",
    ]

    # Set up all query parts
    avg_selection = [f"avg({col}) AS {col}" for col in columns]
    avg_calculations = ",".join(avg_selection)
    query_parts = [
        "SELECT min(timestamp) AS range_start, max(timestamp) AS range_end,",
        "COUNT(DISTINCT device_id) AS number_of_devices,",
        "mode(cell_id) AS cell_id_mode,",
        "sum(precipitation_accumulated) AS total_precipitation,",
        avg_calculations,
    ]

    # Add WHERE clause for time filtering, if applicable
    where_clause = ""
    if start is not None and end is not None:
        where_clause = f"WHERE timestamp >= {start} AND timestamp <= {end}"
    elif start is not None:
        where_clause = f"WHERE timestamp >= {start}"
    elif end is not None:
        where_clause = f"WHERE timestamp <= {end}"

    # Combine all parts into one query and execute
    query = " ".join(query_parts) + " FROM wxm_data" + f" {where_clause}"
    result: PlDataFrame
    try:
        result = db.execute(query).pl()  # Create a polars DataFrame
    except Exception as e:
        err("Error executing DuckDB aggregate queries", e)
    return result


def query_bbox(
    db: DuckDBPyConnection,
    bbox: Tuple[int, int, int, int],
    start: int | None,
    end: int | None,
) -> PdDataFrame:
    """
    Execute a query using DuckDB and return the results as pandas DataFrame.

    Parameters
    ----------
        db (DuckDBPyConnection): DuckDB connection object.
        bbox (Tuple[int, int, int, int]): Bounding box coordinates.
        start (int | None): Start timestamp for time filtering.
        end (int | None): End timestamp for time filtering.

    Returns
    -------
        result (DataFrame): The result of the query.

    Raises
    ------
        Exception: If there is an error executing the query.
    """
    # DuckDB query to select and aggregate data within the bounding box
    query = f"""
    SELECT cell_id, SUM(precipitation_accumulated) as total_precipitation, AVG(lat) as lat, AVG(lon) as lon
    FROM wxm_data
    WHERE lat BETWEEN {bbox[0]} AND {bbox[1]} AND lon BETWEEN {bbox[2]} AND {bbox[3]}
    """

    # Add WHERE clause for time filtering, if applicable
    where_clause = ""
    if start is not None and end is not None:
        where_clause = f"AND timestamp >= {start} AND timestamp <= {end}"
    elif start is not None:
        where_clause = f"AND timestamp >= {start}"
    elif end is not None:
        where_clause = f"AND timestamp <= {end}"

    # Combine all parts into one query and execute
    query = query + f" {where_clause}" + " GROUP BY cell_id"
    try:
        result = db.execute(query).df()  # Create a pandas PdDataframe
        return result
    except Exception as e:
        err("Error executing DuckDB bbox query", e)


def query_all_limit_n_duckdb(db: DuckDBPyConnection, n: int) -> PlDataFrame:
    """
    Returns the first 'n' rows of the provided table. For testing purposes.

    Parameters
    ----------
        db (DuckDBPyConnection): DuckDB connection object.
        n (int): The number of rows to return.

    Returns
    -------
        result (DataFrame): The result of the query.

    Raises
    ------
        Exception: If there is an error executing the query.
    """
    result: PlDataFrame
    try:
        query = f"SELECT * FROM wxm_data LIMIT {n}"
        result = db.execute(query).pl()  # Create a polars DataFrame
    except Exception as e:
        err("Error querying first 'n' rows in DuckDB", e)
    return result
