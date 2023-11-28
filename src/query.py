import duckdb
import os
from utils import err, log_warn


def create_duckdb_connection(remote_files: list[str]):
    """
    Create a DuckDB connection and load data from remote Parquet files.
    """
    remote_files = os.path.join("../xm_data/p1/*.parquet")
    try:
        con = duckdb.connect()
        con.execute(
            f"CREATE VIEW xm_data AS SELECT * FROM read_parquet('{remote_files}');"
        )
        return con
    except Exception as e:
        err("Error creating DuckDB connection", e)


def execute_queries(con, start: int | None, end: int | None):
    """
    Execute all queries using DuckDB and return the results as a DataFrame.
    """
    # Add average calculations
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
    avg_selection = [f"avg({col}) AS {col}" for col in columns]
    avg_calculations = ",".join(avg_selection)
    query_parts = [
        "SELECT min(timestamp) AS range_start, max(timestamp) AS range_end,",
        "COUNT(DISTINCT device_id) AS number_of_devices,",
        "mode(cell_id) AS cell_id_mode,",
        "sum(precipitation_accumulated) AS total_precipitation,",
        avg_calculations,
    ]

    # Add WHERE clause for time filtering
    where_clause = ""
    if start is not None and end is not None:
        where_clause = f"WHERE timestamp > {start} AND timestamp < {end}"
    elif start is not None:
        where_clause = f"WHERE timestamp > {start}"
    elif end is not None:
        where_clause = f"WHERE timestamp < {end}"

    # Combine all parts into one query
    query = " ".join(query_parts) + " " + where_clause + " FROM xm_data"

    try:
        result = con.execute(query).pl()
        return result
    except Exception as e:
        err("Error executing DuckDB queries", e)


def query_all_limit_n_duckdb(con, n: int):
    """
    Returns the first 'n' rows of the provided table.
    """
    try:
        query = f"SELECT * FROM xm_data LIMIT {n}"
        result = con.execute(query).pl()
        return result
    except Exception as e:
        err("Error querying first 'n' rows in DuckDB", e)
