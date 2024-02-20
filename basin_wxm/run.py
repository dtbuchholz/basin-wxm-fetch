"""Set up an in-memory database for remote files, execute queries, and write to CSV and markdown."""

from contextlib import redirect_stdout
from io import StringIO
from math import ceil
from pathlib import Path
from textwrap import dedent
from typing import List, Tuple

import contextily as ctx
import geopandas as gpd
import matplotlib.cm as cm
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from duckdb import DuckDBPyConnection
from matplotlib.colors import TwoSlopeNorm
from polars import Config, DataFrame, Date, Series
from polars import col as pl_col
from polars import concat, read_csv

from .config import get_vaults_config
from .fetch import (
    check_events_cache,
    extract_events,
    get_vault_events,
    write_events_cache,
)
from .query import create_database, execute_queries, query_bbox
from .utils import (
    err,
    format_unix_ms,
    get_current_date,
    log_info,
    to_title_case,
    unix_to_ms,
    wrap_task,
)


def prepare_data(
    root: Path, start: int | None, end: int | None
) -> DuckDBPyConnection | None:
    """
    Prepare an in-memory database for querying by first getting data for
    vaults, events, CAR files, and extracted parquet files.

    Parameters
    ----------
        root (Path): The root directory of the project.
        start (int): The start of the query time range (can be None).
        end (int): The end of the query time range (can be None).

    Returns
    -------
        DuckDBPyConnection | None: The in-memory database connection, or None if
            there are no new events to process.


    Raises
    ------
        Exception: If there is an error getting the vaults, events, or
            remote parquet files.
    """
    # Get vaults for wxm via static config file
    vaults_config = get_vaults_config()
    # Use the weather data vault
    vault = vaults_config["weather_data"]

    # Get events for each vault
    events = wrap_task(
        lambda: get_vault_events(vault, start, end), "Getting events for vault..."
    )
    num_events = len(events)
    log_info(f"Number of events found: {num_events}")

    # Check if there is a cache file; if so, read it and filter out events that
    # have already been downloaded
    cache_file = root / "cache.json"
    new_events = check_events_cache(events, cache_file)
    # Exit early if no new events exist
    log_info(f"Number of new events: {len(new_events)}")
    if len(new_events) == 0:
        log_info("No new events found, exiting...")
        return None

    # Write the updated events to the cache file
    write_events_cache(new_events, cache_file)

    # Set up the directory for the extracted parquet files
    data_dir = root / "inputs"
    if not Path.exists(data_dir):
        Path.mkdir(data_dir)
    else:
        # Clear the directory if it already exists
        for file in data_dir.iterdir():
            file.unlink()
    # Retrieve events & extract the parquet files from each CAR file
    wrap_task(
        lambda: extract_events(new_events, data_dir), "Extracting data from events..."
    )
    # Create a in-memory database from the parquet files
    db = wrap_task(
        lambda: create_database(data_dir),
        "Creating database with parquet files...",
    )

    return db


def run(db: DuckDBPyConnection, root: Path, start: int | None, end: int | None) -> None:
    """
    Execute queries and write results to files.

    Parameters
    ----------
        db (DuckDBPyConnection): The database to query.
        start (int): The start of the query time range (can be None).
        end (int): The end of the query time range (can be None).

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error executing the queries or writing the
            results.
    """
    # Execute queries and get the results as a polars DataFrame
    # Also, set `start` and `end` time dynamically, if None
    # But first, convert to unix ms (the wxm dataset uses this)
    if start is not None:
        start = unix_to_ms(start)
    if end is not None:
        end = unix_to_ms(end)
    df = wrap_task(lambda: execute_queries(db, start, end), "Executing queries...")
    # Generate plots for bbox across the database and write to files
    wrap_task(
        lambda: execute_bbox_plots(db, root, start, end),
        "Generating bbox plots...",
    )
    # Prepare the data and write to files
    wrap_task(
        lambda: write_files(df, root),
        "Writing results to files...",
    )


def execute_bbox_plots(
    db: DuckDBPyConnection, root: Path, start: int | None, end: int | None
) -> None:
    """
    Execute queries for each bbox and generate plots for each.

    Parameters
    ----------
        db (DuckDBPyConnection): The database to query.
        root (Path): The root directory for the program.
        start (int): The start of the query time range.
        end (int): The end of the query time range.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error executing the queries or writing the
            results.
    """
    try:
        bboxes = {
            "north_america": (14, 72, -172, -52),
            "south_america": (-55, 12, -85, -34),
            "europe": (35, 72, -13, 60),
            "africa": (-35, 38, -18, 55),
            "asia": (-11, 81, 25, 179),
            "australia": (-48, -6, 108, 178),
        }
        for region_name, bbox in bboxes.items():
            write_plot_precipitation_by_bbox(db, root, start, end, bbox, region_name)
    except Exception as e:
        err("Error in execute_bbox_plots", e)


def write_plot_precipitation_by_bbox(
    db: DuckDBPyConnection,
    root: Path,
    start: int | None,
    end: int | None,
    bbox: Tuple[int, int, int, int],
    region_name: str,
) -> None:
    """
    Write a plot of precipitation accumulated by cell ID for a given bbox.

    Parameters
    ----------
        db (DuckDBPyConnection): The database to query.
        root (Path): The root directory for the program.
        start (int): The start of the query time range.
        end (int): The end of the query time range.
        bbox (Tuple[int, int, int, int]): The bbox to query.
        region_name (str): The name of the region for the plot.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the plot.
    """
    try:
        df = query_bbox(db, bbox, start, end)
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["lon"], df["lat"]))
        gdf.crs = "EPSG:4326"  # Set the coordinate reference system to WGS84
        gdf = gdf.to_crs(epsg=3857)  # Re-project to Web Mercator

        # Normalize the data for color mapping
        vmin = gdf["total_precipitation"].min()
        vmax = gdf["total_precipitation"].max()
        norm = TwoSlopeNorm(vmin=vmin, vcenter=(vmin + vmax) / 2, vmax=vmax)

        # Create the figure and axes
        fig, ax = plt.subplots(figsize=(15, 10))
        gdf.plot(
            ax=ax,
            column="total_precipitation",
            cmap="viridis",
            alpha=0.6,
            edgecolor="k",
            markersize=5,
            norm=norm,
        )

        # Add a basemap
        ctx.add_basemap(ax, source=ctx.providers.CartoDB.PositronNoLabels)

        # Set axis off
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.set_xticklabels([])
        ax.set_yticklabels([])
        ax.set_title(
            f"Precipitation Accumulated by Cell ID for {to_title_case(region_name)}"
        )

        # Add a color bar
        sm = cm.ScalarMappable(cmap="viridis", norm=norm)
        sm.set_array([])  # Empty array for the data range
        cb = fig.colorbar(sm, ax=ax, fraction=0.03, pad=0.04)
        cb.set_label("Total Precipitation")

        # Save the plot; ensure the directory exists
        maps_dir = Path(root) / "assets" / "maps"
        maps_dir.mkdir(parents=True, exist_ok=True)
        file_name = maps_dir / f"precipitation_map_{region_name}.png"
        plt.savefig(file_name, bbox_inches="tight", pad_inches=0)
        plt.close(fig)  # Close the plot to free memory
    except Exception as e:
        err("Error in write_plot_precipitation_by_bbox", e)


def write_files(df: DataFrame, root: Path) -> None:
    """
    Write the run's dataframe results to a csv file for history and markdown
    for current state.

    Parameters
    -------
        df (DataFrame): The run's polars DataFrame results.
        start (int): The start of the query time range.
        end (int): The end of the query time range.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the results.
    """
    try:
        prepared = prepare_output(df)
        # Write the history CSV file and plots to `assets` directory
        write_history_csv(prepared, root)
        # Exclude certain columns for the plots
        exclude_cols = ["job_date", "cell_id_mode", "range_start", "range_end"]
        write_history_plots(root, exclude_cols)
        write_markdown(prepared, root, exclude_cols)
    except Exception as e:
        err("Error in write_results", e)


def prepare_output(df: DataFrame) -> DataFrame:
    """
    Prepare the DataFrame with the run date and start/end query time range; used
    when writing to files.

    Parameters
    ----------
        df (DataFrame): The run's polars DataFrame results.
        start (int): The start of the query time range.
        end (int): The end of the query time range.

    Returns
    -------
        DataFrame: The full polars DataFrame with run info.
    """
    current_datetime = get_current_date()
    # Include run data qnd query ranges
    run_info = DataFrame([Series("job_date", [current_datetime])])
    df_with_run_info = concat([run_info, df], how="horizontal")

    return df_with_run_info


def write_history_csv(df: DataFrame, root: Path) -> None:
    """
    Append the run's DataFrame results to a csv file.

    Parameters
    ----------
        df (DataFrame): The run's polars DataFrame results.
        root (Path): The root directory for the program.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the results.
    """
    try:
        history_file = Path(root) / "history.csv"
        # Check if the file exists—if so, append to it; otherwise, create it
        if Path.exists(history_file):
            # Open the file in append mode and write without header
            with open(history_file, "a") as csv_file:
                df.write_csv(csv_file, include_header=False)
        else:
            # If the file doesn't exist, create it and write with header
            df.write_csv(history_file)
    except Exception as e:
        err("Error in write_history_csv", e)


def write_history_plots(root: Path, exclude_cols: List[str]) -> None:
    """
    Write plots for all columns in the history CSV file.

    Parameters
    ----------
        root (Path): The root directory for the program.
        exclude_cols (List[str]): The columns to exclude from the plots.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the plots.
    """
    try:
        file_path = Path(root) / "history.csv"
        df = read_csv(file_path)

        # Convert 'job_date' to a datetime type for plotting
        df = df.with_columns(pl_col("job_date").str.strptime(Date))

        # Plot all average and aggregate columns
        for col in df.columns:
            if col not in exclude_cols:
                plt.figure(figsize=(10, 6))
                plt.plot(df["job_date"], df[col], marker="o")
                plt.title(f"{to_title_case(col)} Over Time")
                plt.xlabel("Date")
                plt.ylabel(col)
                # Set date format on x-axis
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
                plt.gca().xaxis.set_major_locator(mdates.DayLocator())
                plt.gcf().autofmt_xdate()
                # Format y-axis to use fixed-point notation with one decimal
                ax = plt.gca()  # Get the current axis
                ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.2f"))
                plt.grid(True)

                # Ensure the directory exists
                averages_dir = Path(root) / "assets" / "averages"
                averages_dir.mkdir(parents=True, exist_ok=True)
                # Save the plot
                file_path = averages_dir / f"{col}.png"
                plt.savefig(file_path)
    except Exception as e:
        err("Error in write_history_plots", e)


def write_markdown(df: DataFrame, root: Path, exclude_cols: List[str]) -> None:
    """
    Overwrite the run's DataFrame results to a markdown file.

    Parameters
    ----------
        df (DataFrame): The run's polars DataFrame results.
        root (Path): The root directory for the program.
        exclude_cols (List[str]): The columns to exclude from the markdown table.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the results.
    """
    try:
        markdown_file = Path(root) / "Data.md"
        # Get job date and the start/end of the query range; needed before we
        # get the markdown table variation of the DataFrame
        job_date = df["job_date"][0]
        range_start = df["range_start"][0]
        range_end = df["range_end"][0]
        df_for_plot = df.drop(exclude_cols)  # Drop columns that aren't plotted
        # Get the DataFrame as markdown tables
        markdown_tables = convert_df_to_markdown(df)

        # Write the markdown table to the file with metadata
        with open(markdown_file, "w") as md_file:
            # Format time range
            start_formatted = format_unix_ms(range_start)
            end_formatted = format_unix_ms(range_end)
            # Write to the file
            md_file.write("# Data\n\n")
            md_file.write(
                f"_Generated on **{job_date}** for data in range **{start_formatted}** to **{end_formatted}**._\n"
            )
            md_content = dedent(
                """
                The schema for the raw data is as follows:

                - `device_id` (varchar): Unique identifier for the device.
                - `timestamp` (bigint): Timestamp (unix milliseconds).
                - `temperature` (double): Temperature (Celsius).
                - `humidity` (double): Relative humidity reading (%).
                - `precipitation_accumulated` (double): Total precipitation (millimeters).
                - `wind_speed` (double): Wind speed (meters per second).
                - `wind_gust` (double): Wind gust (meters per second).
                - `wind_direction` (double): Wind direction (degrees).
                - `illuminance` (double): Illuminance (lux).
                - `solar_irradiance` (double): Solar irradiance (watts per square meter).
                - `fo_uv` (double): UV-related index value.
                - `uv_index` (double): UV index.
                - `precipitation_rate` (double): Precipitation rate (millimeters per hour).
                - `pressure` (double): Pressure (HectoPascals).
                - `name` (varchar): Name of the device.
                - `utc_datetime` (varchar): Timestamp from the raw data in UTC.
                - `model` (varchar): Model of the device (either WXM WS1000 or WXM WS2000).
                - `cell_id` (varchar): Cell ID of the device.
                - `lat` (double): Latitude of the cell.
                - `lon` (double): Longitude of the cell.

                Most of the columns above are included in the mean calculations, and there are three additional columns for aggregates:

                - `total_precipitation` (double): Total `precipitation_accumulated` (millimeters).
                - `number_of_devices` (int): Count of unique `device_id` values.
                - `cell_id_mode` (varchar): Most common `cell_id` value.

                And three additional columns for run metadata:

                - `job_date` (varchar): Date the job was run.
                - `range_start` (bigint): Start of the query range (unix milliseconds).
                - `range_end` (bigint): End of the query range (unix milliseconds).\n
                """
            )
            md_file.write(md_content)
            md_file.write("## Averages & cumulative metrics\n\n")
            md_file.write(markdown_tables)
            # Write the precipitation accumulated maps to the markdown file
            maps_dir = root / "assets" / "maps"
            md_file.write("\n## Precipitation accumulated maps\n\n")
            for file_path in maps_dir.iterdir():
                # Check if the path is a file and it's a PNG image
                if file_path.is_file() and file_path.suffix == ".png":
                    # Extract the base file name without the suffix
                    base_name = file_path.stem
                    title = to_title_case(base_name.replace("precipitation_map_", ""))
                    image_path = f"./assets/maps/{file_path.name}"
                    md_file.write(f"### {title}\n\n")
                    md_file.write(f"![{title}]({image_path})\n\n")
            # Write historical CSV plots to the markdown file
            md_file.write("## Historical plots\n\n")
            for col in df_for_plot.columns:
                # Convert column name to lowercase for the filename
                file_name = col + ".png"
                image_path = f"./assets/averages/{file_name}"

                # Write the Markdown syntax for embedding the image
                md_file.write(f"### {to_title_case(col)}\n\n")
                md_file.write(f"![{to_title_case(col)}]({image_path})\n\n")
    except Exception as e:
        err("Error in write_markdown", e)
        raise


def convert_df_to_markdown(df: DataFrame) -> str:
    """
    Format a DataFrame to a markdown table, splitting into two tables of
    approximately equal widths.
    """
    # Replace underscores with spaces and capitalize column names
    df.columns = [to_title_case(col) for col in df.columns]
    # Split the DataFrame into two DataFrames
    split_at = ceil(df.width / 2)
    df1 = df.select(df.columns[:split_at])
    df2 = df.select(df.columns[split_at:])

    output = StringIO()
    with redirect_stdout(output):
        with Config(
            tbl_formatting="ASCII_MARKDOWN",
            tbl_hide_column_data_types=True,
            tbl_hide_dataframe_shape=True,
            set_tbl_width_chars=5000,  # Prevent line wrapping
            float_precision=3,  # Show 3 decimal points
            set_fmt_float="full",  # Show full float precision
            set_tbl_cols=-1,  # Show all columns
        ):
            print(df1)
            print()
            print(df2)
    return output.getvalue()
