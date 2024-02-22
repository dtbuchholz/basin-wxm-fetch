"""Get wxm vaults & files to set up remote URLs for further queries."""

from json import JSONDecodeError, dump, load, loads
from os import walk
from pathlib import Path
from shutil import Error as ShError
from shutil import move
from subprocess import CalledProcessError, run
from tempfile import TemporaryDirectory
from typing import Dict, List

from requests import get
from requests.exceptions import RequestException

from .utils import err, log_info, log_warn


def get_vaults(address: str) -> List[str]:
    """
    Get all vaults for an address.

    Parameters
    ----------
        address (str): The address of the vault creator.

    Returns
    -------
        List[str]: The list of vaults for the vault.

    Raises
    ------
        Exception: If there is an error getting the vaults.
    """
    try:
        url = "https://basin.tableland.xyz/vaults"
        params = {"account": address}
        response = get(url, params=params)

        vaults = []
        if response.status_code == 200:
            vaults.extend(response.json())
        else:
            error_msg = f"Failed to fetch data: {response.status_code}"
            err(error_msg, Exception(error_msg), type(Exception))
    except RequestException as e:
        error_msg = f"Error getting vaults for address {address}"
        err(error_msg, e, type(e))

    except JSONDecodeError as e:
        error_msg = f"JSON decoding error for address {address}"
        err(error_msg, e, type(e))

    return vaults


def get_vault_events(vault: str, start: int | None, end: int | None) -> List[Dict]:
    """
    Get events for one or more vaults.

    Parameters
    ----------
        vaults (str): The vault.
        start (int): The start time for the events.
        end (int): The end time for the events.

    Returns
    -------
        List[Dict]: The list of events.

    Raises
    ------
        Exception: If there is an error getting the events.
    """
    if not vault:
        err(
            "No vaults provided",
            ValueError("Invalid input"),
            ValueError,
        )
    events = []
    # For each vault, run the `vaults` command to get an array of event
    # objects that contain the CID and other metadata
    try:
        url = f"https://basin.tableland.xyz/vaults/{vault}/events"
        params = {"after": start, "before": end, "limit": 50}
        response = get(url, params)

        if response.status_code == 200:
            vault_events = response.json()
            events.extend(vault_events)
        else:
            # Note: this won't throw. It's possible that events haven't been
            # made yet, so it's ideal to keep things going.
            log_warn(f"No events found for vault: {vault}")

    except RequestException as e:
        error_msg = f"Error finding event for vault {vault}"
        err(error_msg, e, type(e))

    except JSONDecodeError as e:
        error_msg = f"JSON decoding error for vault {vault}"
        err(error_msg, e, type(e))

    # Throw if no events exist for any vaults
    num_events = len(events)
    if num_events == 0:
        err(
            "No events found for any vaults",
            ValueError("Invalid input"),
            ValueError,
        )

    return events


def read_events_cache(cache_file: Path) -> List[Dict]:
    """
    Read the events cache file to get the list of events that have been downloaded.

    Parameters
    ----------
        cache_file (Path): The path to the cache file.

    Returns
    -------
        List[Dict]: The list of events.

    Raises
    ------
        Exception: If there is an error reading the cache file.
    """
    events = []
    try:
        if Path.exists(cache_file):
            with open(cache_file, "r") as f:
                json_data = f.read()
                cached_events = loads(json_data)
                events.extend(cached_events)
    except Exception as e:
        err("Error reading events cache file", e, type(e))

    return events


def check_events_cache(events: List[Dict], cache_file: Path) -> List[Dict]:
    """
    Check if any new events exist by comparing the list of events to the cache.

    Parameters
    ----------
        events (List[Dict]): The list of events.
        cache_file (Path): The path to the cache file.

    Returns
    -------
        List[Dict]: The list of events if new events exist, otherwise, an empty
            list with no events.

    Raises
    ------
        Exception: If there is an error checking the cache file.
    """
    new_events = []
    try:
        if not events:
            raise ValueError("No events provided")

        if not cache_file:
            raise ValueError("No cache file provided")

        if Path.exists(cache_file):
            cached_events = read_events_cache(cache_file)
            diff = [item for item in events if item not in cached_events]

            new_events.extend(diff)
        else:
            new_events.extend(events)
    except Exception as e:
        err("Error checking events cache file", e, type(e))

    return new_events


def write_events_cache(events: List[Dict], cache_file: Path) -> None:
    """
    Write the events cache file to store the list of events that have been
    downloaded.

    Parameters
    ----------
        events (List[Dict]): The list of events.
        cache_file (Path): The path to the cache file.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the cache file.
    """
    if not events:
        raise ValueError("No events provided")

    try:
        # Read existing cache if file exists
        if cache_file.exists():
            with open(cache_file, "r") as f:
                cached_events = load(f)
            cached_events.extend(events)
        else:
            cached_events = events

        # Write updated cache
        with open(cache_file, "w") as f:
            dump(cached_events, f, indent=2)
    except Exception as e:
        raise RuntimeError(f"Error writing events cache file: {e}") from e


def extract_events(events: List[Dict], data_dir: Path) -> None:
    """
    Retrieve Parquet or CAR files for each event & extract to `data_dir`.

    Parameters
    ----------
        events (List[Dict]): The list of events.
        data_dir (Path): The path to the directory for storing parquet files.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error extracting the events.
    """
    if not events:
        err(
            "No events provided",
            ValueError("Invalid input"),
            ValueError,
        )
    # Ensure the 'data' directory exists, or create it
    if not Path.exists(data_dir):
        err(
            "Data directory does not exist",
            ValueError("Invalid input"),
            ValueError,
        )
    # Retrieve Parquet and/or unpack CAR files for each event
    try:
        with TemporaryDirectory() as temp_dir_events:
            retrieve_events(events, temp_dir_events)

            # Process each file in the `temp_dir_events` and store in
            # `temp_dir_files`
            with TemporaryDirectory() as temp_dir_files:
                extract_parquet(temp_dir_events, temp_dir_files)

                # Move each parquet file to `data_dir`
                for temp_root, _, files in walk(temp_dir_files):
                    for file in files:
                        if file.endswith(".parquet"):
                            source_file = Path(temp_root) / file

                            # Move files to the data directory
                            move(str(source_file), str(data_dir))
    except ShError as e:
        if "already exists" in str(e):
            log_info("Data already extracted for event")
        else:
            err("Error extracting events", e, type(e))


def retrieve_events(events: List[Dict], dir: str) -> None:
    """
    Retrieve Parquet or CAR files for each event.

    Parameters
    ----------
        events (List[Dict]): The list of events.
        dir (str): The directory to store the CAR files.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error retrieving the events.
    """
    for event in events:
        cid = event.get("cid")
        if cid:
            try:
                command = ["vaults", "retrieve", cid]
                run(
                    command,
                    capture_output=True,
                    text=True,
                    check=True,
                    cwd=dir,
                )
            except CalledProcessError as e:
                err(f"Error extracting data for CID '{cid}'", e)
            except Exception as e:
                err(f"Unexpected error occurred for CID '{cid}'", e)
        else:
            err(
                "Event does not have a 'cid' key",
                ValueError("Invalid input"),
                ValueError,
            )


def extract_parquet(dir_events: str, dir_parquet: str) -> None:
    """
    Unpack a CAR to parquet file, or move an existing parquet file, for each
    event into a consolidated directory.

    Parameters
    ----------
        dir_events (str): The path to the downloaded event files.
        dir_files (str): The path to the directory for storing parquet files.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error extracting the parquet file.
    """
    for file_path in Path(dir_events).iterdir():
        if Path.is_file(file_path) and file_path.suffix == ".car":
            try:
                # Unpack CAR file to parquet file
                command = [
                    "ipfs-car",
                    "unpack",
                    str(file_path),
                    "--output",
                    str(Path(dir_parquet) / f"{file_path.stem}.parquet"),
                ]
                run(
                    command,
                    capture_output=True,
                    text=True,
                    check=True,
                    cwd=dir_parquet,
                )
            except CalledProcessError as e:
                err(f"Error extracting file {file_path}", e)
            except Exception as e:
                err(f"Unexpected error occurred for file {file_path}", e)
        elif Path.is_file(file_path) and file_path.suffix == ".parquet":
            try:
                # File is already parquet via cache
                move(str(file_path), str(dir_parquet))
            except Exception as e:
                err(f"Unexpected error occurred for file {file_path}", e)
