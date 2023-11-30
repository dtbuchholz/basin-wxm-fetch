"""Get Basin wxm publications & files to set up remote URLs for further queries."""

from json import JSONDecodeError, dump, load, loads
from os import walk
from pathlib import Path
from shutil import move
from subprocess import CalledProcessError, run
from tempfile import TemporaryDirectory

from utils import err, log_info, log_warn


def get_basin_pubs(address: str) -> list[str]:
    """
    Get all publications for a namespace creator at `address`. For example, the
    creator of `xm_data.p1` is `0xfc7C55c4A9e30A4e23f0e48bd5C1e4a865dA06C5`.

    Parameters
    ----------
        address (str): The address of the namespace creator.

    Returns
    -------
        list[str]: The list of publications for the namespace.

    Raises
    ------
        Exception: If there is an error getting the publications.
    """
    try:
        command = ["basin", "publication", "list", "--address", address]
        result = run(command, capture_output=True, text=True, check=True)
        out = result.stdout
        if out:
            pubs = out.strip().split("\n")
            return pubs
        else:
            err(
                f"No publications found for address {address}",
                ValueError("Invalid input"),
                ValueError,
            )

    except CalledProcessError as e:
        error_msg = f"Error getting basin publications for address {address}"
        err(error_msg, e, type(e))

    except JSONDecodeError as e:
        error_msg = f"JSON decoding error for address {address}"
        err(error_msg, e, type(e))


def get_basin_deals(pubs: list[str]) -> list[object]:
    """
    Get deals for one or more publications.

    Parameters
    ----------
        pubs (list[str]): The list of publications.

    Returns
    -------
        list[object]: The list of deals.

    Raises
    ------
        Exception: If there is an error getting the deals.
    """
    if not pubs:
        err(
            "No publications provided",
            ValueError("Invalid input"),
            ValueError,
        )
    deals = []
    # For each publication, run the `basin` command to get an array of deal
    # objects that contain the CID and other metadata
    for pub in pubs:
        try:
            command = [
                "basin",
                "publication",
                "deals",
                "--publication",
                pub,
                "--format",
                "json",
            ]
            result = run(command, capture_output=True, text=True, check=True)
            out = result.stdout
            if out:
                pub_deals = loads(out)
                deals.extend(pub_deals)
            else:
                # Note: this won't throw. It's possible that deals haven't been
                # made yet, so it's ideal to keep things going.
                log_warn(f"No deals found for publication: {pub}")

        except CalledProcessError as e:
            error_msg = f"Error finding basin deal for publication {pub}"
            err(error_msg, e, type(e))

        except JSONDecodeError as e:
            error_msg = f"JSON decoding error for publication {pub}"
            err(error_msg, e, type(e))

    # Throw if no deals exist for any publications
    num_deals = len(deals)
    if num_deals == 0:
        err(
            "No deals found for any publications",
            ValueError("Invalid input"),
            ValueError,
        )

    return deals


def read_deals_cache(cache_file: Path) -> list[dict]:
    """
    Read the deals cache file to get the list of deals that have been downloaded.

    Parameters
    ----------
        cache_file (Path): The path to the cache file.

    Returns
    -------
        list[dict]: The list of deals.

    Raises
    ------
        Exception: If there is an error reading the cache file.
    """
    try:
        if Path.exists(cache_file):
            with open(cache_file, "r") as f:
                json_data = f.read()
                deals = loads(json_data)
                return deals
        else:
            return []
    except Exception as e:
        err("Error reading deals cache file", e, type(e))


def check_deals_cache(deals: list[object], cache_file: Path) -> list[object]:
    """
    Check if any new deals exist by comparing the list of deals to the cache.

    Parameters
    ----------
        deals (list[dict]): The list of deals.
        cache_file (Path): The path to the cache file.

    Returns
    -------
        list[dict]: The list of deals if new deals exist, otherwise, an empty
            list with no deals.

    Raises
    ------
        Exception: If there is an error checking the cache file.
    """
    try:
        if not deals:
            raise ValueError("No deals provided")

        if not cache_file:
            raise ValueError("No cache file provided")

        if Path.exists(cache_file):
            cached_deals = read_deals_cache(cache_file)
            diff = [item for item in deals if item not in cached_deals]
            log_info(f"Number of new deals: {len(diff)}")

            return diff
        else:
            return deals
    except Exception as e:
        err("Error checking deals cache file", e, type(e))


def write_deals_cache(deals: list[dict], cache_file: Path) -> None:
    """
    Write the deals cache file to store the list of deals that have been
    downloaded.

    Parameters
    ----------
        deals (list[dict]): The list of deals.
        cache_file (Path): The path to the cache file.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error writing the cache file.
    """
    if not deals:
        raise ValueError("No deals provided")

    try:
        # Read existing cache if file exists
        if cache_file.exists():
            with open(cache_file, "r") as f:
                cached_deals = load(f)
            cached_deals.extend(deals)
        else:
            cached_deals = deals

        # Write updated cache
        with open(cache_file, "w") as f:
            dump(cached_deals, f, indent=2)
    except Exception as e:
        raise RuntimeError(f"Error writing deals cache file: {e}") from e


def extract_deals(deals: list[dict], data_dir: Path) -> None:
    """
    Retrieve CAR files for each deal & extract the parquet files to `data_dir`.

    Parameters
    ----------
        deals (list[dict]): The list of deals.
        data_dir (Path): The path to the directory for storing parquet files.

    Returns
    -------
        None

    Raises
    ------
        Exception: If there is an error extracting the deals.
    """
    if not deals:
        err(
            "No deals provided",
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
    # Retrieve CAR files for each deal
    try:
        with TemporaryDirectory() as temp_dir_car:
            for deal in deals:
                cid = deal.get("cid")
                if cid:
                    try:
                        command = ["basin", "publication", "retrieve", cid]
                        run(
                            command,
                            capture_output=True,
                            text=True,
                            check=True,
                            cwd=temp_dir_car,
                        )
                    except CalledProcessError as e:
                        err(f"Error extracting data for CID '{cid}'", e)
                    except Exception as e:
                        err(f"Unexpected error occurred for CID '{cid}'", e)
                else:
                    err(
                        f"Deal does not have a 'cid' key",
                        ValueError("Invalid input"),
                        ValueError,
                    )

            # Process each file in the `temp_dir_car` and store in
            # `temp_dir_files`
            with TemporaryDirectory() as temp_dir_files:
                for file_path in Path(temp_dir_car).iterdir():
                    if Path.is_file(file_path):
                        try:
                            # Change working directory to 'data' directory
                            command = ["car", "extract", "--file", file_path]
                            run(
                                command,
                                capture_output=True,
                                text=True,
                                check=True,
                                cwd=temp_dir_files,
                            )
                            # Change back to the original working directory
                        except CalledProcessError as e:
                            err(f"Error extracting file {file_path}", e)
                        except Exception as e:
                            err(f"Unexpected error occurred for file {file_path}", e)
                    else:
                        err(
                            f"Path is not a file: {file_path}",
                            ValueError("Invalid input"),
                            ValueError,
                        )

                # Avoid nested file structure and move each parquet file to `data_dir`
                for temp_root, _, files in walk(temp_dir_files):
                    for file in files:
                        if file.endswith(".parquet"):
                            source_file = Path(temp_root) / file
                            target_file = data_dir / file

                            # Move files to the data directory
                            move(str(source_file), str(target_file))

    except Exception as e:
        err("Error extracting deals", e, type(e))
