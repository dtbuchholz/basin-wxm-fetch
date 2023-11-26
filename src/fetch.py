"""Get Basin wxm publications & files to set up remote URLs for further queries."""

import json
import subprocess
import time
import requests

from utils import err, log_warn


def get_basin_pubs(address: str) -> list[str]:
    """
    Get all publications for a namespace creator at `address`. For example, the
    creator of `xm_data.p1` is `0xfc7C55c4A9e30A4e23f0e48bd5C1e4a865dA06C5`.

    Args:
        address (str): The address of the namespace creator.

    Returns:
        list[str]: The list of publications for the namespace.
    """
    try:
        command = [
            'basin', 'publication', 'list', '--address', address
        ]
        result = subprocess.run(
            command,
            capture_output=True, text=True, check=True
        )
        out = result.stdout
        if out:
            pubs = out.strip().split('\n')
            return pubs
        else:
            err(f"No publications found for address {address}",
                ValueError("Invalid input"), ValueError)

    except subprocess.CalledProcessError as e:
        error_msg = f"Error getting basin publications for address {address}"
        err(error_msg, e, type(e))

    except json.JSONDecodeError as e:
        error_msg = f"JSON decoding error for address {address}"
        err(error_msg, e, type(e))


def get_basin_deals(pubs: list[str]) -> list[object]:
    """
    Get deals for one or more publications, also inserting the corresponding
    `namespace.publication` into the returned objects (used in forming URL
    path when requesting remote files).
    """
    deals = []
    # For each publication, run the `basin` command to get an array of deal
    # objects that contain the CID and other metadata
    for pub in pubs:
        try:
            command = [
                'basin', 'publication', 'deals',
                '--publication', pub, '--format', 'json'
            ]
            result = subprocess.run(
                command,
                capture_output=True, text=True, check=True
            )
            out = result.stdout
            if out:
                pub_deals = json.loads(out)
                for deal in pub_deals:
                    # Add `namespace.publication` as `publication` to deal
                    # object; helps with forming URL path when requesting
                    deal['publication'] = pub
                deals.extend(pub_deals)
            else:
                # Note: this won't throw. It's possible that deals haven't been
                # made yet, so it's ideal to keep things going.
                log_warn(f"No deals found for publication {pub}")

        except subprocess.CalledProcessError as e:
            error_msg = f"Error finding basin deal for publication {pub}"
            err(error_msg, e, type(e))

        except json.JSONDecodeError as e:
            error_msg = f"JSON decoding error for publication {pub}"
            err(error_msg, e, type(e))

    # Throw if no deals exist for any publications
    if (len(deals) == 0):
        err("No deals found for any publications",
            ValueError("Invalid input"), ValueError)

    return deals


def get_basin_urls(pubs: list[object], max_retries=10, retry_delay=2) -> list[str]:
    """
    Form remote request URLs for each deal. This is needed in order to get the
    parquet filename (via `dweb.link`) and then form the full URL for the as:
    `https://<cid>.ipfs.w3s.link/<namespace>/<publication>/<file>`

    Args:
        deals (list[object]): The list of deals.
        max_retries (int): he maximum number of times to retry a failed
        request. Defaults to 10.
        retry_delay (int): he number of seconds to wait between retries.
        Defaults to 2.

    Returns:
        list[str]: The list of remote URLs.
    """
    # Use dweb.link to quickly get the filename for each deal where the path
    # includes the CID, namespace, and publication
    base_url = "https://dweb.link/api/v0/"
    urls = []

    # For each publication, get the parquet filenames and create a w3s URL
    for pub in pubs:
        cid = pub['cid']
        formatted_path = pub['publication'].replace('.', '/')
        list_url = f"{base_url}ls?arg={cid}/{formatted_path}/"

        attempts = 0
        while attempts < max_retries:
            try:
                response = requests.get(list_url)

                # Check if the request was successful
                if response.status_code == 200:
                    data = response.json()
                    file_name = data['Objects'][0]['Links'][0]['Name']
                    get_url = f"https://{cid}.ipfs.w3s.link/{formatted_path}/{file_name}"
                    urls.append(get_url)
                    break  # Break out of the retry loop on success
                else:
                    error_msg = "HTTP request error"
                    err(error_msg, response.status_code,
                        requests.exceptions.HTTPError)

            except requests.exceptions.HTTPError as e:
                # Sometimes, 500 errors will occur and requires retry logic
                if response.status_code == 500:
                    attempts += 1
                    log_warn(
                        f"Error forming request url for {cid}. Retrying (attempt {attempts} of {max_retries})...",)
                    time.sleep(retry_delay)
                else:
                    err("HTTP error occurred", e, type(e))
            except Exception as e:
                err("Unexpected error getting urls", e)
        if attempts >= max_retries:
            error_msg = f"Failed to retrieve urls after {max_retries} attempts."
            err(error_msg, e)

    return urls
