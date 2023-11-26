"""Get Basin wxm publications & files to set up remote URLs for further queries."""

import json
import subprocess
import time
import requests

from utils import err, log_info, log_warn


# Get publications for a namespace creator at `address`
def get_basin_pubs(address: str) -> list[str]:
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


# Get deals for each publication, also inserting the corresponding
# `namespace.publication` into the returned objects (used in forming URL path)
def get_basin_deals(pubs: list[str]) -> list[object]:
    deals = []
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
                # Convert string output to json
                pub_deals = json.loads(out)
                for deal in pub_deals:
                    # Add publication to deal object
                    deal['publication'] = pub
                deals.extend(pub_deals)
            else:
                log_info(f"No deals found for publication {pub}")

        except subprocess.CalledProcessError as e:
            error_msg = f"Error finding basin deal for publication {pub}"
            err(error_msg, e, type(e))

        except json.JSONDecodeError as e:
            error_msg = f"JSON decoding error for publication {pub}"
            err(error_msg, e, type(e))

    return deals


# Form remote URLs for each deal—first get the filename via `dweb.link`, then
# use Web3.Storage gateway to crete a list of URLs like:
# `https://<cid>.ipfs.w3s.link/<namespace>/<publication>/<file>`
def get_basin_urls(pubs: list[object], max_retries=10, retry_delay=2) -> list[str]:
    base_url = "https://dweb.link/api/v0/"
    urls = []

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
