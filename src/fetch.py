"""Get Basin wxm publications & files to set up remote URLs for further queries."""

import subprocess
import logging
import json
import time
import requests

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


# Get publications for wxm2 namespace creator (note: must use legacy contract)
def get_basin_pubs_legacy(address: str) -> list[str]:
    logging.info(f"Getting publications for {address}...")
    try:
        # Run the basin `list` command (note: this is a workaround since wxm2
        # uses the old contract)
        command = [
            'cast', 'call', '0xd0ee658f1203302e35b9b9e3a73cb3472a2c2373',
            '--rpc-url', 'https://api.calibration.node.glif.io/rpc/v1',
            'pubsOfOwner(address)(string[])', address
        ]
        result = subprocess.run(
            command,
            capture_output=True, text=True, check=True
        )
        out = result.stdout
        if out:
            pubs = json.loads(out)
            return pubs
            # return ['wxm2.date_2023_10_15']
        else:
            logging.info(f"No publications found for address {address}")
            return []

    except subprocess.CalledProcessError as e:
        error_msg = f"Error getting basin publications for address {address}: {e.stderr}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)

    except json.JSONDecodeError as e:
        error_msg = f"JSON decoding error for address {address}: {str(e)}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)


# Get publications for wxm2 namespace creator (note: this is the new contract,
# so it's not used in the current wxm use case)
def get_basin_pubs(address: str) -> list[str]:
    logging.info(f"Getting publications for {address}...")
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
            pubs = json.loads(out)
            return pubs
        else:
            logging.info(f"No publications found for address {address}")
            return []

    except subprocess.CalledProcessError as e:
        error_msg = f"Error getting basin publications for address {address}: {e.stderr}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)

    except json.JSONDecodeError as e:
        error_msg = f"JSON decoding error for address {address}: {str(e)}"
        logging.error(error_msg)
        raise RuntimeError(error_msg)


# Get deals for each publication, also inserting the corresponding
# `namespace.publication` into the returned objects (used in forming URL path)
def get_basin_deals(pubs: list[str]) -> list[object]:
    logging.info(f"Getting deals for publications...")
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
                logging.info(f"No deals found for publication {pub}")

        except subprocess.CalledProcessError as e:
            error_msg = f"Error finding basin deal for publication {pub}: {e.stderr}"
            logging.error(error_msg)
            raise RuntimeError(error_msg)

        except json.JSONDecodeError as e:
            error_msg = f"JSON decoding error for publication {pub}: {str(e)}"
            logging.error(error_msg)
            raise RuntimeError(error_msg)

    return deals


# Form remote URLs for each deal—first get the filename via `dweb.link`, then
# use Web3.Storage gateway to crete a list of URLs like:
# `https://<cid>.ipfs.w3s.link/<bamespace>/<publication>/<file>`
def get_basin_urls(pubs: list[object], max_retries=10, retry_delay=2) -> list[str]:
    logging.info("Forming remote URLs for publication deals...")
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
                    raise requests.exceptions.HTTPError(
                        f"HTTP error: {response.status_code}")

            except requests.exceptions.HTTPError as e:
                if response.status_code == 500:
                    attempts += 1
                    logging.error(
                        f"Error forming request URL for {cid}; attempt {attempts} of {max_retries}. Retrying...",)
                    time.sleep(retry_delay)
                else:
                    logging.error(f"HTTP error occurred: {e}")
                    break
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                break
        if attempts >= max_retries:
            raise RuntimeError(
                f"Failed to retrieve urls after {max_retries} attempts.")

    return urls
