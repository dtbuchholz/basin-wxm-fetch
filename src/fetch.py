"""Get Basin wxm publications & files to set up remote URLs for further queries."""

import subprocess
import json
import time
import requests

from web3 import Web3
from utils import err, log_err, log_info


# Get publications for wxm2 namespace creator (note: must use legacy contract)
def get_basin_pubs_legacy(address: str) -> list[str]:
    try:
        # Get `pubsOfOwner` (note: this is a workaround since wxm2
        # uses the old contract); load abi
        with open('basin-abi-old.json') as abi_file:
            abi = abi_file.read()
        # Connect to contract & query for publications
        w3 = Web3(Web3.HTTPProvider(
            'https://api.calibration.node.glif.io/rpc/v1'))
        address = Web3.to_checksum_address(
            '0xd0ee658f1203302e35b9b9e3a73cb3472a2c2373')
        contract = w3.eth.contract(
            address=address, abi=abi)
        pubs = contract.functions.pubsOfOwner(
            "0x64251043A35ab5D11f04111B8BdF7C03BE9cF0e7").call()

        if pubs:
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


# Get publications for wxm2 namespace creator (note: this is the new contract,
# so it's not used in the current wxm use case)
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
            pubs = json.loads(out)
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
                    log_err(
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
