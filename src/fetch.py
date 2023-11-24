"""Fetch Basin wxm data."""

import subprocess
import logging
import json
import requests

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


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
        logging.error(
            f"Error getting basin publications for address {address}: {e.stderr}")
        return []  # Return an empty list in case of an error

    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding error for address {address}: {str(e)}")
        return []  # Return an empty list in case of an error

    except Exception as e:
        logging.error(
            f"Unexpected error occurred for address {address}: {str(e)}")
        return []  # Return an empty list in case of an error


# Note: existing wxm data is on the old contract, so this returns nothing;
# placeholder method for future use
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
        logging.error(
            f"Error getting basin publications for address {address}: {e.stderr}")
        return []  # Return an empty list in case of an error

    except json.JSONDecodeError as e:
        logging.error(f"JSON decoding error for address {address}: {str(e)}")
        return []  # Return an empty list in case of an error

    except Exception as e:
        logging.error(
            f"Unexpected error occurred for address {address}: {str(e)}")
        return []  # Return an empty list in case of an error


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
            logging.error(
                f"Error finding basin deal for publication {pub}: {e.stderr}")
        except json.JSONDecodeError as e:
            logging.error(
                f"JSON decoding error for publication {pub}: {str(e)}")
        except Exception as e:
            logging.error(
                f"Unexpected error occurred for publication {pub}: {str(e)}")

    return deals


def get_basin_links(pubs: list[object]) -> list[str]:
    logging.info(f"Forming remote links for publication deals...")
    base_url = f"https://dweb.link/api/v0/"
    links = []
    for pub in pubs:
        cid = pub['cid']
        formatted_path = pub['publication'].replace('.', '/')
        list_url = f"{base_url}ls?arg={cid}/{formatted_path}/"
        response = requests.get(list_url)
        # Check if the request was successful
        if response.status_code == 200:
            try:
                # Parse the response content as JSON
                data = response.json()

                # Navigate through the JSON to find the file name
                file_name = data['Objects'][0]['Links'][0]['Name']
                get_url = f"https://{cid}.ipfs.w3s.link/{formatted_path}/{file_name}"
                links.append(get_url)

            except (KeyError, IndexError, TypeError) as e:
                logging.error(
                    f"Unexpected error parsing response data for publication {pub}: {str(e)}")
        else:
            logging.error(
                f"Failed to fetch data. Status code: {response.status_code}")
    return links
