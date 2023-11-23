"""Fetch Basin wxm data."""

import subprocess
import logging
import json
import tempfile
import os

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


def extract(deals: list[dict], data_dir: str) -> None:
    logging.info(f"Retrieving & extracting deals...")
    cwd = os.getcwd()
    # Ensure the 'data' directory exists
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    logging.info(f"  Retrieving deals from Basin...")
    with tempfile.TemporaryDirectory() as temp_dir:
        for deal in deals:
            cid = deal.get('cid')
            if cid:
                try:
                    command = ['basin', 'publication', 'retrieve', cid]
                    subprocess.run(
                        command,
                        check=True,
                        cwd=temp_dir
                    )
                except subprocess.CalledProcessError as e:
                    logging.error(
                        f"Error extracting basin data for CID {cid}: {e.stderr}")
                except Exception as e:
                    logging.error(
                        f"Unexpected error occurred for CID {cid}: {str(e)}")
            else:
                logging.error(f"Deal {deal} does not have a 'cid' key.")

        # Process each file in the temp_dir
        logging.info(f"  Extracting CAR files to '{data_dir}'...")
        for file_name in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, file_name)
            if os.path.isfile(file_path):
                try:
                    # Change working directory to 'data' directory
                    os.chdir(data_dir)
                    command = ['car', 'extract', '--file', file_path]
                    subprocess.run(
                        command,
                        stdout=subprocess.DEVNULL,
                        check=True
                    )
                    # Change back to the original working directory
                    os.chdir(cwd)
                except subprocess.CalledProcessError as e:
                    logging.error(
                        f"Error extracting file {file_path}: {e.stderr}")
                except Exception as e:
                    logging.error(
                        f"Unexpected error occurred for file {file_path}: {str(e)}")
                finally:
                    os.chdir(cwd)

        logging.info("Extraction completed.")
