import argparse
import json
import logging
import requests
import time
from pathlib import Path
import repair_tools.prsv_api as prsvapi

# parent ref to search within (INGEST folder), can be changed
PARENT_HIERARCHY = "380c_d78-0a8a-4843-b472-2199ba7fad72" # INGEST folder
# PARENT_HIERARCHY = "183a74b5-7247-4fb2-8184-959366bc0cbc" # DigAMI folder
# PARENT_HIERARCHY = "e80315bc-42f5-44da-807f-446f78621c08" # DigArch folder

PRESERVICA_API_URL = "https://nypl.preservica.com/api"

DELETION_LIST = [line for line in Path("/Users/emileebuytkins/Documents/Buytkins_Programming/reingest.txt").read_text().splitlines() if line.strip()]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--credentials",
        type=str,
        required=True,
        choices=["test-ingest", "prod-ingest", "test-manage"],
        help="which set of credentials to use",
        )
    parser.add_argument(
        "--pkg_title",
        "-p",
        nargs='+',
        help="One or more titles of packages to find and move."
    )
    parser.add_argument(
        "--new-parent-ref",
        "-npf",
        required=True,
        help="The parentref of the new folder."
    )
    parser.add_argument(
        "--parent",
        type=str,
        required=True,
        choices=["ingest", "digami", "digarch"],
        help="The parentref of the new folder. Options: 'ingest', 'digami', 'digarch'"
    )
    return parser.parse_args()

def get_pkg_uuid(accesstoken: str, pkg_title: str, initial_parent: str, new_parent: str) -> str | None:

    def perform_search(parent_uuid: str, current_token: str) -> requests.Response | str | None:
        """
        Performs the search request with a retry mechanism.
        Returns a response object on success, None on failure,
        or the string "REAUTH" if a 401 error occurs.
        """
        query_params = {"q": "", "fields": [{"name": "xip.title", "values": [pkg_title]}]}
        query_str = json.dumps(query_params)
        search_url = (
            f"{PRESERVICA_API_URL}/content/search-within"
            f"?q={requests.utils.quote(query_str)}"
            f"&parenthierarchy={parent_uuid}"
            "&start=0&max=10&metadata=id"
        )
        headers = {"Preservica-Access-Token": current_token, "accept": "application/json"}

        for attempt in range(3):
            try:
                response = requests.get(search_url, headers=headers)
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                # check for token related errors
                if e.response.status_code == 401:
                    logging.error(f"Authorization failed (401): Token is expired. Signaling for re-authentication.")
                    return "REAUTH" #signals to refresh token
                # HTTP errors other than 401
                logging.warning(f"HTTP Error on attempt {attempt + 1}/3: {e}.")
            except requests.exceptions.RequestException as e:
                # conn errors, timeouts, etc.
                logging.warning(f"Connection Error on attempt {attempt + 1}/3: {e}.")

            if attempt < 2:
                logging.info("Retrying in 15 seconds...")
                time.sleep(15)

        logging.error(f"API request failed after 3 attempts for '{pkg_title}' in parent '{parent_uuid}'.")
        return None

    # check parent ref
    initial_response = perform_search(initial_parent, accesstoken)
    if initial_response == "REAUTH":
        return "REAUTH" 
    if initial_response:
        try:
            data = initial_response.json()
            if data.get("success") and data["value"]["totalHits"] > 0:
                metadata = data.get("value", {}).get("metadata", [])
                if metadata and metadata[0]:
                    return metadata[0][0].get("value")
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logging.error(f"Failed to parse initial search response for '{pkg_title}': {e}")


    # check deletion folder
    logging.warning(f"Package '{pkg_title}' not in initial folder. Checking deletion folder:")
    new_parent_response = perform_search(new_parent, accesstoken)
    if new_parent_response == "REAUTH":
        return "REAUTH" # pass token retry signal up to refresh
    if new_parent_response:
        try:
            data = new_parent_response.json()
            if data.get("success") and data["value"]["totalHits"] > 0:
                logging.info(f"Package '{pkg_title}' already exists in the deletion folder.")
                return True 
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logging.error(f"Failed to parse second search response for '{pkg_title}': {e}")


    # not in either
    logging.warning(f"Package '{pkg_title}' not found in either location.")
    return None 

def set_new_parent_ref(accesstoken: str, pkg_uuid: str, new_parent_uuid: str) -> bool:
    """Moves pkg to new parentref"""
    move_url = f"{PRESERVICA_API_URL}/entity/structural-objects/{pkg_uuid}/parent-ref"
    
    headers = {
        "Preservica-Access-Token": accesstoken,
        "Content-Type": "text/plain",
        "accept": "text/plain;charset=UTF-8"
    }

    try:
        response = requests.put(move_url, headers=headers, data=new_parent_uuid)

        if response.status_code == 202:
            return True
        else:
            logging.error(f"FAILED to move. Status: {response.status_code}, Response: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed during move: {e}")
        return False


def main():
    global PARENT_HIERARCHY
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    if "ingest" in args.parent:
        PARENT_HIERARCHY = "380c6d78-0a8a-4843-b472-2199ba7fad72" # INGEST folder
    elif "digami" in args.parent:
        PARENT_HIERARCHY = "183a74b5-7247-4fb2-8184-959366bc0cbc" # DigAMI folder
    elif "digarch" in args.parent:
        PARENT_HIERARCHY = "e80315bc-42f5-44da-807f-446f78621c08" # DigArch folder
    else:
        print("Define parent folder to search in")
        return

    accesstoken = prsvapi.get_token(args.credentials)

    failed_moves = set()
    successful_moves = set()
    deletion_exists = set()

    # set -> list conversion to avoid miscounts in summary (gets rid of duplicates)
    pkg_set = set(args.pkg_title if args.pkg_title else DELETION_LIST)
    pkg_list = list(pkg_set)
    
    i = 0
    while i < len(pkg_list):
        pkg_title = pkg_list[i]
        try:
            print(f"\n--- Processing package: {pkg_title} ---")
            
            pkg_uuid = get_pkg_uuid(accesstoken, pkg_title, PARENT_HIERARCHY, args.new_parent_ref)

            if pkg_uuid == "REAUTH":
                print("Accesstoken expired, attempting to refresh.")
                accesstoken = prsvapi.get_token(args.credentials)
                print("Token refreshed. Retrying last package.")
                continue 

            if not pkg_uuid:
                print(f"Move FAILED: Could not find package {pkg_title}, skipping.")
                failed_moves.add(pkg_title)
            elif pkg_uuid is True:
                print(f"Move SKIPPED: Package {pkg_title} already exists in the destination folder.")
                deletion_exists.add(pkg_title)
            else:
                print("Found package, safe to move.")
                success = set_new_parent_ref(accesstoken, pkg_uuid, args.new_parent_ref)

                if success:
                    print(f"Move workflow for '{pkg_title}' started.")
                    successful_moves.add(pkg_title)
                else:
                    print(f"Move FAILED: Could not initiate the move for package {pkg_title} / uuid {pkg_uuid}.")
                    failed_moves.add(pkg_title)
            
            i += 1 # move to the next pkg only if current was processed

        except Exception as e:
            logging.error(f"An unexpected error occurred while processing '{pkg_title}': {e}")
            failed_moves.add(pkg_title)
            i += 1 # move to the next pkg even if unexpected error

    print(f"\n--- SUMMARY ---")
    print(f"\nTotal packages processed: {len(pkg_list)}")
    print(f"Successful moves started: {len(successful_moves)}")

    if len(deletion_exists) > 0:
        print(f"Pkgs already in deletion folder: {len(deletion_exists)}")

    if len(failed_moves) > 0:
        print(f"\nFailed moves or errors: ({len(failed_moves)})")
        for pkg in sorted(list(failed_moves)):
            print(f"- {pkg}")


if __name__ == "__main__":
    main()
