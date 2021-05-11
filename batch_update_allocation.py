#!/usr/bin/env python3

import sys
import argparse
import csv
import requests
import json
from requests.exceptions import HTTPError
from json.decoder import JSONDecodeError
from typing import List, Dict, Tuple, Any

class APIClient:
    def __init__(self, token, platform="cyverse"):
        self._token = token
        if platform == "cyverse":
            self._api_base_url = "atmo.cyverse.org"
        elif platform == "jetstream":
            self._api_base_url = "use.jetstream-cloud.org"
        else:
            raise ValueError("Unknown platform")

    @property
    def token(self):
        return self._token

    @property
    def api_base_url(self):
        return self._api_base_url

    def user_alloc_src(self, username : str) -> Tuple[str, int]:
        """
        Get uuid of the 1st allocation source of a given user specified by username
        """
        url = "/api/v2/allocation_sources?username={}".format(username)
        json_obj = self._atmo_get_req(url)

        if json_obj["count"] < 1:
            return None
        # get the 1st alloc src
        alloc_src = json_obj["results"][0]

        uuid = alloc_src["uuid"]
        current_au = alloc_src["compute_allowed"]
        print("{}, current AU count: {}".format(username, current_au))

        return (uuid, current_au)
    
    def update_AU(self, alloc_src_uuid : str, alloc_unit_count : int) -> int:
        """
        Update allocation unit limit of an allocation source
        """
        url = "/api/v2/allocation_sources/{}".format(alloc_src_uuid)

        data = dict()
        data["compute_allowed"] = alloc_unit_count

        resp = self._atmo_patch_req(url, json_data=data)

        if resp["compute_allowed"] == alloc_unit_count and resp["uuid"] == alloc_src_uuid:
            return resp["compute_allowed"]

        return -1

    def account_username(self) -> str:
        """
        Get username of account from identity
        """
        id_list = self.identity_list()
        if len(id_list) == 0:
            raise IncompleteResponse("Account has no identity")
        try:
            username = id_list[0]["user"]["username"]
            return username
        except IndexError as e:
            raise IncompleteResponse("Response incomplete") from e

    def identity_list(self) -> dict:
        """
        Returns a list of identity
        """
        try:
            json_obj = self._atmo_get_req("/api/v2/identities")
        except HTTPError as e:
            raise HTTPError(str(e) + " Fail to list all identity") from e
        return json_obj["results"]

    def _atmo_get_req(self, url : str, additional_header : dict = {}, full_url : str = "") -> dict:
        """
        Send a GET request to the target service, will prepend a base url in front of the url depends on platform

        Args:
            url: url to send the request to
            additional_header: other header to be included
            full_url: to override the use of api_base_url
        Returns:
            return the response parsed by json module
        """
        try:
            headers = additional_header
            headers["Host"] = self.api_base_url
            headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
            if self.token:
                headers["Authorization"] = "TOKEN " + self.token

            if full_url:
                url = full_url
            else:
                url = "https://" + self.api_base_url + url
            resp = requests.get(url, headers=headers)
            resp.raise_for_status()
            json_obj = json.loads(resp.text)
        except JSONDecodeError:
            raise HTTPError("Fail to parse response body as JSON")
        return json_obj

    def _atmo_patch_req(self, url : str, additional_header : dict = {}, full_url : str = "", json_data : dict = {}) -> dict:
        """
        Send a PATCH request to the target service, will prepend a base url in front of the url depends on platform

        Args:
            url: url to send the request to
            additional_header: other header to be included
            full_url: to override the use of api_base_url
        Returns:
            return the response parsed by json module
        """
        try:
            headers = additional_header
            headers["Host"] = self.api_base_url
            headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
            if self.token:
                headers["Authorization"] = "TOKEN " + self.token

            if full_url:
                url = full_url
            else:
                url = "https://" + self.api_base_url + url
            resp = requests.patch(url, headers=headers, json=json_data)
            resp.raise_for_status()
            json_obj = json.loads(resp.text)
        except JSONDecodeError:
            raise HTTPError("Fail to parse response body as JSON")
        return json_obj

class IncompleteResponse(ValueError):
    pass
class CSVError(ValueError):
    pass

def parse_arg() -> Tuple[list, argparse.Namespace]:
    """
    Parse cmd args
    """
    parser = argparse.ArgumentParser(description="Update allocation unit limit of accounts using access token of an admin account")
    parser.add_argument("--csv", dest="csv_filename", type=str, required=True, help="filename of the csv file that contains all the usernames and target allocation unit count")
    parser.add_argument("--cyverse", dest="cyverse", action="store_true", help="Target platform: Cyverse Atmosphere (default)")
    parser.add_argument("--jetstream", dest="jetstream", action="store_true", help="Target platform: Jetstream")
    parser.add_argument("--token", dest="admin_token", type=str, required=True, help="access token of an admin account")
    parser.add_argument("--force-set", dest="force_set", action="store_true", help="force set the target AU, even if it is lower than current")

    args = parser.parse_args()

    rows = read_info_from_csv(args.csv_filename)

    return rows, args

def read_info_from_csv(filename : str) -> List[Dict[str, Any]]:
    """
    Read instance info from a csv file

    Args:
        filename: file name of the csv file
        use_token: use token or username&password, only valid for Cyverse Atmosphere
    Returns:
        a list of instance
    """
    row_info_list = list()

    with open(filename) as csvfile:
        csv_reader = csv.reader(csvfile)
        all_fields : List[str] = list()

        try:
            for row_index, row in enumerate(csv_reader):
                # find the relevant field
                if not all_fields:
                    all_fields = row
                    required = ["username", "alloc_unit_count"]
                    required_index, optional_index = find_fields(all_fields, required, list())
                    continue # skip the 1st row

                try:
                    row_info = parse_row(row, required_index, optional_index)
                    row_info["alloc_unit_count"] = int(row_info["alloc_unit_count"])
                    print_row(row_info)
                except ValueError as e:
                    raise CSVError(str(e) + "\nallocation unit count on row {} is not integer".format(row_index)) from e
                except Exception as e:
                    raise CSVError(str(e) + "\nrow {} missing required field".format(row_index)) from e
                row_info_list.append(row_info)
        except CSVError as e:
            print(e)
            exit(1)

    return row_info_list

def find_fields(all_fields : List[str], required_fields : List[str], optional_fields : List[str]) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Find the index of a field in the field row

    Args:
        all_fields: all of the fields in a row
        required_fields: fields that are required in csv
        optional_fields: fields that are optional in csv
    Returns:
        a tuple of 2 list of index, (required_fields_index, optional_fields_index)
    """
    required_fields_index = {}
    optional_fields_index = {}
    for i, field in enumerate(all_fields):
        if field in required_fields:
            required_fields_index[field] = i
        elif field in optional_fields:
            optional_fields_index[field] = i
    if len(required_fields_index) != len(required_fields):
        for field in required_fields:
            if field not in required_fields_index:
                raise ValueError("No field called " + field)
    return required_fields_index, optional_fields_index

def parse_row(row : List[str], required_index : Dict[str, int], optional_index : Dict[str, int]) -> Dict[str, Any]:
    """
    Args:
        use_token: whether or not to use token rather than username and password
        row: a list that contains all the fields in a row in csv
        required_index: a list of index in the row list that represent fields that are required
        optional_index: a list of index in the row list that represent fields that are optional
    Returns:
        return a dict contains info obtained from the row, with the required and optional fields
    """
    parsed = dict()

    for key in required_index.keys():
        parsed[key] = row[required_index[key]]
    for key in optional_index.keys():
        parsed[key] = row[required_index[key]]
    return parsed


def print_row(row : Dict[str, str]):
    """
    Args:
        row: a dict that represents an parsed row from csv file
    """
    print("username: {}, target allocation unit count: {}".format(row["username"], row["alloc_unit_count"]))

def update_user_AU(admin_token : str, username : str, target_alloc_unit_count : int, force_set: bool = False) -> None:
    """
    Update a user's allocation unit limit
    """

    client = APIClient(admin_token)

    try:
        # get uuid of allocation source
        alloc_src_uuid, current_au = client.user_alloc_src(username)
    except Exception as e:
        print(e)
        print("{}, fail to update AU limit".format(username))
        return
    try:
        if target_alloc_unit_count < current_au and not force_set:
            print("Skipped, target is lower than current AU, uses --force-set to force setting the target AU count")
            return
        new_au_count = client.update_AU(alloc_src_uuid, target_alloc_unit_count)
        if new_au_count < 0:
            print("Inconsistent response, fail to update AU limit")
        else:
            print("{}, new AU count: {}".format(username, new_au_count))
    except Exception as e:
        print(e)
        print("{}, fail to update AU limit".format(username))
        return

def main():
    rows, args = parse_arg()

    for row in rows:
        update_user_AU(args.admin_token, row["username"], row["alloc_unit_count"], args.force_set)

if __name__ == '__main__':
    main()


