#!/usr/bin/env python

import requests
import json
import csv
import argparse
import sys
import getpass
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from requests.exceptions import HTTPError
from json.decoder import JSONDecodeError

"""
Launch instances in batch for multiple accounts

By default, instance will be
    launched using the allocation source with the same name as username (1st found),
    launched under the project with the same name as the username (1st found),
    launched using the identity with the same username (1st found),
    launched using the same name as the image name

example csv:

username,password,image,image_version,size
cyverse_us_01,Discovery#Science01#,https://atmo.cyverse.org/application/images/1552,2.0,tiny1

"""

def retry_3(exception=Exception):
    """
    a decorator that retry function 3 times
    """
    def inner_wrapper(func):
        def inner(*args, **kwargs):
            error = None
            for attempt in range(3):
                try:
                    return func(*args, **kwargs)
                except exception as e:
                    error = e
                    if attempt < 2:
                        print("Retry")
                    pass
            else:
                raise error
        return inner
    return inner_wrapper


class APIClient:
    """
    API client that send request to the Atmosphere REST API endpoints to perform varies actions
    """
    def __init__(self, platform="cyverse"):
        if platform == "jetstream":
            self.api_base_url = "use.jetstream-cloud.org"
        elif platform == "cyverse":
            self.api_base_url = "atmo.cyverse.org"
        else:
            raise ValueError("Unknown platform")
        self.token = None

    @retry_3()
    def login(self, username, password):
        """
        Obtain a temp authorization token with username and password.
        Only usable on Cyverse Atmosphere.

        Args:
            username: username of the account
            password: password of the account
        Returns:
            a temporary access token
        """
        try:
            resp = requests.get("https://de.cyverse.org/terrain/token", auth=(username, password))
            resp.raise_for_status()
            self.token = resp.json()['access_token']
        except HTTPError:
            raise HTTPError("{}, Auentication failed, username: {}".format(resp.status_code, username))
        except json.decoder.JSONDecodeError:
            raise IncompleteResponse("Fail to parse response body as JSON")
        except KeyError:
            raise IncompleteResponse("Token missing from login")

    @retry_3()
    def list_instance_of_user(self):
        """
        List all instance of an account

        Returns:
            a json obj from the parsing the response
        """
        try:
            json_obj = self._atmo_get_req("/api/v2/instances")
            return json_obj["results"]
        except HTTPError as e:
            raise HTTPError(str(e.args) + "Fail to list all the instances")
        except KeyError:
            raise IncompleteResponse("Missing field")


    def get_project(self, name):
        """
        Search for a project by project name, return the 1st found, ignore duplicates

        Args:
            name: name of the project to search for
        Returns:
            the project (dict) with the given name
        """
        project_list = self.list_project_of_user()
        # ignore dulpcaite entry with the same name, return the 1st
        project = list_contains(project_list, "name", name)
        if not project:
            raise ValueError("No project with the name of " + name)
        return project

    @retry_3()
    def list_project_of_user(self):
        """
        List all the projects of an account
        """
        try:
            json_obj = self._atmo_get_req("/api/v2/projects")
        except HTTPError as e:
            raise HTTPError(str(e) + " Fail to list all the projects") from e
        return json_obj["results"]

    @retry_3()
    def instance_size_list(self):
        """
        Return a list of instance size supported by the target platform
        """
        try:
            json_obj = self._atmo_get_req("/api/v2/sizes")
        except HTTPError as e:
            raise HTTPError(str(e) + " Fail to list all the instance size") from e
        return json_obj["results"]

    def get_allocation_source(self, name):
        """
        Search for the allocation sources in the account by the name, return the 1st found, ignore duplicates

        Args:
            name: name of the allocation source to search for
        Returns:
            the allocation source with the given name
        """
        source_list = self.allocation_source_list()
        # ignore dulpcaite entry with the same name, return the 1st
        alloc_src = list_contains(source_list, "name", name)
        if not alloc_src:
            raise ValueError("No allocation source with the name of " + name)
        return alloc_src

    @retry_3()
    def allocation_source_list(self):
        """
        Returns a list of allocation source of the account
        """
        try:
            json_obj = self._atmo_get_req("/api/v2/allocation_sources")
        except HTTPError as e:
            raise HTTPError(str(e) + " Fail to list all allocation sources") from e
        return json_obj["results"]

    def get_identity(self, username):
        """
        Search for identity of account with the given username

        Args:
            username: username to search for
        Returns:
            return a json_obj of the identity
        """
        id_list = self.identity_list()
        for id in id_list:
        # ignore dulpcaite entry with the same name, return the 1st
            if id["user"]["username"] == username:
                return id
        raise ValueError("No identity with the username of " + username)

    @retry_3()
    def identity_list(self):
        """
        Returns a list of identity
        """
        try:
            json_obj = self._atmo_get_req("/api/v2/identities")
        except HTTPError as e:
            raise HTTPError(str(e) + " Fail to list all identity") from e
        return json_obj["results"]

    def get_image(self, id):
        """
        Search for the image with the given id

        Args:
            id: id of the image
        Returns:
            return the image with the given id
        """
        img_list = self.image_list()
        img = list_contains(img_list, "id", id)
        if not img:
            raise ValueError("No image with the id of " + str(id))
        return img

    @retry_3()
    def image_list(self):
        """
        Returns a list of image
        """
        try:
            json_obj = self._atmo_get_req("/api/v2/images")
        except HTTPError as e:
            raise HTTPError(str(e) + " Fail to list all images") from e
        return json_obj["results"]

    @retry_3()
    def list_machines_of_image_version(self, image_id, image_version):
        """
        Get a list of machines of the image with the specific version,
        the uuid of the machine is the source_uuid that launch() is refer to.

        Args:
            image_id: id of the image
            image_version: version of the image, e.g. "2.1"
        Returns:
            "machines" field of json response of the image version
        """
        image = self.get_image(image_id)
        version = list_contains(image["versions"], "name", image_version)
        if not version:
            raise IncompleteResponse("No version with the name of " + image_version)
        version_url = version["url"]

        try:
            json_obj = self._atmo_get_req("", full_url=version_url)
        except HTTPError as e:
            raise HTTPError(str(e) + " Fail to list all images") from e
        machines = json_obj["machines"]
        return machines

    def account_username(self):
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

    @retry_3()
    def user_profile(self):
        """
        Get the user profile
        """
        try:
            json_obj = self._atmo_get_req("/api/v1/profile")

            return json_obj
        except HTTPError as e:
            raise HTTPError("Fail to list profile") from e
        except JSONDecodeError as e:
            raise IncompleteResponse("Fail to parse response body as JSON") from e
    
    @retry_3(exception=HTTPError)
    def launch_instance_off_image(self, name, source_uuid, size_alias, alloc_src_uuid, project_uuid, identity_uuid):
        """
        Launch a instance from an image

        Args:
            name: name of the instance
            source_uuid: uuid of the source machine of the image
            size_alias: uuid of the size
            alloc_src_uuid: uuid of the allocation source
            project_uuid: uuid of the project to create the instance under
            identity_uuid: uuid of the identity to create the instance with
        Returns:
            parsed json response about the launched instance
        """
        try:
            headers = {}
            headers["Content-Type"] = "application/json"

            url = "/api/v2/instances"

            data = {}
            data["source_alias"] = source_uuid
            data["size_alias"] = size_alias
            data["allocation_source_id"] = alloc_src_uuid
            data["name"] = name
            data["scripts"] = []
            data["project"] = project_uuid
            data["identity"] = identity_uuid

            json_obj = self._atmo_post_req(url, json_data=data, additional_header=headers)

            json_formatted_str = json.dumps(json_obj, indent=2)
            print(json_formatted_str)

            return json_obj
        except HTTPError:
            raise HTTPError("Fail to launch instance with the specified image")

    @retry_3()
    def instance_status(self, instance_id):
        """
        Get the status and activiy of the instance

        Args:
            instance_id: id of the instance
        Returns:
            a tuple of (status, activity)
        """
        try:
            url = "/api/v2/instances/" + str(instance_id)
            json_obj = self._atmo_get_req(url)

            self.last_status = json_obj["status"]
            return (json_obj["status"], json_obj["activity"])
        except HTTPError:
            raise HTTPError("Failed to retrieve instance status")

    @retry_3()
    def instance_status_v1(self, provider_uuid, identity_uuid, instance_uuid):
        """
        Get the status and activiy of the instance

        Args:
            instance_id: id of the instance
        Returns:
            a tuple of (status, activity)
        """
        try:
            url = "/api/v1/provider/{}/identity/{}/instance/{}".format(provider_uuid, identity_uuid, instance_uuid)
            json_obj = self._atmo_get_req(url)

            self.last_status = json_obj["status"]
            return (json_obj["status"], json_obj["activity"])
        except HTTPError:
            raise HTTPError("Failed to retrieve instance status")
        except KeyError:
            raise IncompleteResponse("Failed to retrieve instance status, missing status and activiy from response")

    @retry_3()
    def instance_action(self, proivder_uuid, identity_uuid, instance_uuid, action, reboot_type=""):
        """
        Perform an action on an instance.
        Available actions: reboot, suspend

        Args:
            provider_uuid: uuid of the provider the instance is on
            identity_uuid: uuid of the identity the instance is created with
            instance_uuid: uuid of the instance to perform the action on
            action: the action to be performed
            reboot_type: if the action is reboot, specify the type of the reboot, HARD or SOFT
        """
        try:
            url = "/api/v1"
            url += "/provider/" + proivder_uuid
            url += "/identity/" + identity_uuid
            url += "/instance/" + instance_uuid
            url += "/action"

            data = {}
            data["action"] = action
            if action == "reboot" and reboot_type:
                data["reboot_type"] = "HARD"

            resp = self._atmo_post_req(url, json_data=data)
            resp.raise_for_status()

            json_obj = json.loads(resp.text)
        except HTTPError:
            raise HTTPError("Fail to {} instance".format(action))
        except JSONDecodeError:
            raise IncompleteResponse("Fail to parse response body as JSON")

    @retry_3()
    def delete_instance(self, proivder_uuid, identity_uuid, instance_uuid):
        """
        Delete an instance

        Args:
            provider_uuid: uuid of the provider the instance is on
            identity_uuid: uuid of the identity the instance is creaed with
            instance_uuid: uuid of the instance to be deleted
        """
        try:
            url = "/api/v1"
            url += "/provider/" + proivder_uuid
            url += "/identity/" + identity_uuid
            url += "/instance/" + instance_uuid

            json_obj = self._atmo_delete_req(url)

        except HTTPError:
            raise HTTPError("Fail to delete instance")

    def _atmo_get_req(self, url, additional_header={}, full_url=""):
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
            raise IncompleteResponse("Fail to parse response body as JSON")
        return json_obj

    def _atmo_post_req(self, url, data={}, json_data={}, additional_header={}):
        """
        Send a POST request to the target service, will prepend a base url in front of the url depends on platform

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
            headers["Content-Type"] = "application/json"
            if self.token:
                headers["Authorization"] = "TOKEN " + self.token

            url = "https://" + self.api_base_url + url
            if json_data:
                resp = requests.post(url, headers=headers, json=json_data)
            else:
                resp = requests.post(url, headers=headers, data=data)
            resp.raise_for_status()
            json_obj = json.loads(resp.text)
        except JSONDecodeError:
            if json_data:
                print(json_data)
            raise IncompleteResponse("Fail to parse response body as JSON")
        return json_obj

    def _atmo_delete_req(self, url, additional_header={}):
        """
        Send a DELETE request to the target service, will prepend a base url in front of the url depends on platform

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
            headers["Content-Type"] = "application/json"
            if self.token:
                headers["Authorization"] = "TOKEN " + self.token

            url = "https://" + self.api_base_url + url
            resp = requests.delete(url, headers=headers)
            resp.raise_for_status()
            json_obj = json.loads(resp.text)
        except JSONDecodeError:
            raise IncompleteResponse("Fail to parse response body as JSON")
        return json_obj


class Instance:

    def  __init__(self, api_client, image_id, image_version, size, opt=None):
        self.api_client = api_client
        self.image_id = image_id
        self.image_version = image_version
        self.size = size
        self.instance_json = {}

        self.name = ""
        self.project = ""
        self.alloc_src = ""
        if opt:
            if "name" in opt:
                self.name = opt["name"]
            if "project" in opt:
                self.project = opt["project"]
            if "alloc_src" in opt:
                self.alloc_src = opt["alloc_src"]
        
        self.owner = self.api_client.account_username()

    def status(self):
        """
        Get the status of the instance.

        Returns:
            a tuple of (status, activity)
        """
        status, activity = self.api_client.instance_status_v1(self.provider_uuid, self.identity_uuid, self.uuid)
        self.last_status = status
        return (status, activity)
        
    def launch(self):
        """
        Launch the instance with all the given parameters
        """
        try:
            size_list = self.api_client.instance_size_list()
            size_entry = list_contains(size_list, "name", self.size)
            if not size_entry:
                raise ValueError("Invalid size")

            if self.alloc_src:
                alloc_src = self.api_client.get_allocation_source(self.alloc_src)
            else:
                alloc_src = self.api_client.get_allocation_source(self.owner)
            # if self.project:
            #     project = self.api_client.get_project(self.project)
            # else:
            #     project = self.api_client.get_project(self.owner)
            project = self.api_client.get_project(self.owner)
            identity = self.api_client.get_identity(self.owner)
            source_uuid = self.api_client.list_machines_of_image_version(self.image_id, self.image_version)[0]["uuid"]
            if not self.name:
                self.name = self.api_client.get_image(self.image_id)["name"]

            print(self.name, source_uuid, size_entry["alias"], alloc_src["uuid"], project["uuid"], identity["uuid"])
            self.instance_json = self.api_client.launch_instance_off_image(self.name, source_uuid, size_entry["alias"], alloc_src["uuid"], project["uuid"], identity["uuid"])
            self.provider_uuid = self.instance_json["provider"]["uuid"]
            self.identity_uuid = self.instance_json["identity"]["uuid"]
            self.uuid = self.instance_json["uuid"]
            self.launch_time = time.mktime(time.localtime())
            self.id = self.instance_json["id"]
        except Exception as exc:
            print(exc)
            raise exc

    def wait_active(self):
        """
        Returns:
            a tuple (succeed or not, id)
        """
        result = self._wait_active()
        return result, self

    def _wait_active(self):
        """
        Wait for the instance to become fully active (status == "active" && activity == "").
        Check for the instance status every some second
        """
        status = ""
        acivity = ""
        while not (status == "active" and acivity == ""):
            try:
                new_status = self.status()
            except Exception as e:
                print(e)
                False
            # only print updates if new status or new activity
            if new_status != (status, acivity):
                print("instance id: {}, status: {}, activity: {}".format(self.id, new_status[0], new_status[1]))
            status, acivity = new_status

            # timeout after 30min
            curr_time = time.mktime(time.localtime())
            if curr_time - self.launch_time > 1800:
                return False

            # error or deploy_error
            if status == "error":
                return False
            if status == "deploy_error":
                return False

            time.sleep(5)
        return True

    def delete(self):
        """
        Delete the instance
        """
        try:
            json_obj = self.api_client.delete_instance(self.instance_json["provider"]["uuid"],
                self.instance_json["identity"]["uuid"],
                self.instance_json["uuid"])
        except HTTPError as e:
            raise HTTPError(str(e) + "Fail to delete instance")

    def reboot(self):
        """
        Reboot the instance
        """
        try:
            json_obj = self.api_client.instance_action(
                self.instance_json["provider"]["uuid"],
                self.instance_json["identity"]["uuid"],
                self.instance_json["uuid"],
                "reboot",
                reboot_type="HARD"
            )
        except HTTPError as e:
            raise HTTPError(str(e) + "Fail to reboot instance")
       
    def __str__(self):
        if hasattr(self, "id") and self.id:
            return "username: {}, id: {}, uuid: {}".format(self.owner, self.id, self.instance_json["uuid"])
        else:
            return "username: {}, image id: {}, image version: {}, size: {}".format(self.owner, self.image_id, self.image_version, self.size)

def main():
    # read accounts credentials
    instance_list = parse_args()

    launched_instances = []

    # Check credential
    api_clients = []
    for row_index, row in enumerate(instance_list):
        api_client = account_login(row, row_index)
        # Quit if any row has incorrect credential
        if not api_client:
            return
        api_clients.append(api_client)

    # launch instance for each row (in csv or from arg)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [ executor.submit(launch_instance, api_clients[row_index], row, row_index) for row_index, row in enumerate(instance_list) ]
        for launched in as_completed(futures):
            instance_json = launched.result()
            if instance_json:
                launched_instances.append(instance_json)

    launched_summary(launched_instances)

    if not args.dont_wait:
        # wait for instance to be active
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [ executor.submit(Instance.wait_active, instance) for instance in launched_instances ]
            for launch in as_completed(futures):
                succeed, instance = launch.result()
                if not succeed:
                    print("Instance failed to become fully active in time, {}, {}, last_status: {}".format(instance.owner, str(instance), instance.last_status))

class IncompleteResponse(ValueError):
    pass
class CSVError(ValueError):
    pass

def parse_args():
    """
    Parsing the cmd arguments

    Returns:
        a list of instance to be launched
        "username", "password" or "token"
        ["image", "image version", "instance size"]
        ["instance name", "project name", "allocation source"
"""
    parser = argparse.ArgumentParser(description="Clean up all resources allocated by one or more accounts, use csv file for more than one account")
    parser.add_argument("--csv", dest="csv_filename", type=str, required=True, help="filename of the csv file that contains credential for all the accounts")
    parser.add_argument("--dont-wait", dest="dont_wait", action="store_true", default=False, help="do not wait for instance to be fully launched (active)")
    parser.add_argument("--cyverse", dest="cyverse", action="store_true", help="Target platform: Cyverse Atmosphere (default)")
    parser.add_argument("--jetstream", dest="jetstream", action="store_true", help="Target platform: Jetstream")
    parser.add_argument("--token", dest="token", action="store_true", help="use access token instead of username & password, default for Jetstream")

    global args
    args = parser.parse_args()

    # target platform
    if args.jetstream:
        args.token = True   # Use token on Jetstream
    else:
        args.cyverse = True

    if args.csv_filename:
        account_list = read_info_from_csv(args.csv_filename, args.token)
    else:
        print("--csv required but not specified\n")
        parser.print_help()
        exit(1)
    return account_list

def read_info_from_csv(filename, use_token):
    """
    Read instance info from a csv file

    Args:
        filename: file name of the csv file
        use_token: use token or username&password, only valid for Cyverse Atmosphere
    Returns:
        a list of instance
    """
    instance_list = []

    with open(filename) as csvfile:
        csv_reader = csv.reader(csvfile)
        all_fields = []

        try:
            for row_index, row in enumerate(csv_reader):
                # find the relevant field
                if not all_fields:
                    all_fields = row
                    required = ["image", "image version", "instance size"]
                    optional = ["instance name", "project name", "allocation source"]
                    if use_token:
                        required.append("token")
                    else:
                        required.append("username")
                        required.append("password")
                    required_index, optional_index = find_fields(all_fields, required, optional)
                    continue # skip the 1st row

                try:
                    instance = parse_row(use_token, row, required_index, optional_index)
                    print_row(instance)
                except Exception as e:
                    raise CSVError(str(e) + "\nrow {} missing reuquired field".format(row_index)) from e
                instance_list.append(instance)
        except CSVError as e:
            print(e)
            exit(1)

    return instance_list

def find_fields(all_fields, required_fields, optional_fields):
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

def image_id_from_url(url):
    """
    Get the image id from the url

    Args:
        url: url of the image
    Returns:
        image id as an integer
    """
    try:
        # get the image id from the url
        image_id_str = url.split("/")[-1]
        return int(image_id_str)
    except IndexError:
        # unable to find the image id from url
        raise IndexError("Bad image url")
    except ValueError:
        # unable to convert image id to integer
        raise ValueError("image id not integer")

def parse_row(use_token, row, required_index, optional_index):
    """
    Args:
        use_token: whether or not to use token rather than username and password
        row: a list that contains all the fields in a row in csv
        required_index: a list of index in the row list that represent fields that are required
        optional_index: a list of index in the row list that represent fields that are optional
    Returns:
        return a dict contains info obtained from the row, with the required and optional fields
    """
    instance = {}
    if use_token:
        instance["token"] = row[required_index["token"]]
    else:
        instance["username"] = row[required_index["username"]]
        instance["password"] = row[required_index["password"]]

    instance["image"] = image_id_from_url(row[required_index["image"]])
    instance["image_version"] = row[required_index["image version"]]
    instance["size"] = row[required_index["instance size"]]
    if "instance name" in optional_index:
        instance["name"] = row[optional_index["instance name"]]
    if "allocation source" in optional_index:
        instance["alloc_src"] = row[optional_index["allocation source"]]
    if "project name" in optional_index:
        instance["project"] = row[optional_index["project name"]]
    return instance


def print_row(instance):
    """
    Args:
        instance: a dict that represents an instance
    """
    if "token" in instance:
        print("token: ", instance["token"], end='')
    else:
        password = "".join([ "*" for c in instance["password"] ])
        print("username: ", instance["username"], "\t", "password: ", password, end='')
    print("\timage: {}\timage ver: {}\tsize: {}".format(instance["image"], instance["image_version"], instance["size"]))

def list_contains(l, field, value):
    """
    Args:
        l: a list in which each element is a dict
        field: field in an element (a dict) to look up on
        value: value corrsponding to the field to find a match with
    Returns:
        False if no match is found
    """
    for entry in l:
        if entry[field] == value:
            return entry
    return False

def account_login(row, row_index):
    """
    Launch an instance

    Args:
        row: a dict contains info about an instance to be launched
        row_index: index (row number) in the list of instance, used to report errors
    """
    try:
        # platform
        if args.jetstream:
            api_client = APIClient(platform="jetstream")
        else:
            api_client = APIClient(platform="cyverse")

        # token or username
        if args.token:
            api_client.token = row["token"]
        else:
            api_client.login(row["username"], row["password"])

        # try to get username via api to confirm token works
        api_client.account_username()

        return api_client
    except Exception as e:
        print("row {} failed during authentication, check credential".format(row_index))
        print(row)
        print(e)
        return None

def launch_instance(api_client, instance, row_index):
    """
    Launch an instance

    Args:
        api_client: api_client that has been authenticated
        row_index: index (row number) in the list of instance, used to report errors
    """
    try:
        instance = Instance(api_client, instance["image"], instance["image_version"], instance["size"], opt=instance)
        instance.launch()
        print("Instance launched, username: {}, id: {}".format(instance.owner, instance.id))
    except Exception as e:
        print("row {} failed".format(row_index))
        print(instance)
        print(e)
        instance = None
    return instance

def launched_summary(launched_instances):
    print("==============================")
    print("{} instance launched".format(len(launched_instances)))
    print("\n\n")

if __name__ == '__main__':
    main()


