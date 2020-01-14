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

def retry_3(func):
    """
    retry function 3 times
    """
    def inner(*args, **kwargs): 
        error = None
        for attempt in range(3):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error = e
                if attempt < 2:
                    print("Retry")
                pass
        else:
            raise error
    return inner


class APIClient:
    def __init__(self, platform="cyverse"):
        if platform == "jetstream":
            self.api_base_url = "use.jetstream-cloud.org"
        elif platform == "cyverse":
            self.api_base_url = "atmo-36.cyverse.org"
        else:
            raise RuntimeError("Unknow platform")
        self.token = None

    def login(self, username, password):
        """
        Obtain a temp authorization token with username and password
        """
        try:
            resp = requests.get("https://de.cyverse.org/terrain/token", auth=(username, password))
            resp.raise_for_status()
            self.token = resp.json()['access_token']
        except requests.exceptions.HTTPError:
            print("Auentication failed, username: ", username)
            raise
        except json.decoder.JSONDecodeError:
            print("Fail to parse response body as JSON")
            raise

    def list_instance_of_user(self):
        try:
            json_obj = self._atmo_get_req("/api/v2/instances")
        except requests.exceptions.HTTPError:
            print("Fail to list all the instances")
            raise
        return json_obj["results"]

    def get_project(self, name):
        project_list = self.list_project_of_user()
        # ignore dulpcaite entry with the same name, return the 1st
        project = list_contains(project_list, "name", name)
        if not project:
            raise RuntimeError("No project with the name of " + name)
        return project

    def list_project_of_user(self):
        try:
            json_obj = self._atmo_get_req("/api/v2/projects")
        except requests.exceptions.HTTPError:
            print("Fail to list all the projects")
            raise
        return json_obj["results"]

    def instance_size_list(self):
        try:
            json_obj = self._atmo_get_req("/api/v2/sizes")
        except requests.exceptions.HTTPError:
            print("Fail to list all the instance size")
            raise
        return json_obj["results"]

    def get_allocation_source(self, name):
        source_list = self.allocation_source_list()
        # ignore dulpcaite entry with the same name, return the 1st
        alloc_src = list_contains(source_list, "name", name)
        if not alloc_src:
            raise RuntimeError("No allocation source with the name of " + name)
        return alloc_src

    def allocation_source_list(self):
        try:
            json_obj = self._atmo_get_req("/api/v2/allocation_sources")
        except requests.exceptions.HTTPError:
            print("Fail to list all allocation sources")
        return json_obj["results"]

    def get_identity(self, username):
        id_list = self.identity_list()
        for id in id_list:
        # ignore dulpcaite entry with the same name, return the 1st
            if id["user"]["username"] == username:
                return id
        raise RuntimeError("No identity with the username of " + username)

    def identity_list(self):
        try:
            json_obj = self._atmo_get_req("/api/v2/identities")
        except requests.exceptions.HTTPError:
            print("Fail to list all identity")
            raise
        return json_obj["results"]

    def get_image(self, id):
        img_list = self.image_list()
        img = list_contains(img_list, "id", id)
        if not img:
            raise RuntimeError("No image with the id of " + str(id))
        return img

    def image_list(self):
        try:
            json_obj = self._atmo_get_req("/api/v2/images")
        except requests.exceptions.HTTPError:
            print("Fail to list all images")
            raise
        return json_obj["results"]

    def list_machines_of_image_version(self, image_id, image_version):
        image = self.get_image(image_id)
        version = list_contains(image["versions"], "name", image_version)
        if not version:
            raise RuntimeError("No version with the name of " + image_version)
        version_url = version["url"]

        try:
            json_obj = self._atmo_get_req("", full_url=version_url)
        except requests.exceptions.HTTPError:
            print("Fail to list all images")
            raise
        machines = json_obj["machines"]
        return machines

    def account_username(self):
        profile = self.user_profile()
        return profile["username"]

    def user_profile(self):
        try:
            json_obj = self._atmo_get_req("/api/v1/profile")

            return json_obj
        except requests.exceptions.HTTPError:
            print("Fail to list profile")
            raise
        except json.decoder.JSONDecodeError:
            print("Fail to parse response body as JSON")
            raise
    
    def launch_instance_off_image(self, name, source_uuid, size_alias, alloc_src_uuid, project_uuid, identity_uuid):
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
        except requests.exceptions.HTTPError:
            print("Fail to launch instance with the specified image")
            raise

    def instance_status(self, instance_id):
        try:
            url = "/api/v2/instances/" + str(instance_id)
            json_obj = self._atmo_get_req(url)

            self.last_status = json_obj["status"]
            return (json_obj["status"], json_obj["activity"])
        except requests.exceptions.HTTPError:
            print("Failed to retrieve instance status")
            raise

    def instance_action(self, proivder_uuid, identity_uuid, instance_uuid, action, reboot_type=""):
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
        except requests.exceptions.HTTPError:
            print("Fail to {} instance".format(action))
            raise
        except json.decoder.JSONDecodeError:
            print("Fail to parse response body as JSON")
            raise

    def delete_instance(self, proivder_uuid, identity_uuid, instance_uuid):
        try:
            url = "/api/v1"
            url += "/provider/" + proivder_uuid
            url += "/identity/" + identity_uuid
            url += "/instance/" + instance_uuid

            json_obj = self._atmo_delete_req(url)

        except requests.exceptions.HTTPError:
            print("Fail to delete instance")
            raise

    def _atmo_get_req(self, url, additional_header={}, full_url=""):
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
        except json.decoder.JSONDecodeError:
            print("Fail to parse response body as JSON")
            raise
        return json_obj

    def _atmo_post_req(self, url, data={}, json_data={}, additional_header={}):
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
        except requests.exceptions.HTTPError:
            print(resp.text)
            raise
        except json.decoder.JSONDecodeError:
            print("Fail to parse response body as JSON")
            raise
        return json_obj

    def _atmo_delete_req(self, url, additional_header={}):
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
        except requests.exceptions.HTTPError:
            print(resp.text)
            raise
        except json.decoder.JSONDecodeError:
            print("Fail to parse response body as JSON")
            raise
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

    @retry_3
    def status(self):
        status, activity = self.api_client.instance_status(self.id)
        self.last_status = status
        return (status, activity)
        
    def launch(self):
        size_list = self.api_client.instance_size_list()
        size_entry = list_contains(size_list, "name", self.size)
        if not size_entry:
            raise RuntimeError("Invalid size")

        if self.alloc_src:
            alloc_src = self.api_client.get_allocation_source(self.alloc_src)
        else:
            alloc_src = self.api_client.get_allocation_source(self.owner)
        project = self.api_client.get_project(self.owner)
        identity = self.api_client.get_identity(self.owner)
        source_uuid = self.api_client.list_machines_of_image_version(self.image_id, self.image_version)[0]["uuid"]
        if not self.name:
            self.name = self.api_client.get_image(self.image_id)["name"]

        self.instance_json = self.api_client.launch_instance_off_image(self.name, source_uuid, size_entry["alias"], alloc_src["uuid"], project["uuid"], identity["uuid"])
        self.launch_time = time.mktime(time.localtime())
        self.id = self.instance_json["id"]

    def wait_active(self):
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
        try:
            json_obj = self.api_client.delete_instance(self.instance_json["provider"]["uuid"],
                self.instance_json["identity"]["uuid"],
                self.instance_json["uuid"])
        except requests.exceptions.HTTPError:
            print("Fail to delete instance")
            raise

    def reboot(self):
        try:
            json_obj = self.api_client.instance_action(
                self.instance_json["provider"]["uuid"],
                self.instance_json["identity"]["uuid"],
                self.instance_json["uuid"],
                "reboot",
                reboot_type="HARD"
            )
        except requests.exceptions.HTTPError:
            print("Fail to reboot instance")
            raise
       
    def __str__(self):
        if self.id:
            return "username: {}, id: {}, uuid: {}".format(self.owner, self.id, self.instance_json["uuid"])
        else:
            return "username: {}, image id: {}, image version: {}, size: {}".format(self.owner, self.image_id, self.image_version, self.size)

def main():
    # read accounts credentials
    instance_list = parse_args()

    launched_instances = []

    # launch instance for each row (in csv or from arg)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [ executor.submit(launch_instance, row, row_index) for row_index, row in enumerate(instance_list) ]
        for launched in as_completed(futures):
            instance_json = launched.result()
            if instance_json:
                launched_instances.append(instance_json)

    print("==============================")
    print("{} instance launched".format(len(launched_instances)))
    print("\n\n")

    if not args.dont_wait:
        # wait for instance to be active
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [ executor.submit(Instance.wait_active, instance) for instance in launched_instances ]
            for launch in as_completed(futures):
                if not launch.result():
                    instance = launch.result()
                    print("Instance failed to become fully active in time, {}, last_status: {}".format(str(instance), instance.last_status))

def parse_args():
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
                    raise RuntimeError("row {} missing reuquired field".format(row_index))
                instance_list.append(instance)
        except RuntimeError as e:
            print(e)
            exit(1)

    return instance_list

def find_fields(all_fields, required_fields, optional_fields):
    """
    Find the index of a field in the field row
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
                raise RuntimeError("No field called " + field)
    return required_fields_index, optional_fields_index

def image_id_from_url(url):
    try:
        # get the image id from the url
        image_id_str = url.split("/")[-1]
        return int(image_id_str)
    except IndexError:
        # unable to find the image id from url
        print("Bad image url")
        raise
    except ValueError:
        # unable to convert image id to integer
        print("image id not integer")
        raise

def parse_row(use_token, row, required_index, optional_index):
    instance = {}
    if use_token:
        instance["token"] = row[required_index["token"]]
    else:
        instance["username"] = row[required_index["username"]]
        instance["password"] = row[required_index["username"]]

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
    if "token" in instance:
        print("token: ", instance["token"], end='')
    else:
        password = "".join([ "*" for c in instance["password"] ])
        print("username: ", instance["username"], "\t", "password: ", password, end='')
    print("\timage: {}\timage ver: {}\tsize: {}".format(instance["image"], instance["image_version"], instance["size"]))

def list_contains(l, field, value):
    for entry in l:
        if entry[field] == value:
            return entry
    return False

def launch_instance(row, row_index):
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
    try:
        instance = Instance(api_client, row["image"], row["image_version"], row["size"], opt=row)
        instance.launch()
        print("Instance launched, username: {}, id: {}".format(instance.owner, instance.id))
    except Exception as e:
        print("row {} failed".format(row_index))
        print(row)
        print(e)
        instance = None
    return instance

main()


