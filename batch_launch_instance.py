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
            raise RuntimeError("No image with the id of " + id)
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
            exit(1)
        except json.decoder.JSONDecodeError:
            print("Fail to parse response body as JSON")
            exit(1)
    
    def launch_instance_off_image(self, name, source_uuid, size_alias, alloc_src_uuid, project_uuid, identity_uuid):
        try:
            headers = {}
            headers["Content-Type"] = "application/json"

            url = "/api/v2/instances"

            data = {}
            data["source_alias"] = "320b6a20-38eb-47a6-a33c-1216b1aa3399"
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
            exit(1)

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

    def delete_instance(self, proivder_uuid, identity_uuid, instance_uuid, action, reboot_type=""):
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

    def  __init__(self, api_client, image_id, image_version, size, name="", project=""):
        self.api_client = api_client
        self.name = name
        self.project = project
        self.image_id = image_id
        self.image_version = image_version
        self.size = size
        self.instance_json = {}

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

        alloc_src = self.api_client.get_allocation_source(self.owner)
        project = self.api_client.get_project(self.owner)
        identity = self.api_client.get_identity(self.owner)
        source_uuid = self.api_client.list_machines_of_image_version(self.image_id, self.image_version)[0]["uuid"]
        if not self.name:
            self.name = self.api_client.get_image(self.image_id)["name"]

        self.instance_json = self.api_client.launch_instance_off_image(self.owner, source_uuid, size_entry["alias"], alloc_src["uuid"], project["uuid"], identity["uuid"])
        self.launch_time = time.mktime(time.localtime())
        self.id = self.instance_json["id"]

    def wait_active(self):
        status = ""
        acivity = ""
        while not (status == "active" and acivity == ""):
            new_status = self.status()
            if new_status != (status, acivity):
                print("instance id: {}, status: {}, activity: {}".format(self.id, status, acivity))
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
            url = "/api/v1"
            url += "/provider/" + self.instance_json["provider"]["uuid"]
            url += "/identity/" + self.instance_json["identity"]["uuid"]
            url += "/instance/" + self.instance_json["uuid"]

            json_obj = self.api_client.delete_instance(url)
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

    def launch_instance(row, row_index):
        api_client = APIClient(platform="cyverse")
        api_client.login(row["username"], row["password"])
        try:
            instance = Instance(api_client, row["image"], row["image_version"], row["size"])
            instance.launch()
            print("Instance launched, username: {}, id: {}".format(instance.owner, instance.id))
        except Exception:
            print("row {} failed".format(row_index))
            instance = None
        return instance

    launched_instances = []

    # launch instance for each row (in csv or from arg)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [ executor.submit(launch_instance, row, row_index) for row_index, row in enumerate(instance_list) ]
        for launched in as_completed(futures):
            instance_json = launched.result()
            if instance_json:
                launched_instances.append(instance_json)

    if not args.dont_wait:
        # wait for instance to be active
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [ executor.submit(Instance.wait_active, instance) for instance in launched_instances ]
            for instance in as_completed(futures):
                if not instance.result():
                    print("Instance failed to launched in time, {}, last_status: {}".format(str(instance), instance.last_status))

def parse_args():
    parser = argparse.ArgumentParser(description="Clean up all resources allocated by one or more accounts, use csv file for more than one account")
    parser.add_argument("--username", dest="username", type=str, help="usernameing of the cyverse account, only specify 1 account")
    parser.add_argument("--csv", dest="csv_filename", type=str, help="filename of the csv file that contains credential for all the accounts")
    parser.add_argument("--dont-wait", dest="dont_wait", action="store_true", default=False, help="do not wait for instance to be fully launched (active)")

    global args
    args = parser.parse_args()

    if args.username:
        account = {}
        account["username"] = args.username
        account["password"] = getpass.getpass()
        account_list = [account]
    elif args.csv_filename:
        account_list = read_info_from_csv(args.csv_filename)
    else:
        print("Neither --username or --csv is specified, one is needed\n")
        parser.print_help()
        exit(1)
    return account_list

def read_info_from_csv(filename):
    instance_list = []

    with open(filename) as csvfile:
        csv_reader = csv.reader(csvfile)
        field = []

        for row_index, row in enumerate(csv_reader):
            # find the username and password field
            if not field:
                field = row
                username_index = find_field(field, "username")
                password_index = find_field(field, "password")
                image_url_index = find_field(field, "image")
                image_ver_index = find_field(field, "image version")
                size_index = find_field(field, "instance size")
                continue # skip the 1st row

            print_row(row, username_index, password_index)

            try:
                instance = parse_row(row, username_index, password_index, image_url_index, image_ver_index, size_index)
            except:
                print("row {} missing username or password field".format(row_index))
                raise
            instance_list.append(instance)

    return instance_list

def find_field(all_fields, field_name):
    """
    Find the index of a field in the field row
    """
    for i, field in enumerate(all_fields):
        if field == field_name:
            return i
    print("No field called " + field_name)
    exit(1)

def parse_row(row, username_index, password_index, image_url_index, image_ver_index, size_index):
    try:
        instance = {}
        instance["username"] = row[username_index]
        instance["password"] = row[password_index]
        url = row[image_url_index]
        image_id_str = url.split("/")[-1]   # get the image id from the url
        instance["image"] = int(image_id_str)
        instance["image_version"] = row[image_ver_index]
        instance["size"] = row[size_index]
    except IndexError:
        # unable to find the image id from url
        print("Bad image url")
        exit(1)
    except ValueError:
        # unable to convert image id to integer
        print("image id not integer")
        exit(1)

    return instance

def print_row(row, username_index, password_index):
    password = "".join([ "*" for c in row[password_index] ])
    print("username: ", row[username_index], "\t", "password: ", password)

def list_contains(l, field, value):
    for entry in l:
        if entry[field] == value:
            return entry
    return False

main()


