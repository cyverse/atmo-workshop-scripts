#!/usr/bin/env python

import requests
import json
import csv
import argparse
import sys
import getpass

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


def main():
    # read accounts credentials
    instance_list = parse_args()

    for row, instance in enumerate(instance_list):
        # obtain token
        token = login(instance["username"],instance["password"])

        launch_instance_off_image(token, instance["username"], instance["image"], instance["image_version"], instance["size"])

def parse_args():
    parser = argparse.ArgumentParser(description="Clean up all resources allocated by one or more accounts, use csv file for more than one account")
    parser.add_argument("--username", dest="username", type=str, help="username of the cyverse account, only specify 1 account")
    parser.add_argument("--csv", dest="csv_filename", type=str, help="filename of the csv file that contains credential for all the accounts")

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

def login(username, password):
    """
    Obtain a temp authorization token with username and password
    """
    try:
        resp = requests.get("https://de.cyverse.org/terrain/token", auth=(username, password))
        resp.raise_for_status()
        token = resp.json()['access_token']
    except requests.exceptions.HTTPError:
        print("Auentication failed, username: ", username)
        exit(1)
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        exit(1)
    return token

def list_instance_of_user(token):
    try:
        json_obj = atmo_get_request("https://atmo.cyverse.org/api/v2/instances", token=token)
    except requests.exceptions.HTTPError:
        print("Fail to list all the instances")
        exit(1)
    return json_obj["results"]

def get_project(token, name):
    project_list = list_project_of_user(token)
    # ignore dulpcaite entry with the same name, return the 1st
    project = list_contains(project_list, "name", name)
    if not project:
        raise RuntimeError("No project with the name of " + name)
    return project

def list_project_of_user(token):
    try:
        json_obj = atmo_get_request("https://atmo.cyverse.org/api/v2/projects", token=token)
    except requests.exceptions.HTTPError:
        print("Fail to list all the projects")
        exit(1)
    return json_obj["results"]

def instance_size_list():
    try:
        json_obj = atmo_get_request("https://atmo.cyverse.org/api/v2/sizes")
    except requests.exceptions.HTTPError:
        print("Fail to list all the instance size")
        exit(1)
    return json_obj["results"]

def get_allocation_source(token, name):
    source_list = allocation_source_list(token)
    # ignore dulpcaite entry with the same name, return the 1st
    alloc_src = list_contains(source_list, "name", name)
    if not alloc_src:
        raise RuntimeError("No allocation source with the name of " + name)
    return alloc_src

def allocation_source_list(token):
    try:
        json_obj = atmo_get_request("https://atmo.cyverse.org/api/v2/allocation_sources", token=token)
    except requests.exceptions.HTTPError:
        print("Fail to list all allocation sources")
        exit(1)
    return json_obj["results"]

def get_identity(token, username):
    id_list = identity_list(token)
    for id in id_list:
    # ignore dulpcaite entry with the same name, return the 1st
        if id["user"]["username"] == username:
            return id
    raise RuntimeError("No identity with the username of " + username)

def identity_list(token):
    try:
        json_obj = atmo_get_request("https://atmo.cyverse.org/api/v2/identities", token=token)
    except requests.exceptions.HTTPError:
        print("Fail to list all identity")
        exit(1)
    return json_obj["results"]

def get_image(id):
    img_list = image_list()
    img = list_contains(img_list, "id", id)
    if not img:
        raise RuntimeError("No image with the id of " + id)
    return img

def image_list():
    try:
        json_obj = atmo_get_request("https://atmo.cyverse.org/api/v2/images")
    except requests.exceptions.HTTPError:
        print("Fail to list all images")
        exit(1)
    return json_obj["results"]

def list_machines_of_image_version(image_id, image_version):
    image = get_image(image_id)
    version = list_contains(image["versions"], "name", image_version)
    if not version:
        raise RuntimeError("No version with the name of " + image_version)
    version_url = version["url"]

    try:
        json_obj = atmo_get_request(version_url)
    except requests.exceptions.HTTPError:
        print("Fail to list all images")
        exit(1)
    machines = json_obj["machines"]
    return machines

def launch_instance_off_image(token, username, image_id, image_version, size, name=""):
    size_list = instance_size_list()
    size_entry = list_contains(size_list, "name", size)
    if not size_entry:
        raise RuntimeError("Invalid size")

    alloc_src = get_allocation_source(token, username)
    project = get_project(token, username)
    identity = get_identity(token, username)
    source_uuid = list_machines_of_image_version(image_id, image_version)[0]["uuid"]
    if not name:
        name = get_image(image_id)["name"]
    _launch_instance_off_image(token, name, source_uuid, size_entry["alias"], alloc_src["uuid"], project["uuid"], identity["uuid"])

def _launch_instance_off_image(token, name, source_uuid, size_alias, alloc_src_uuid, project_uuid, identity_uuid):
    try:
        headers = {}
        headers["Content-Type"] = "application/json"

        url = "https://atmo.cyverse.org/api/v2/instances"

        data = {}
        data["source_alias"] = "320b6a20-38eb-47a6-a33c-1216b1aa3399"
        data["size_alias"] = size_alias
        data["allocation_source_id"] = alloc_src_uuid
        data["name"] = name
        data["scripts"] = []
        data["project"] = project_uuid
        data["identity"] = identity_uuid

        json_obj = atmo_post_request(url, token=token, data=data, additional_header=headers)

        json_formatted_str = json.dumps(json_obj, indent=2)
        print(json_formatted_str)
    except requests.exceptions.HTTPError:
        print("Fail to launch instance with the specified image")
        exit(1)

def reboot_instance(token, instance_json):
    try:
        headers = {}
        headers["Host"] = "atmo.cyverse.org"
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://atmo.cyverse.org/api/v1"
        url += "/provider/" + instance_json["provider"]["uuid"]
        url += "/identity/" + instance_json["identity"]["uuid"]
        url += "/instance/" + instance_json["uuid"]
        url += "/action"

        data = {}
        data["action"] = "reboot"
        data["reboot_type"] = "HARD"

        resp = requests.post(url, headers=headers, json=data)
        resp.raise_for_status()

        json_obj = json.loads(resp.text)
        json_formatted_str = json.dumps(json_obj, indent=2)
        print(json_formatted_str)
    except requests.exceptions.HTTPError:
        print("Fail to list all the volumes")
        exit(1)
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        exit(1)

def delete_instance(token, instance_json):
    try:
        headers = {}
        headers["Host"] = "atmo.cyverse.org"
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://atmo.cyverse.org/api/v1"
        url += "/provider/" + instance_json["provider"]["uuid"]
        url += "/identity/" + instance_json["identity"]["uuid"]
        url += "/instance/" + instance_json["uuid"]

        resp = requests.delete(url, headers=headers)
        resp.raise_for_status()

        json_obj = json.loads(resp.text)
        json_formatted_str = json.dumps(json_obj, indent=2)
        print(json_formatted_str)
    except requests.exceptions.HTTPError:
        print("Fail to list all the volumes")
        exit(1)
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        exit(1)

def list_contains(l, field, value):
    for entry in l:
        if entry[field] == value:
            return entry
    return False

def atmo_get_request(url, token="", additional_header={}):
    try:
        headers = additional_header
        headers["Host"] = "atmo.cyverse.org"
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        if token:
            headers["Authorization"] = "TOKEN " + token

        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        json_obj = json.loads(resp.text)
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        exit(1)
    return json_obj

def atmo_post_request(url, token="", data={}, additional_header={}):
    try:
        headers = additional_header
        headers["Host"] = "atmo.cyverse.org"
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Content-Type"] = "application/json"
        if token:
            headers["Authorization"] = "TOKEN " + token

        resp = requests.post(url, headers=headers, json=data)
        json_obj = json.loads(resp.text)
        resp.raise_for_status()
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        exit(1)
    return json_obj

main()


