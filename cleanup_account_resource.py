#!/usr/bin/env python

import requests
import json
import csv
import argparse
import sys
import getpass

"""
Clean up all the resources(instances, volumes) allocated by 1 or more accounts.

For 1 account one could use the --username cmd option, and enter the password when prompt.
For >= 1 accounts use the --csv cmd option to pass in a csv file that contains "username" and "password" field.

For jetstream, only token is accepted

"""
cyverse_base_url = "atmo.cyverse.org"
jetstream_base_url = "use.jetstream-cloud.org" 
global api_base_url
api_base_url = cyverse_base_url

def main():
    # read accounts credentials
    account_list = parse_args()

    for row_index, account in enumerate(account_list):
        try:
            # obtain token
            if "token" in account:
                token = account["token"]    # from csv
            else:
                token = login(account["username"], account["password"])

            # delete all links
            all_links = list_links_of_user(token)
            for link in all_links:
                print(link["title"])
                print(link["link"])
                print(link["id"])
                delete_link(token, link["id"])

            # deattach all the volumes
            deattached = []
            all_volumes = list_volume_of_user(token)
            for vol in all_volumes:
                if deattach_volume(token, vol["uuid"]):
                    deattached.append(vol["uuid"])

            # wait for all volumes to deattach
            while deattached:
                for vol_uuid in list(deattached):
                    # remove from list if finished deattaching
                    if not vol_attached_to(token, vol_uuid):
                        deattached.remove(vol_uuid)

            # delete all instances
            all_instances = list_instance_of_user(token)
            for instance in all_instances:
                print(instance["name"])
                print(instance["uuid"])
                delete_instance(token, instance)

            # delete all volumes
            all_volumes = list_volume_of_user(token)
            for vol in all_volumes:
                print(vol["name"])
                print(vol["uuid"])
                delete_volume(token, vol)

            # find the default project 
            all_projects = list_project_of_user(token)
            username = account_username(token)
            default_project = False
            for project in all_projects:
                if project["name"] == username:
                    default_project = project
                    break
            # create a default projects if do not exist
            if not default_project:
                default_project = create_project(token, username, "", username)

            # delete all extra projects
            for project in all_projects:
                if project["uuid"] != default_project["uuid"]:
                    delete_project(token, project)

        except:
            print("Errors when freeing up resources for account in row {}".format(row_index))
            raise
            exit(1)

def parse_args():
    """
    Parsing cmd args

    Returns:
        a list of dict which contains the variable read from the csv or cmd line,
        could contains keys "username", "password", "token"
    """
    parser = argparse.ArgumentParser(description="Clean up all resources allocated by one or more accounts, use csv file for more than one account")
    parser.add_argument("--username", dest="username", type=str, help="username of the cyverse account, only specify 1 account")
    parser.add_argument("--csv", dest="csv_filename", type=str, help="filename of the csv file that contains credentials for all the accounts")
    parser.add_argument("--cyverse", dest="cyverse", action="store_true", help="Target platform: Cyverse Atmosphere (default)")
    parser.add_argument("--jetstream", dest="jetstream", action="store_true", help="Target platform: Jetstream")
    parser.add_argument("--token", dest="token", action="store_true", help="use access token instead of username & password, default for Jetstream")

    args = parser.parse_args()

    global api_base_url
    # target platform
    if args.jetstream:
        api_base_url = jetstream_base_url
        args.token = True   # Use token on Jetstream
    else:
        args.cyverse = True
        api_base_url = cyverse_base_url

    if args.username and args.token:
        print("Conflict option, --username and --token")
        parser.print_help()
        exit(1)

    if args.username and args.csv_filename:
        print("Conflict option, --username and --csv")
        parser.print_help()
        exit(1)

    if args.jetstream and args.username:
        print("Conflict option, --jetsream and --username")
        parser.print_help()
        exit(1)

    if args.username:
        account = {}
        account["username"] = args.username
        account["password"] = getpass.getpass()
        account_list = [account]
    elif args.csv_filename:
        account_list = read_account_from_csv(args.csv_filename, args.token)
    elif args.token:
        account = {}
        account["token"] = getpass.getpass("Access token: ")
        account_list = [account]
    else:
        print("Neither --username or --csv or --token is specified, one is needed\n")
        parser.print_help()
        exit(1)

    return account_list

def read_account_from_csv(filename, use_token):
    """
    Read out account credentials from a csv file

    Args:
        filename: filename of the csv file
        use_token: a boolean flag that determines whether or not token is used instead of username&password

    Returns:
        a list of dict, each dict contains credential about 1 account, like "username", "password", "token"
    """
    account_list = []

    with open(filename) as csvfile:
        csv_reader = csv.reader(csvfile)
        field = []

        token = False
        for row_index, row in enumerate(csv_reader):
            # find the username and password field
            if not field:
                field = row
                if use_token:
                    token_index = find_field(field, "token")
                else:
                    username_index = find_field(field, "username")
                    password_index = find_field(field, "password")
                continue # skip the 1st row

            if use_token:
                try:
                    account = {"token" : row[token_index]}
                    print(account["token"][:3] + "*****")
                except:
                    print("row {} missing token field".format(row_index))
                    exit(1)
            else:
                try:
                    account = row_to_account(row, username_index, password_index)
                    print_row(row, username_index, password_index)
                except:
                    print("row {} missing username or password field".format(row_index))
                    exit(1)
            account_list.append(account)

    return account_list

def find_field(all_fields, field_name):
    """
    Find the index of a field in the field row

    Args:
        all_fields: a list contains all field name from the header line of csv file
        field_name: the field name to look up

    Returns:
        the index of field_name in the list all_fields
    """
    for i, field in enumerate(all_fields):
        if field == field_name:
            return i
    print("No field called " + field_name)
    exit(1)

def row_to_account(row, username_index, password_index):
    """
    Convert a row to a dict with username and password as key

    Args:
        row: list of fields in a csv row
        username_index: index of username in the row
        password_index: index of password in the row
    Returns:
        a dict that contains field "username", "password"
    """
    account = {}
    account["username"] = row[username_index]
    account["password"] = row[password_index]

    return account

def print_row(row, username_index, password_index):
    """
    Print a row

    Args:
        row: list of fields in a csv row
        username_index: index of username in the row
        password_index: index of password in the row
    """
    password = "".join([ "*" for c in row[password_index] ])
    print("username: {} \t password: {}".format(row[username_index], password))

def login(username, password):
    """
    Obtain a temp authorization token with username and password

    Args:
        username: username of account
        password: password of account

    Returns:
        a temporary access token
    """
    try:
        resp = requests.get("https://de.cyverse.org/terrain/token", auth=(username, password))
        resp.raise_for_status()
        token = resp.json()['access_token']
    except requests.exceptions.HTTPError:
        print("Authentication failed, username: {}".format(username))
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise
    return token

def list_instance_of_user(token):
    """
    List all instances that a user has

    Args:
        token: access token of the account
    Returns:
        a list that contains all the instance (dict), parsed from json
    """
    try:
        headers = {}
        headers["Host"] = api_base_url
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v2/instances"
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        json_obj = json.loads(resp.text)
    except requests.exceptions.HTTPError:
        print("Fail to list all the instances")
        print(resp.text)
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise
    return json_obj["results"]

def list_project_of_user(token):
    """
    List all projects that a user has

    Args:
        token: access token of account
    Returns:
        a list of all the projects (dict), parsed from json
    """
    try:
        headers = {}
        headers["Host"] = api_base_url
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v2/projects"
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        json_obj = json.loads(resp.text)
    except requests.exceptions.HTTPError:
        print("Fail to list all the projects")
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise
    return json_obj["results"]

def list_volume_of_user(token):
    """
    List all volumes that a user has, both attached and deattached

    Args:
        token: access token of account
    Returns:
        a list of all volumes (dict) that user has
    """
    try:
        headers = {}
        headers["Host"] = api_base_url
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v2/volumes"
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        json_obj = json.loads(resp.text)
    except requests.exceptions.HTTPError:
        print("Fail to list all the volumes")
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise
    return json_obj["results"]

def get_volume(token, vol_uuid, provider_uuid, identity_uuid):
    """
    Get detail info of a volume, used V1 API.
    Contains info about the instance the volume is attached to if any.

    Args:
        token: access token of the account that owns the volume
        vol_uuid: uuid of the volume
        provider_uuid: uuid of the provider that the volume is at
        identity_uuid: uuid of the identity that the volume is created from
    Returns:
        parsed json result of the API
    """
    try:
        headers = {}
        headers["Host"] = api_base_url
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v1"
        url += "/provider/" + provider_uuid
        url += "/identity/" + identity_uuid
        url += "/volume/" + vol_uuid
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        json_obj = json.loads(resp.text)
    except requests.exceptions.HTTPError:
        print("Fail to list all the volumes")
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise
    return json_obj

def get_volume_v2(token, vol_uuid):
    """
    Get detail info of a volume, used V2 API.
    Does not include info about the attaching status, or the instance that volume is attached to if any

    Args:
        token: access token of the account that owns the volume
        vol_uuid: uuid of the volume to look up
    Returns:
        parsed json result of the API
    """
    try:
        headers = {}
        headers["Host"] = api_base_url
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v2"
        url += "/volumes/" + vol_uuid
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        json_obj = json.loads(resp.text)
    except requests.exceptions.HTTPError:
        print("Fail to list all the volumes")
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise
    return json_obj

def vol_attached_to(token, vol_uuid):
    """
    Check to see if a volume is attached or not, and which instance is it attached to

    Args:
        token: access token of the account that owns the volume
        vol_uuid: uuid of the volume to look up for
    Returns:
        None if the the volume is deattached, otherwise, a dict contains info of the instance the volume attached to
    """
    vol = get_volume_v2(token, vol_uuid)
    json_obj = get_volume(token, vol_uuid, vol["provider"]["uuid"], vol["identity"]["uuid"])
    return json_obj["attach_data"]

def deattach_volume(token, vol_uuid):
    """
    Deattach the volume, if the volume is attached.
    If the volume is already deattached, do nothing

    Args:
        token: access token of the account that owns the volume
        vol_uuid: uuid of the volume to perform the deattach on
    Returns:
        boolean flag of whether or not the deattach is performed
    """
    vol = get_volume_v2(token, vol_uuid)
    vol_v2 = get_volume(token, vol_uuid, vol["provider"]["uuid"], vol["identity"]["uuid"])
    if "attach_data" in vol_v2 and vol_v2["attach_data"]:
        _deattach_volume(token, vol_uuid, vol["provider"]["uuid"], vol["identity"]["uuid"], vol_v2["attach_data"]["instance_alias"])
        return True
    else:
        print("Volume {} not attached to any instance".format(vol_uuid))
        return False

def _deattach_volume(token, vol_uuid, provider_uuid, identity_uuid, instance_uuid):
    """
    Perform the deattch of a volume.
    It is an action performed on the instance rahter than the volume.

    Args:
        token: access token of the account that owns the volume
        vol_uuid:
        provider_uuid: uuid of the provider that the instance is created with
        identity_uuid: uuid of the identity that the instance is created with
        instance_uuid: uuid of the instance that the volume is attached to
    Returns:
        parsed json result of the response
    """
    try:
        headers = {}
        headers["Host"] = api_base_url
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v1"
        url += "/provider/" + provider_uuid
        url += "/identity/" + identity_uuid
        url += "/instance/" + instance_uuid
        url += "/action"

        data = {}
        data["action"] = "detach_volume"
        data["volume_id"] = vol_uuid

        resp = requests.post(url, headers=headers, json=data)
        resp.raise_for_status()
        json_obj = json.loads(resp.text)
    except requests.exceptions.HTTPError:
        print("Fail to list all the volumes")
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise
    return json_obj

def reboot_instance(token, instance_json):
    """
    Reboot instance

    Args:
        token: access token of the account that owns the instance
        instance_json: json obj of an instance, comes from list_instances_of_user(), needs "provider", "identity", "uuid" fields
    """
    try:
        headers = {}
        headers["Host"] = api_base_url
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v1"
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
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise

def delete_instance(token, instance_json):
    """
    Delete an instance

    Args:
        token: access token of an instance
        instance_json: json obj of an instance, comes from list_instances_of_user(), needs "provider", "identity", "uuid" fields
    """
    try:
        headers = {}
        headers["Host"] = api_base_url
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v1"
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
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise

def delete_project(token, project_json):
    """
    Delete a project

    Args:
        token: access token of an account that owns the project
        project_json: json obj of an project, needs "id" field
    """
    try:
        headers = {}
        headers["Host"] = api_base_url
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v2"
        url += "/projects/" + str(project_json["id"])

        resp = requests.delete(url, headers=headers)
        resp.raise_for_status()

        if len(resp.text) > 0:
            json_obj = json.loads(resp.text)
            json_formatted_str = json.dumps(json_obj, indent=2)
            print("Deleted instance")
            print(json_formatted_str)
    except requests.exceptions.HTTPError:
        print("Fail to list all the volumes")
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise

def delete_volume(token, volume_json):
    """
    Delete a volume

    Args:
        token: access token of the account that owns the volume
        volume_json: json obj of the volume, needs "provider", "identity", "uuid" fields
    """
    try:
        headers = {}
        headers["Host"] = api_base_url
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v1"
        url += "/provider/" + volume_json["provider"]["uuid"]
        url += "/identity/" + volume_json["identity"]["uuid"]
        url += "/volume/" + volume_json["uuid"]

        resp = requests.delete(url, headers=headers)
        resp.raise_for_status()

        json_obj = json.loads(resp.text)
        json_formatted_str = json.dumps(json_obj, indent=2)
        print("Deleted volume")
        print(json_formatted_str)
    except requests.exceptions.HTTPError:
        print("Fail to list all the volumes")
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise

def create_project(token, name, description, owner):
    """
    Create a project with given name

    Args:
        token: access token of the account that owns the project
        name: name of the project to be created
        description: description of the project to be created
        owner: owner of the project
    Returns:
        return the json obj of the response
    """
    try:
        headers = {}
        headers["Host"] = api_base_url
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v2/projects"

        data = {}
        data["name"] = name
        data["description"] = description
        data["owner"] = owner

        resp = requests.post(url, headers=headers, data=data)
        resp.raise_for_status()

        json_obj = json.loads(resp.text)
        print("Project created")
        return json_obj
    except requests.exceptions.HTTPError:
        print("Fail to create project")
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise

def list_links_of_user(token):
    """
    List all the links of an account

    Args:
        token: access token of the account
    Returns:
        a json obj from the response
    """
    try:
        headers = {}
        headers["Host"] = "atmo.cyverse.org"
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        if token:
            headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v2/links"
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        json_obj = json.loads(resp.text)

        return json_obj["results"]
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise
    except requests.HTTPError:
        print("Fail to list all links of user")
        raise

def delete_link(token, link_uuid):
    """
    Delete an external link

    Args:
        token: access token of the account that owns the link
    Returns:
        None
    """
    try:
        headers = {}
        headers["Host"] = "atmo.cyverse.org"
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        if token:
            headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v2/links/" + link_uuid
        resp = requests.delete(url, headers=headers)
        resp.raise_for_status()
        if resp.text:
            json_obj = json.loads(resp.text)
            return json_obj
        return None
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise
    except requests.HTTPError:
        print("Fail to delete link")
        raise

def account_username(token):
    """
    Get username of an account using token

    Args:
        token: access token of the account
    Returns:
        the username
    """
    profile = user_profile(token)
    return profile["username"]

def user_profile(token):
    """
    Get the user's profile

    Args:
        token: access token of the account
    Returns:
        json obj of the response
    """
    try:
        headers = {}
        headers["Host"] = api_base_url
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://" + api_base_url + "/api/v1/profile"

        resp = requests.get(url, headers=headers)
        resp.raise_for_status()

        json_obj = json.loads(resp.text)
        json_formatted_str = json.dumps(json_obj, indent=2)

        return json_obj
    except requests.exceptions.HTTPError:
        print("Fail to list profile")
        raise
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        raise



if __name__ == '__main__':
    main()


