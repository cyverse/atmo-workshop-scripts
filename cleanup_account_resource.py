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

"""


def main():
    # read accounts credentials
    account_list = parse_args()

    for account in account_list:
        # obtain token
        token = login(account["username"], account["password"])

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
        account_list = read_account_from_csv(args.csv_filename)
    else:
        print("Neither --username or --csv is specified, one is needed\n")
        parser.print_help()
        exit(1)
    return account_list

def read_account_from_csv(filename):
    account_list = []

    with open(filename) as csvfile:
        csv_reader = csv.reader(csvfile)
        field = []

        for row_index, row in enumerate(csv_reader):
            # find the username and password field
            if not field:
                field = row
                username_index = find_field(field, "username")
                password_index = find_field(field, "password")
                continue # skip the 1st row

            print_row(row, username_index, password_index)

            try:
                account = row_to_account(row, username_index, password_index)
            except:
                print("row {} missing username or password field".format(row_index))
                raise
            account_list.append(account)

    return account_list

def find_field(all_fields, field_name):
    """
    Find the index of a field in the field row
    """
    for i, field in enumerate(all_fields):
        if field == field_name:
            return i
    print("No field called " + field_name)
    exit(1)

def row_to_account(row, username_index, password_index):
    account = {}
    account["username"] = row[username_index]
    account["password"] = row[password_index]

    return account

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
        headers = {}
        headers["Host"] = "atmo.cyverse.org"
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        resp = requests.get("https://atmo.cyverse.org/api/v2/instances", headers=headers)
        resp.raise_for_status()
        json_obj = json.loads(resp.text)
    except requests.exceptions.HTTPError:
        print("Fail to list all the instances")
        exit(1)
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        exit(1)
    return json_obj["results"]

def list_project_of_user(token):
    try:
        headers = {}
        headers["Host"] = "atmo.cyverse.org"
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        resp = requests.get("https://atmo.cyverse.org/api/v2/projects", headers=headers)
        resp.raise_for_status()
        json_obj = json.loads(resp.text)
    except requests.exceptions.HTTPError:
        print("Fail to list all the projects")
        exit(1)
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        exit(1)
    return json_obj["results"]

def list_volume_of_user(token):
    try:
        headers = {}
        headers["Host"] = "atmo.cyverse.org"
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        resp = requests.get("https://atmo.cyverse.org/api/v2/volumes", headers=headers)
        resp.raise_for_status()
        json_obj = json.loads(resp.text)
    except requests.exceptions.HTTPError:
        print("Fail to list all the volumes")
        exit(1)
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        exit(1)
    return json_obj["results"]

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

def delete_project(token, project_json):
    try:
        headers = {}
        headers["Host"] = "atmo.cyverse.org"
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://atmo.cyverse.org/api/v2"
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
        exit(1)
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        exit(1)

def delete_volume(token, volume_json):
    try:
        headers = {}
        headers["Host"] = "atmo.cyverse.org"
        headers["Accept"] = "application/json;q=0.9,*/*;q=0.8"
        headers["Authorization"] = "TOKEN " + token

        url = "https://atmo.cyverse.org/api/v1"
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
        exit(1)
    except json.decoder.JSONDecodeError:
        print("Fail to parse response body as JSON")
        exit(1)



main()


