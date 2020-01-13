
## Helper Scripts

### Use on Jetstream

script default to use Cyverse Atmosphere as the target platform, if Jetstream is the target platform, pass `--jetstream` flag on the command line when running the following scripts.

### `cleanup_account_resource.py`

#### Summary:

Free up all the resources allocated by 1 or more accounts, like instances, volumens, and projects (the default project will be left inplace)

#### Options:

`--username` pass the username of account, password will be prompted, for a single account

`--csv` pass in a csv file containing credential (username & password, or token) of accounts, without `--token`, it will look for username & password

`--token` uses access token instead of username & password, default to enable with `--jetstream`, token will be prompted if used for a single account (without `--csv`)

`--jetstream` target Jetstream cloud instead of Cyverse Atmosphere

`--cyverse` target Cyverse Atmosphere (default)

#### Descriptions:

For a single account, use the `--username` option or the `--token` option, script will prompt for password or access token

For use with more than 1 accounts, use the `--csv` option, without `--token`, the script will only look for `username` and `password` field;
e.g.
```csv
username,password
cyverse_us_01,some_password
```
with `--token`, script will only look for `token` field.
```csv
token
this_is_an_access_token
```

If a default project (project with the same name as username) do not exist, it will be created.

### `batch_launch_instance.py`

#### Summary:

Launched instance off image for 1 or more account.

#### Options:

`--username` pass the username of account, password will be prompted, for a single account

`--csv` pass in a csv file containing credential (username and password) of accounts

`--dont-wait` script will not wait for the instance launched to become fully active (status: active, activity: N/A), by default, script will wait for the instance to be fully active

#### Descriptions:

Instance will be

- launched under the project with the same name as the username (1st found),

- launched using the allocation source with the same name as username (1st found),

- launched using the identity with the same username (1st found),

- launched using the same name as the image name

Example csv
```csv
username,password,image,image version,instance size
cyverse_us_01,some_password,https://atmo.cyverse.org/application/images/1552,2.0,tiny1
```
