
## Helper Scripts

### Use on Jetstream

Script default is to use Cyverse Atmosphere as the target platform, if Jetstream is the target, pass `--jetstream` flag on the command line when running the following scripts.

### `cleanup_account_resource.py`

#### Summary:

Free up all the resources allocated by 1 or more accounts, like instances, volumes, and projects (the default project will be left in place)

#### Options:

| | |
|-|-|
`--username`    | pass the username, password will be prompted for a single account
`--csv`         | pass in a csv file containing credentials (username & password, or token) of accounts, without `--token`, it will look for username & password
`--token`       | uses access token instead of username & password, default to enable with `--jetstream`, token will be prompted if used for a single account (without `--csv`)
`--jetstream`   | target Jetstream cloud instead of Cyverse Atmosphere
`--cyverse`     | target Cyverse Atmosphere (default)

#### Descriptions:

For a single account, use the `--username` option or the `--token` option, script will prompt for password or access token

For use with more than 1 accounts, use the `--csv` option without `--token`, the script will only look for `username` and `password` field;
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

If a default project (project with the same name as username) does not exist, it will be created.

### `batch_launch_instance.py`

#### Summary:

Launched instance off image for multiple accounts.

#### Options:

| | |
|-|-|
`--csv`         | pass in a csv file containing credentials (username and password) of accounts
`--token`       | uses access token instead of username & password, default to enable with `--jetstream`
`--dont-wait`   | script will not wait for the instance launched to become fully active (status: active, activity: N/A), by default script will wait for the instance to be fully active
`--jetstream`   | target Jetstream cloud instead of Cyverse Atmosphere
`--cyverse`     | target Cyverse Atmosphere (default)

#### Descriptions:

Script only takes input via a csv file, provided by `--csv` option.

Failure during the launch of an instance does not terminate the script, the script will proceed to launch other instances,
or wait for instance to be active.

By default the script will wait for all instances launched to become fully active (status: active, activity: N/A),
but will timeout 30min after launched and report as "failed to become fully active in time"

Instance will be

- launched under the project with the same name as the username (1st found),

- launched using the allocation source with the same name as username (1st found),
  or specified the name of the allocation source in csv file with "allocation source" field

- launched using the identity with the same username (1st found),

- launched using the same name as the image name

Example csv
```csv
username,password,image,image version,instance size
cyverse_us_01,some_password,https://atmo.cyverse.org/application/images/1552,2.0,tiny1
```
```csv
token,image,image version,instance size
this_is_an_access_token,https://use.jetstream-cloud.org/application/images/717,1.27,m1.tiny
```
```csv
token,image,image version,instance size,allocation source
this_is_an_access_token,https://use.jetstream-cloud.org/application/images/717,1.27,m1.tiny,TG-XXX999999
```

### `batch_update_allocation.py`

#### Summary:

Update the allocation unit limit for multiple accounts as admin. By default AU count lower than current one is not applied.

#### Options:

| | |
|-|-|
`--csv`         | pass in a csv file containing username and target allocation unit count
`--token`       | pass a access token of an admin account
`--jetstream`   | target Jetstream cloud instead of Cyverse Atmosphere
`--cyverse`     | target Cyverse Atmosphere (default)
`--force-set`   | force setting target AU count, even if target is lower than current


#### Descriptions:

Example csv
```
username,alloc_unit_count
cyverse_us_01,500
```

