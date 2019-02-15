# datagov-dedupe

Detect and remove duplicate packages on data.gov.


## Usage

Install the dependencies.

    $ pip install -r requirements.txt

Deduplicate packages for a specific organization.

    $ python duplicates-identifier-api.py [organization-name]

Scan all organizations and dedupe packges for each.

    $ python duplicates-identifier-api.py

View the full help documentation with `--help`.

```
$ python duplicates-identifier-api.py --help
usage: duplicates-identifier-api.py [-h] [--api-key API_KEY]
                                    [--api-url API_URL] [--commit] [--debug]
                                    [--run-id RUN_ID] [--verbose]
                                    [organization_name [organization_name ...]]

Detects and removes duplicate packages on data.gov. By default, duplicates are
detected but not actually removed.

positional arguments:
  organization_name  Names of the organizations to deduplicate.

optional arguments:
  -h, --help         show this help message and exit
  --api-key API_KEY  Admin API key
  --api-url API_URL  The API base URL to query
  --commit           Treat the API as writeable and commit the changes.
  --debug            Include debug output from urllib3.
  --run-id RUN_ID    An identifier for a single run of the deduplication
                     script.
  --verbose, -v      Include verbose log output.
```


## Development

Install the latest dependencies.

    $ pip install -r requirements.in
