# datagov-dedupe

Delete duplicate packages on data.gov.


## Usage

Install the dependencies.

    $ pip install -r requirements.txt

Deduplicate packages for a specific organization.

    $ python duplicates-identifier-api.py [organization-name]

Scan all organizations and dedupe packges for each.

    $ python duplicates-identifier-api.py

View the full help documentation with `--help`.

```
$ python duplicates-identifier-api.py -h
usage: duplicates-identifier-api.py [-h] [--api-key API_KEY]
                                    [--api-url API_URL] [--dry-run]
                                    [organization_name [organization_name ...]]

Removes duplicate packages on data.gov.

positional arguments:
  organization_name  Names of the organizations to deduplicate.

optional arguments:
  -h, --help         show this help message and exit
  --api-key API_KEY  Admin API key
  --api-url API_URL  The API base URL to query
  --dry-run          Treat the API as read-only and make no changes.
```

## Development

Install the latest dependencies.

    $ pip install -r requirements.in
