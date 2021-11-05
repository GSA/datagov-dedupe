# datagov-dedupe

Detect and remove duplicate packages on data.gov.


## Usage

Install the dependencies.

    $ pipenv sync

Deduplicate packages for a specific organization.

    $ pipenv run python duplicates-identifier-api.py [organization-name]

Scan all organizations and dedupe packges for each.

    $ pipenv run python duplicates-identifier-api.py

View the full help documentation with `--help`.

```
$ pipenv run python duplicates-identifier-api.py --help
usage: duplicates-identifier-api.py [-h] [--api-key API_KEY]
                                    [--api-url API_URL] [--commit] [--debug]
                                    [--run-id RUN_ID] [--verbose]
                                    [--newest] [--update-name]
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
  --newest           Keep the newest dataset and remove older ones 
                     (by default the oldest is kept)
  --update-name      Update the name of the kept package to be the standard
                     shortest name, whether that was the duplicate package
                     name or the to be kept package name.
  --verbose, -v      Include verbose log output.
```


## Development

Install the latest dependencies.

    $ pip install -r requirements.in


## Running on staging

BSP staging uses a GSA internal SSL certificate. `requests` will fail to verify
the certificate with an SSLError because the GSA root certificate is not
included in requests' CA bundle. Instead, use the OS CA bundle, which already
has the GSA root certificate installed.

    $ export REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
    $ python duplicates-identifier-api.py ...
