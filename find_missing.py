import argparse
import logging
import logging.config
import json
import sys
import time

logFormatter = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")
log = logging.getLogger('dedupe')
fileHandler = logging.FileHandler("output.log")
fileHandler.setFormatter(logFormatter)
log.addHandler(fileHandler)
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
log.addHandler(consoleHandler)
log.setLevel(logging.INFO)

from dedupe.ckan_api import CkanApiClient, CkanApiStatusException


def run():

    parser = argparse.ArgumentParser(description='Detects drift between SOLR and DB')
    parser.add_argument('--api-url', default='https://catalog-prod-admin-datagov.app.cloud.gov',
                        help='The API base URL to query')
    parser.add_argument('organization_name', nargs='*',
                        help='Names of the organizations to evaluate.')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Include verbose log output.')
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)
    
    ckan_api = CkanApiClient(args.api_url, "None")

    ckan_datasets = []
    broken_datasets = []
    

    if args.organization_name:
        org_list = args.organization_name
    else:
        # get all organizations that have datajson harvester
        org_list = ckan_api.get_organizations()
    for organization in org_list:
        start=0
        rows=1000
        org_datasets = []
        while len(org_datasets) % rows == 0:
            org_datasets += ckan_api.get_all_datasets(start=start, rows=rows, organization=organization)
            start += rows
        ckan_datasets += org_datasets
        log.info(f"Have {organization}'s datasets, {len(org_datasets)}")
        if len(ckan_datasets) > 20:
            break


    with open("broken_datasets.jsonld", "w") as output:
        for d in ckan_datasets:
            # Check if dataset page works/exists
            try:
                # time.sleep(1)
                ckan_api.check_dataset(d.get('id'))
                log.debug(f"{d.get('name')} checks out")
            except CkanApiStatusException:
                log.error(f"{d.get('name')} does not exist")
                broken_datasets.append(d)
                output.write(json.dumps(d) + ("\n"))


if __name__ == "__main__":
    run()