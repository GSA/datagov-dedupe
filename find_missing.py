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
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Include verbose log output.')
    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)
    
    ckan_api = CkanApiClient(args.api_url, 
                             "None")

    # datasets = [{"id": "819bfd1a-154b-4fea-94d8-13215637db51", "name": "fails"}, {"id": "5a659ccb-2929-4e3f-8412-8062d084d619", "name": "works"}]
    datasets = []
    broken_datasets = []
    start = 0
    rows = 1000
    ckan_datasets = ckan_api.get_duplicate_identifiers("doi-gov", False)
    # Get all datasets
    # while True:
        
        # time.sleep(1)
        # datasets = datasets + ckan_datasets.get('results')
        # if ckan_datasets.get('count') < start + rows:
        #     log.debug(f"Got all datasets: {ckan_datasets.get('count')}")
        #     break
        # log.debug(f"Got datasets: {start + rows}")
        # start = start + rows
    with open("broken_datasets.jsonld", "w") as output:
        for d in ckan_datasets:
            # Check if dataset page works/exists
            dataset_dupes = ckan_api.get_datasets("doi-gov", d)
            for dd in dataset_dupes:
                try:
                    # time.sleep(1)
                    ckan_api.check_dataset(dd.get('id'))
                    log.debug(f"{dd.get('name')} checks out")
                except CkanApiStatusException:
                    log.error(f"{dd.get('name')} does not exist")
                    broken_datasets.append(dd)
                    output.write(json.dumps(dd) + ("\n"))


if __name__ == "__main__":
    run()