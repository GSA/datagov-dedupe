
import argparse
import logging
import logging.config
import os
import time

from dedupe.audit import RemovedPackageLog
from dedupe.ckan_api import CkanApiClient
from dedupe.deduper import Deduper

logging.config.fileConfig('logging.ini')
log = logging.getLogger('dedupe')


def get_org_list(ckan):
    log.debug('Fetching organizations...')
    organizations_list = ckan.get_organizations()

    log.debug('Found organizations count=%d', len(organizations_list))
    return organizations_list


def run():
    '''
        This code for getting the list of organizations and duplicate duplicate data sets
    '''
    parser = argparse.ArgumentParser(description='Removes duplicate packages on data.gov.')
    parser.add_argument('--api-key', default=os.getenv('CKAN_API_KEY', None), help='Admin API key')
    parser.add_argument('--api-url', default='https://admin-catalog.data.gov',
                        help='The API base URL to query')
    parser.add_argument('--dry-run', action='store_true',
                        help='Treat the API as read-only and make no changes.')
    parser.add_argument('organization_name', nargs='*',
                        help='Names of the organizations to deduplicate.')

    args = parser.parse_args()

    ckan_api = CkanApiClient(args.api_url, args.api_key, dry_run=args.dry_run)
    removed_package_log = RemovedPackageLog()

    if args.dry_run:
        log.info('Dry run enabled')

    if args.organization_name:
        org_list = args.organization_name
    else:
        # get all organizations that have datajson harvester
        org_list = get_org_list(ckan_api)

    log.info('Deduplicating organizations=%d', len(org_list))

    # Loop over the organizations one at a time
    for organization in org_list:
        try:
            deduper = Deduper(organization, ckan_api, removed_package_log)
            deduper.dedupe()
        except Exception as e:
            log.exception(e)
            # Continue with the next organization
            continue


if __name__ == "__main__":
    run()
