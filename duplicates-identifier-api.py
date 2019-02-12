
import argparse
from datetime import datetime
import itertools
import logging
import logging.config
import os
import signal
import time

from dedupe.audit import DuplicatePackageLog, RemovedPackageLog
from dedupe.ckan_api import CkanApiClient
from dedupe.deduper import Deduper

logging.config.fileConfig('logging.ini')
log = logging.getLogger('dedupe')

# Define module-level context for signal handling
stopped = False
deduper = None


def get_org_list(ckan):
    log.debug('Fetching organizations...')
    organizations_list = ckan.get_organizations()

    log.debug('Found organizations count=%d', len(organizations_list))
    return organizations_list


def cleanup(signum, frame):
    global deduper, stopped
    log.warning('Stopping any in-progress dedupers...')
    stopped = True
    deduper.stop()


def run():
    '''
        This code for getting the list of organizations and duplicate duplicate data sets
    '''
    global deduper, stopped

    parser = argparse.ArgumentParser(description='Removes duplicate packages on data.gov.')
    parser.add_argument('--api-key', default=os.getenv('CKAN_API_KEY', None), help='Admin API key')
    parser.add_argument('--api-url', default='https://admin-catalog.data.gov',
                        help='The API base URL to query')
    parser.add_argument('--dry-run', action='store_true',
                        help='Treat the API as read-only and make no changes.')
    parser.add_argument('--run-id', default=datetime.now().strftime('%Y%m%d%H%M%S'),
                        help='An identifier for a single run of the deduplication script.')
    parser.add_argument('organization_name', nargs='*',
                        help='Names of the organizations to deduplicate.')

    args = parser.parse_args()

    log.info('run_id=%s', args.run_id)
    ckan_api = CkanApiClient(args.api_url, args.api_key, dry_run=args.dry_run)
    duplicate_package_log = DuplicatePackageLog(api_url=args.api_url, run_id=args.run_id)
    removed_package_log = RemovedPackageLog(run_id=args.run_id)

    # Setup signal handlers
    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)

    log.info('Using api=%s', args.api_url)

    if args.dry_run:
        log.info('Dry run enabled')

    if args.organization_name:
        org_list = args.organization_name
    else:
        # get all organizations that have datajson harvester
        org_list = get_org_list(ckan_api)

    log.info('Deduplicating organizations=%d', len(org_list))

    # Loop over the organizations one at a time
    count = itertools.count(start=1)
    for organization in org_list:
        if stopped:
            break

        log.info('Deduplicating organization=%s progress=%r',
                 organization, (next(count), len(org_list)))
        deduper = Deduper(
            organization,
            ckan_api,
            removed_package_log,
            duplicate_package_log,
            run_id=args.run_id)
        deduper.dedupe()


if __name__ == "__main__":
    run()
