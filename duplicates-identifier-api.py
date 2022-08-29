
from __future__ import absolute_import
import argparse
from datetime import datetime
import itertools
import logging
import logging.config
import os
import signal
import sys

from dedupe.audit import DuplicatePackageLog, RemovedPackageLog
from dedupe.ckan_api import CkanApiClient
from dedupe.deduper import Deduper

logging.basicConfig(stream=sys.stdout, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')
log = logging.getLogger('dedupe')
log.setLevel(logging.INFO)

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

    parser = argparse.ArgumentParser(description='Detects and removes duplicate packages on '
                                     'data.gov. By default, duplicates are detected but not '
                                     'actually removed.')
    parser.add_argument('--api-key', default=os.getenv('CKAN_API_KEY', None), help='Admin API key')
    parser.add_argument('--api-url', default='https://catalog-prod-admin-datagov.app.cloud.gov',
                        help='The API base URL to query')
    parser.add_argument('--api-read-url', default=None,
                        help='The API base URL to query read-only info, for faster processing')
    parser.add_argument('--commit', action='store_true',
                        help='Treat the API as writeable and commit the changes.')
    parser.add_argument('--newest', action='store_true',
                        help='Keep the newest dataset and remove older ones (default keeps oldest)')
    parser.add_argument('--reverse', action='store_true',
                        help='Reverse the order of ids to parse (for running with another script in parallel)')
    parser.add_argument('--update-name', action='store_true',
                        help=('Update the name of the kept package to be the standard shortest name, '
                              'whether that was the duplicate package name or the to be kept package name.'))
    parser.add_argument('--debug', action='store_true',
                        help='Include debug output from urllib3.')
    parser.add_argument('--run-id', default=datetime.now().strftime('%Y%m%d%H%M%S'),
                        help='An identifier for a single run of the deduplication script.')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Include verbose log output.')
    parser.add_argument('organization_name', nargs='*',
                        help='Names of the organizations to deduplicate.')
    parser.add_argument('--geospatial', action='store_true',
                        help='If the organization has geospatial metadata that should be de-duped')

    args = parser.parse_args()

    if args.verbose:
        log.setLevel(logging.DEBUG)

    if args.debug:
        logging.getLogger('urllib3').setLevel(logging.DEBUG)

    dry_run = not args.commit
    if dry_run:
        log.info('Dry-run enabled')

    identifier_type = 'guid' if args.geospatial else 'identifier'

    log.info('run_id=%s', args.run_id)
    ckan_api = CkanApiClient(args.api_url,
                             args.api_key,
                             dry_run=dry_run,
                             identifier_type=identifier_type,
                             api_read_url=args.api_read_url,
                             reverse=args.reverse)

    duplicate_package_log = DuplicatePackageLog(api_url=args.api_url, run_id=args.run_id)
    removed_package_log = RemovedPackageLog(run_id=args.run_id)

    # Setup signal handlers
    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)

    log.info('Using api=%s', args.api_url)

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
            run_id=args.run_id,
            oldest=not args.newest,
            update_name=args.update_name,
            identifier_type=identifier_type)
        deduper.dedupe()


if __name__ == "__main__":
    run()
