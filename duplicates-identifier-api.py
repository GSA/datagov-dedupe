
import argparse
import logging
import os
import time

from dedupe import CkanApiClient, Deduper

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('dedupe')


def get_org_list(ckan):
    organizations_list = []

    log.debug('Fetching organizations...')
    org_list = ckan.get("/action/package_search?q=source_type:datajson&rows=1000")
    org_list = org_list.json()['result']['results']

    for organization in org_list:
        if organization['organization']['name'] not in organizations_list:
            organizations_list.append(organization['organization']['name'])

    log.debug('Found organizations count=%d', len(organizations_list))
    return organizations_list

def remove_package(ckan, package):
    log.info('Removing duplicate package=%s', package['id'])

def dedupe_organization(ckan, org_name):
    '''
        Get the datasets on data.gov that we have for the organization
    '''

    deduper = Deduper(org_name, ckan)

    # get list of harvesters for the organization
    log.debug('Fetching harvesters for organization=%s', org_name)
    response = ckan.get('/3/action/package_search', params={
        'q': 'organization:%s' % org_name,
        'facet.field': '["identifier"]',
        'facet.limit': -1,
        'facet.mincount': 2,
        })

    harvester_identifiers = response.json()['result']['search_facets']['identifier']['items']
    log.info('Found harvest identifiers for organization=%s count=%d', org_name, len(harvester_identifiers))

    duplicate_count = 0
    for harvester in harvester_identifiers:
        identifier = harvester['name']
        log.debug('Fetching count of datasets for harvest organization=%s identifier=%s',
                 org_name, identifier)
        dataset_list = ckan.get('/action/package_search', params={
            'q': 'identifier:"%s"' % identifier,
            'fq': 'type:dataset',
            'sort': 'metadata_created desc',
            'rows': 0,
            })

        harvest_data_count = dataset_list.json()['result']['count']
        log.info('Found packages for organization=%s identifier=%s count=%d', org_name, identifier, harvest_data_count)

        if harvest_data_count <= 1:
            log.debug('No duplicates found for organization=%s identifier=%s', org_name, identifier)
            continue

        # We want to keep the most recent dataset, but there is a name conflict
        # with the oldest dataset. Rename the oldest dataset so that we can
        # give it's name to the newest
        new_dataset = deduper.replace_oldest_dataset_with_newest(ckan, org_name, identifier)

        # Now we can collect the datasets for removal
        def get_datasets(total, rows=1000):
            start = 0
            while start < total:
                log.debug(
                    'Batch fetching datasets for harvest organization=%s identifier=%s offset=%d rows=%d total=%d',
                    org_name, identifier, start, rows, total)
                dataset_list = ckan.get('/action/package_search', params={
                    'q': 'identifier:"%s"' % identifier,
                    'start': start,
                    'rows': rows,
                    })
                start += rows

                for dataset in dataset_list.json()['result']['results']:
                    yield dataset

        for dataset in get_datasets(harvest_data_count):
            if dataset['organization']['name'] != org_name:
                log.warn('Dataset harvested by organization but not part of organization organization=%s identifier=%s pkg_org_name=%s pkg_name=%s',
                         org_name, identifier, dataset['organization']['name'], dataset['name'])
                continue

            if dataset['id'] == new_dataset['id']:
                log.debug('This package is the most recent, not removing package=%s', dataset['id'])
                continue

            remove_package(ckan, dataset)
            duplicate_count += 1

    log.info('Summary orgaization=%s duplicate_count=%d', org_name, duplicate_count)


def rename_dataset_for_purge(ckan, dataset):
    pass


def remove_duplicate_datasets(ckan, duplicate_datasets):
    pass


def run():
    '''
        This code for getting the list of organizations and duplicate duplicate data sets
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', default=os.getenv('CKAN_API_KEY', None), help='Admin API key')
    parser.add_argument('--api-url', default='https://admin-catalog.data.gov',
                        help='The API base URL to query')
    parser.add_argument('--dry-run', action='store_true',
                        help='Treat the API as read-only and make no changes.')
    parser.add_argument('organization_name', nargs='*',
                        help='The name of the organization.')

    args = parser.parse_args()

    ckan = CkanApiClient(args.api_url, args.api_key)

    if args.organization_name:
        org_list = args.organization_name
    else:
        # get all organizations that have datajson harvester
        org_list = get_org_list(ckan)

    # get list of duplicate_datasets
    for organization in org_list:
        try:
            dedupe_organization(ckan, organization)
        except Exception as e:
            log.exception(e)
            # Continue with the next organization
            continue


if __name__ == "__main__":
    run()
