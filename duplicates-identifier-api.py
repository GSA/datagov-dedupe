
import argparse
import logging
import os
import time

import requests

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('dedupe')


class CkanApiException(Exception):
    def __init__(self, message, response):
        super(CkanApiException, self).__init__(message)
        self.response = response


class CkanApiClient(object):
    '''
    Represents a client to query and submit requests to the CKAN API.
    '''

    def __init__(self, api_url, api_key):
        self.api_url = api_url
        self.client = requests.Session()
        self.client.headers.update(Authorization=api_key)

    def request(self, method, path, **kwargs):
        log.debug('Api request method=%s path=%s', method, path)
        url = '%s/api%s' % (self.api_url, path)
        response = self.client.request(method, url, **kwargs)
        if not response.status_code == 200 or not response.json()['success']:
            log.error('CKAN API request failed response=%s', response.content)
            raise CkanApiException('CKAN API request failed.', response)


        return response

    def get(self, path, **kwargs):
        return self.request('GET', path, **kwargs)


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


def get_dataset_list(ckan, org_name):
    '''
        Get the datasets on data.gov that we have for the organization
    '''

    dataset_keep = []
    org_harvest = []
    dataset_harvest_list = []
    totla_dup_data = []
    duplicates = []
    dup_log = []
    dup_json_log = []


    # get list of harvesters for the organization
    log.info('Fetching harvesters for organization=%s', org_name)
    org_harvest_tmp = ckan.get('/3/action/package_search', params={
        'q': 'organization:%s' % org_name,
        'facet.field': '["identifier"]',
        'facet.limit': -1,
        'facet.mincount': 2,
        })

    log.debug('%r', org_harvest_tmp)
    org_harvest_tmp = org_harvest_tmp.json()['result']['search_facets']['identifier']['items']
    log.info('Found harvest identifiers for organization=%s count=%d', org_name, len(org_harvest_tmp))

    for harvest in org_harvest_tmp:
        org_harvest.append(harvest['name'])

    for identifier in org_harvest:
        log.info('Fetching count of datasets for harvest organization=%s identifier=%s',
                 org_name, identifier)
        dataset_list = ckan.get('/action/package_search', params={
            'q': 'identifier:"%s"' % identifier,
            'fq': 'type:dataset',
            'sort': 'metadata_created+desc',
            'rows': 0,
            })

        harvest_data_count = dataset_list.json()['result']['count']
        start = 0
        rows = 1000
        while start <= harvest_data_count:
            try:
                log.info(
                    'Batch fetching datasets for harvest organization=%s identifier=%s offset=%d rows=%d',
                    org_name, identifier, start, rows)
                dataset_list = ckan.get('/action/package_search', params={
                    'q': 'identifier:"%s"' % identifier,
                    'start': start,
                    'rows': rows,
                    })
                dataset_harvest_list += dataset_list.json()['result']['results']
                start += rows
            except IndexError:
                time.sleep(20)
                continue
        if dataset_list.status_code == 200:
            try:
                dataset_count = dataset_list.json()['result']['count']
                data = dataset_list.json()['result']['results']

                if dataset_count > 1:
                    if data[dataset_count - 1]['id'] not in dataset_keep and \
                            data[dataset_count - 1]['organization']['name'] == org_name:
                        dataset_keep.append(data[dataset_count - 1]['id'])
                else:
                    dataset_keep.append(dataset_list['id'])

            except IndexError:
                continue

        for dataset_harvest in dataset_harvest_list:
            if dataset_harvest['id'] not in totla_dup_data and dataset_harvest['organization']['name'] == org_name:
                totla_dup_data.append(dataset_harvest['id'])

        duplicates += list(set(totla_dup_data) - set(dataset_keep))


    log.info('Found duplicate datasets organization=%s count=%d', org_name, len(duplicates))
    return duplicates

#def remove_duplicate_datasets(duplicate_datasets, o_name,sysadmin_api_key):
def remove_duplicate_datasets(duplicate_datasets):
    # conn_string = "' dbname='' user='' password=''"
    # print "Connecting to database\n ->%s" % (conn_string)

    # conn = psycopg2.connect(conn_string)
    #  cursor = conn.cursor()
    with open('duplicate_datasets__out.txt', 'a') as f:
    #with open('duplicate_datasets_' + org_name + '_out.txt', 'a') as f:
        for data in duplicate_datasets:
            #cursor.execute("update package set state='duplicate-removed' where name='" + data + "';")
            print >> f, "update package set state='duplicate-removed' where name='" + data + "';"
    #        conn.commit()

    # conn.close()

def run():
    '''
        This code for getting the list of organizations and duplicate duplicate data sets
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', default=os.getenv('CKAN_API_KEY', None), help='Admin API key')
    parser.add_argument('--api-url', default='https://admin-catalog.data.gov',
                        help='The API base URL to query')
    parser.add_argument('organization_id', default=None,
                        help='The API base URL to query')

    args = parser.parse_args()

    ckan = CkanApiClient(args.api_url, args.api_key)

    if args.organization_id:
        org_list = [args.organization_id]
    else:
        # get all organizations that have datajson harvester
        org_list = get_org_list(ckan)

    # get list of duplicate_datasets
    for organization in org_list:
        dataset_dup_tmp = get_dataset_list(ckan, organization)
        #remove_duplicate_datasets(dataset_dup_tmp)


if __name__ == "__main__":
    run()
