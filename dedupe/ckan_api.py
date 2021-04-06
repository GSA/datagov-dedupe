
import logging

import requests

log = logging.getLogger(__name__)

READ_ONLY_METHODS = ['GET']


class CkanApiException(Exception):
    def __init__(self, message, response):
        super(CkanApiException, self).__init__(message)
        self.response = response


class CkanApiFailureException(CkanApiException):
    '''
    CKAN API reported {success: false}. It should be okay to continue using the API.
    '''
    pass


class CkanApiStatusException(CkanApiException):
    '''
    CKAN API returned an unhealthy status code. This indicates something might
    not be working correctly with our configuration or the server could be
    having issues and we should not continue using the API in this state.
    '''
    pass

class CkanApiCountException(CkanApiException):
    '''
    CKAN API (and solr) returned a non-zero count, but no data. Could this be
    solr index corruption?
    '''
    pass


class DryRunException(Exception):
    '''
    Something happened during a dry-run execution that shouldn't have, like
    trying to write to the API.
    '''
    pass


class CkanApiClient(object):
    '''
    Represents a client to query and submit requests to the CKAN API.
    '''

    def __init__(self, api_url, api_key, dry_run=True):
        self.api_url = api_url
        self.dry_run = dry_run
        self.client = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        self.client.mount('https://', adapter)
        self.client.headers.update(Authorization=api_key)
        # Set the auth_tkt cookie to talk to admin API
        self.client.cookies = requests.cookies.cookiejar_from_dict(dict(auth_tkt='1'))

    def request(self, method, path, **kwargs):
        url = '%s/api%s' % (self.api_url, path)

        if self.dry_run and method not in READ_ONLY_METHODS:
            raise DryRunException('Cannot call method in dry_run method=%s' % method)

        # Set a 60 second timeout for connections
        kwargs.setdefault('timeout', 60)

        response = self.client.request(method, url, **kwargs)
        if response.status_code >= 400:
            log.error('Unsuccessful status code status=%d body=%s', response.status_code, response.content)
            raise CkanApiStatusException('Unsuccessful status code %d' % response.status_code, response)

        if not response.json().get('success', False):
            log.error('API failure status=%d body=%s', response.status_code, response.content)
            raise CkanApiFailureException('API reported failure', response)

        return response

    def get(self, path, **kwargs):
        return self.request('GET', path, **kwargs)

    def get_dataset(self, organization_name, identifier, is_collection, sort_order='asc'):
        filter_query = \
            'identifier:"%s" AND organization:"%s" AND type:dataset' % \
            (identifier, organization_name)
        if is_collection:
            filter_query = '%s AND collection_package_id:*' % filter_query

        rows = 1
        response = self.get('/action/package_search', params={
            'fq': filter_query,
            'sort': 'metadata_created ' + sort_order,
            'rows': rows,
            })

        results = response.json()['result']['results']

        if len(results) != rows:
            count = response.json()['result']['count']
            raise CkanApiCountException(
                'Query reported non-zero count but no data '
                'count=%(count)s results=%(results)s' % {
                    'count': count,
                    'results': len(results),
                },
                response)

        return results[0]

    def get_duplicate_identifiers(self, organization_name, is_collection):
        filter_query = 'organization:"%s" AND type:dataset' % organization_name
        if is_collection:
            filter_query = '%s AND collection_package_id:*' % filter_query

        response = self.get('/3/action/package_search', params={
            'fq': filter_query,
            'facet.field': '["identifier"]',
            'facet.limit': -1,
            'facet.mincount': 2,
            'rows': 0,
            })

        return \
            response.json()['result']['search_facets']['identifier']['items']

    def get_dataset_count(self, organization_name, identifier, is_collection):
        filter_query = \
            'identifier:"%s" AND organization:"%s" AND type:dataset' % \
            (identifier, organization_name)
        if is_collection:
            filter_query = '%s AND collection_package_id:*' % filter_query

        response = self.get('/action/package_search', params={
            'fq': filter_query,
            'sort': 'metadata_created desc',
            'rows': 0,
            })

        return response.json()['result']['count']

    def get_datasets(self, organization_name, identifier, start=0, rows=1000,
                     is_collection=False):
        filter_query = \
            'identifier:"%s" AND organization:"%s" AND type:dataset' % \
            (identifier, organization_name)
        if is_collection:
            filter_query = '%s AND collection_package_id:*' % filter_query

        response = self.get('/action/package_search', params={
            'fq': filter_query,
            'start': start,
            'rows': rows,
            })

        return response.json()['result']['results']

    def get_organizations(self):
        response = self.get('/action/organization_list')
        return response.json()['result']

    def remove_package(self, package_id):
        if self.dry_run:
            log.info('Not removing package in dry_run package=%s', package_id)
            return

        self.request('POST', '/action/package_delete', json={
            'id': package_id,
        })

    def update_package(self, package):
        if self.dry_run:
            log.info('Not updating package in dry_run package=%s', package['id'])
            return

        self.request('POST', '/action/package_update', json=package)
