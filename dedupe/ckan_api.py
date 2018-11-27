
import requests

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
        url = '%s/api%s' % (self.api_url, path)

        # Set a 60 second timeout for connections
        kwargs.setdefault('timeout', 60)

        response = self.client.request(method, url, **kwargs)

        assert response.status_code == 200
        assert response.json()['success']

        return response

    def get(self, path, **kwargs):
        return self.request('GET', path, **kwargs)

    def get_oldest_dataset(self, harvest_identifier):
        response = self.get('/action/package_search', params={
            'q': 'identifier:"%s"' % harvest_identifier,
            'fq': 'type:dataset',
            'sort': 'metadata_created desc',
            'rows': 1,
            })

        return response.json()['result']['results'][0]

    def get_newest_dataset(self, harvest_identifier):
        response = self.get('/action/package_search', params={
            'q': 'identifier:"%s"' % harvest_identifier,
            'fq': 'type:dataset',
            'sort': 'metadata_created asc',
            'rows': 1,
            })

        return response.json()['result']['results'][0]

    def get_harvester_identifiers(self, organization_name):
        response = self.get('/3/action/package_search', params={
            'q': 'organization:%s' % organization_name,
            'facet.field': '["identifier"]',
            'facet.limit': -1,
            'facet.mincount': 2,
            })

        return response.json()['result']['search_facets']['identifier']['items']


    def get_dataset_count(self, organization_name, harvest_identifier):
        response = self.get('/action/package_search', params={
            'q': 'identifier:"%s"' % harvest_identifier,
            'fq': 'type:dataset',
            'sort': 'metadata_created desc',
            'rows': 0,
            })

        return response.json()['result']['count']

    def get_datasets(self, organization_name, harvest_identifier, start=0, rows=1000):
        response = self.get('/action/package_search', params={
            'q': 'identifier:"%s"' % harvest_identifier,
            'start': start,
            'rows': rows,
            })

        return response.json()['result']['results']

    def get_organizations(self):
        response = self.get('/action/package_search?q=source_type:datajson&rows=1000')
        return response.json()['result']['results']

    def remove_package(self, package):
        self.request('DELETE', '/action/package_search', params={
            'id': package['id'],
        })
