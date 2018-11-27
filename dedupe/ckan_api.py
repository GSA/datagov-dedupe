
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
        response = self.client.request(method, url, **kwargs)

        assert response.status_code == 200
        assert response.json()['success']

        return response

    def get(self, path, **kwargs):
        return self.request('GET', path, **kwargs)

    def get_oldest_dataset(self, harvest_identifier):
        response = self.request('GET', '/action/package_search', params={
            'q': 'identifier:"%s"' % harvest_identifier,
            'fq': 'type:dataset',
            'sort': 'metadata_created desc',
            'rows': 1,
            })

        return response.json()['result']['results'][0]

    def get_newest_dataset(self, harvest_identifier):
        response = self.request('GET', '/action/package_search', params={
            'q': 'identifier:"%s"' % harvest_identifier,
            'fq': 'type:dataset',
            'sort': 'metadata_created asc',
            'rows': 1,
            })

        return response.json()['result']['results'][0]
