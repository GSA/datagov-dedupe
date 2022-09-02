from __future__ import absolute_import
import unittest

import mock

from ..ckan_api import DryRunException, CkanApiClient, CkanApiCountException


class StubResponse(object):
    '''
    A fake Response object.
    '''
    def __init__(self, data=None):
        self.data = data if data else dict()

    def json(self):
        return self.data


class TestCkanApiClient(unittest.TestCase):
    def test_request_dry_run(self):
        with self.assertRaises(DryRunException):
            api = CkanApiClient('http://test', 'api-key-abc', dry_run=True)
            api.request('POST', '/action/test')

    def test_remove_package(self):
        with mock.patch.object(CkanApiClient, 'request', return_value=None) as mock_request:
            api = CkanApiClient('http://test', 'api-key-abc', dry_run=False)
            api.remove_package('package-123')

        mock_request.assert_called_with('POST', mock.ANY, json=dict(id='package-123'))

    def test_remove_package_dry_run(self):
        with mock.patch.object(CkanApiClient, 'request', return_value=None) as mock_request:
            api = CkanApiClient('http://test', 'api-key-abc', dry_run=True)
            api.remove_package('package-123')

        mock_request.assert_not_called()

    def test_get_oldest_dataset_count_exception(self):
        invalid_count_response = {
            'result': {
                'count': 2,
                'results': []
            },
        }

        with mock.patch.object(CkanApiClient, 'request', return_value=StubResponse(invalid_count_response)):
            api = CkanApiClient('http://test', 'api-key-abc')
            with self.assertRaises(CkanApiCountException):
                api.get_dataset('test-organization', 'package-123', is_collection=False)
