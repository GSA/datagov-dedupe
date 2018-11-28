import unittest

import mock

from ..ckan_api import DryRunException, CkanApiClient

class TestCkanApiClient(unittest.TestCase):
    def test_request_dry_run(self):
        with self.assertRaises(DryRunException):
            api = CkanApiClient('http://test', 'api-key-abc', dry_run=True)
            api.request('POST', '/action/test')

    def test_remove_package(self):
        with mock.patch.object(CkanApiClient, 'request', return_value=None) as mock_request:
            api = CkanApiClient('http://test', 'api-key-abc')
            api.remove_package('package-123')

        mock_request.assert_called_with('DELETE', mock.ANY, params=dict(id='package-123'))

    def test_remove_package_dry_run(self):
        with mock.patch.object(CkanApiClient, 'request', return_value=None) as mock_request:
            api = CkanApiClient('http://test', 'api-key-abc', dry_run=True)
            api.remove_package('package-123')

        mock_request.assert_not_called()
