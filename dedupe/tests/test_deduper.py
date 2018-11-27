import unittest

import mock

from ..ckan_api import CkanApiClient
from ..deduper import Deduper

class TestDeduper(unittest.TestCase):
    def setUp(self):
        self.ckan_api = mock.Mock(CkanApiClient)
        self.deduper = Deduper('test-org', self.ckan_api)

    def test_replace_dataset(self):
        harvest_identifier = 'harvest-123'
        self.ckan_api.get_oldest_dataset.return_value = dict(name='test-dataset')
        self.ckan_api.get_newest_dataset.return_value = dict(name='test-dataset')
        self.deduper.replace_oldest_dataset_with_newest(harvest_identifier)

        self.ckan_api.get_oldest_dataset.assert_called_once_with(harvest_identifier)
        self.ckan_api.get_newest_dataset.assert_called_once_with(harvest_identifier)
