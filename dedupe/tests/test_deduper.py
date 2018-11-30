import unittest

import mock

from ..ckan_api import CkanApiClient
from ..deduper import Deduper
from ..audit import DuplicatePackageLog, RemovedPackageLog


class TestDeduper(unittest.TestCase):
    def setUp(self):
        self.duplicate_package_log = mock.Mock(DuplicatePackageLog)
        self.removed_package_log = mock.Mock(RemovedPackageLog)

        self.ckan_api = mock.Mock(CkanApiClient)
        self.deduper = Deduper('test-org', self.ckan_api, removed_package_log=self.removed_package_log, duplicate_package_log=self.duplicate_package_log)

    def test_replace_dataset(self):
        harvest_identifier = 'harvest-123'

        self.ckan_api.get_oldest_dataset.return_value = dict(id='old', name='test-dataset-old')
        self.ckan_api.get_newest_dataset.return_value = dict(id='new', name='test-dataset-new')

        self.deduper.replace_oldest_dataset_with_newest(harvest_identifier)

        self.ckan_api.get_oldest_dataset.assert_called_once_with(harvest_identifier)
        self.ckan_api.get_newest_dataset.assert_called_once_with(harvest_identifier)

        expected_update_package_calls = [
            mock.call(dict(id='old', name='test-dataset-old-dedupe-purge')), # old package renamed
            mock.call(dict(id='new', name='test-dataset-old')), # new package takes original name
        ]

        assert self.ckan_api.update_package.mock_calls == expected_update_package_calls


    def test_remove_duplicate(self):
        duplicate = {'id': '123', 'name': 'duplicate-package'}
        retained = {'id': '456', 'name': 'retained-package'}

        self.deduper.remove_duplicate(duplicate, retained)

        self.duplicate_package_log.add.assert_called_once_with(duplicate, retained)
        self.removed_package_log.add.assert_called_once_with(duplicate)
