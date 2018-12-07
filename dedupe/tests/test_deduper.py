import unittest

import mock

from ..ckan_api import CkanApiClient
from ..deduper import Deduper
from ..audit import DuplicatePackageLog, RemovedPackageLog
from .. import util


class TestDeduper(unittest.TestCase):
    def setUp(self):
        self.duplicate_package_log = mock.Mock(DuplicatePackageLog)
        self.removed_package_log = mock.Mock(RemovedPackageLog)

        self.ckan_api = mock.Mock(CkanApiClient)
        self.deduper = Deduper('test-org', self.ckan_api, removed_package_log=self.removed_package_log, duplicate_package_log=self.duplicate_package_log)

    def test_remove_duplicate(self):
        duplicate = {'id': '123', 'name': 'duplicate-package'}
        retained = {'id': '456', 'name': 'retained-package'}

        self.deduper.remove_duplicate(duplicate, retained)

        self.duplicate_package_log.add.assert_called_once_with(duplicate, retained)
        self.removed_package_log.add.assert_called_once_with(duplicate)
        self.ckan_api.remove_package.assert_called_once_with(duplicate['id'])


    def test_mark_retained_package(self):
        identifier = 'harvest-identifier-1'
        original = {
            'id': '123',
            'name': 'original-package',
            'extras': [
                {'key': 'identifier', 'value': identifier},
            ],
        }
        retained = {
            'id': '456',
            'name': 'retained-package',
            'extras': [
                {'key': 'identifier', 'value': identifier},
            ],
        }
        self.ckan_api.get_oldest_dataset.return_value = original

        self.deduper.mark_retained_package(retained)

        self.ckan_api.get_oldest_dataset.assert_called_once_with(identifier)

        extra_keys = [extra['key'] for extra in retained['extras']]
        assert 'datagov_dedupe' in extra_keys

        datagov_dedupe = util.get_package_extra(retained, 'datagov_dedupe')
        assert 'rename_to' in datagov_dedupe
        self.assertEqual(datagov_dedupe['rename_to'], 'original-package')

    def test_commit_retained_package(self):
        retained = {
            'id': '456',
            'name': 'retained-package',
            'extras': [
                {'key': 'identifier', 'value': 'harvest-identifier-1'},
                {'key': 'datagov_dedupe', 'value': {'rename_to': 'original-package'}},
            ],
        }

        self.deduper.commit_retained_package(retained)

        extra_keys = [extra['key'] for extra in retained['extras']]
        assert 'datagov_dedupe' not in extra_keys, \
            'Expected datagov_dedupe extra to be removed from package'
        self.assertEqual(retained['name'], 'original-package', 'Expected package to be renamed to rename_to')
        self.ckan_api.update_package.assert_called_once_with(retained)
