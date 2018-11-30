'''
Duper looks for duplicate packages within a single organization, updates the
most recent duplicate and removes the rest.
'''

import logging

from .ckan_api import CkanApiFailureException

module_log = logging.getLogger(__name__)


class ContextLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        kv_pairs = ' '.join('%s=%s' % (key, value) for key, value in self.extra.items())
        return '%s %s' % (msg, kv_pairs), kwargs


class Deduper(object):
    def __init__(self, organization_name, ckan_api, removed_package_log=None, duplicate_package_log=None):
        self.organization_name = organization_name
        self.ckan_api = ckan_api
        self.log = ContextLoggerAdapter(module_log, {'organization': organization_name})
        self.removed_package_log = removed_package_log
        self.duplicate_package_log = duplicate_package_log

    def dedupe(self):
        # get list of harvesters for the organization
        self.log.debug('Fetching harvesters')
        try:
            harvester_identifiers = self.ckan_api.get_harvester_identifiers(self.organization_name)
        except CkanApiFailureException, e:
            self.log.error('Failed to fetch harvest identifiers for organization')
            self.log.exception(e)
            # continue onto the next organization
            return

        self.log.info('Found harvest identifiers count=%d', len(harvester_identifiers))

        duplicate_count = 0
        for identifier in harvester_identifiers:
            try:
                duplicate_count += self.dedupe_harvest_identifier(identifier['name'])
            except CkanApiFailureException, e:
                self.log.error('Failed to dedupe harvest identifier=%s', identifier['name'])
                continue

        self.log.info('Summary duplicate_count=%d', duplicate_count)

    def replace_oldest_dataset_with_newest(self, harvest_identifier):
        self.log.debug('Fetching oldest and most recent pacakges for harvest identifier=%s', harvest_identifier)
        oldest_dataset = self.ckan_api.get_oldest_dataset(harvest_identifier)
        newest_dataset = self.ckan_api.get_newest_dataset(harvest_identifier)

        self.log.info('Replacing oldest dataset with most recent oldest=%r newest=%r identifier=%s',
                      (oldest_dataset['id'], oldest_dataset['name']),
                      (newest_dataset['id'], newest_dataset['name']),
                      harvest_identifier)

        # save the original dataset name to give it to the most recent
        name = oldest_dataset['name']

        if name.endswith('-dedupe-purge'):
            self.log.warning('Package already renamed, continuing without rename package=%r',
                             (oldest_dataset['id'], oldest_dataset['name']))
            return newest_dataset

        # update oldest dataset
        self.log.info('Renaming oldest dataset identifier=%s package=%s name=%s',
                      harvest_identifier, oldest_dataset['id'], oldest_dataset['name'])
        oldest_dataset['name'] = '%s-dedupe-purge' % name
        self.ckan_api.update_package(oldest_dataset)

        # update neweset dataset
        self.log.info('Renaming most recent dataset identifier=%s package=%s name=%s',
                      harvest_identifier, newest_dataset['id'], newest_dataset['name'])
        newest_dataset['name'] = name
        self.ckan_api.update_package(newest_dataset)

        return newest_dataset

    def remove_duplicate(self, duplicate_package, retained_package):
        self.log.info('Removing duplicate package=%r', (duplicate_package['id'], duplicate_package['name']))
        if self.removed_package_log:
            self.removed_package_log.add(duplicate_package)

        if self.duplicate_package_log:
            self.duplicate_package_log.add(duplicate_package, retained_package)

        self.ckan_api.remove_package(duplicate_package['id'])

    def dedupe_harvest_identifier(self, identifier):
        '''
        Removes duplicate datasets for the given harvest identifier.

        Returns the number of duplicate datasets.
        '''

        log = ContextLoggerAdapter(module_log, {'organization': self.organization_name, 'identifier': identifier})

        log.debug('Fetching number of datasets for harvest identifier')
        harvest_data_count = self.ckan_api.get_dataset_count(self.organization_name, identifier)
        log.info('Found packages count=%d', harvest_data_count)

        # If there is only one or less, there's no duplicates.
        if harvest_data_count <= 1:
            log.debug('No duplicates found for harvest identifier.')
            return 0

        # We want to keep the most recent dataset, but there is a name conflict
        # with the oldest dataset. Rename the oldest dataset so that we can
        # give it's name to the newest
        new_dataset = self.replace_oldest_dataset_with_newest(identifier)

        # Fetch datasets in batches
        def get_datasets(total, rows=1000):
            start = 0
            while start < total:
                log.debug(
                    'Batch fetching datasets for harvest offset=%d rows=%d total=%d',
                    start, rows, total)
                datasets = self.ckan_api.get_datasets(self.organization_name, identifier, start, rows)
                start += len(datasets)

                for dataset in datasets:
                    yield dataset

        # Now we can collect the datasets for removal
        duplicate_count = 0
        for dataset in get_datasets(harvest_data_count):
            if dataset['organization']['name'] != self.organization_name:
                log.warning('Dataset harvested by organization but not part of organization pkg_org_name=%s package=%r',
                            dataset['organization']['name'], (dataset['id'], dataset['name']))
                continue

            if dataset['id'] == new_dataset['id']:
                log.debug('This package is the most recent, not removing package=%s', dataset['id'])
                continue

            duplicate_count += 1
            try:
                self.remove_duplicate(dataset, new_dataset)
            except CkanApiFailureException, e:
                log.error('Failed to remove dataset status_code=%s package=%r', e.response.status_code, (dataset['id'], dataset['name']))
                continue


        return duplicate_count
