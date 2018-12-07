'''
Duper looks for duplicate packages within a single organization, updates the
most recent duplicate and removes the rest.
'''

from datetime import datetime
import itertools
import logging

from .ckan_api import CkanApiFailureException
from . import util

module_log = logging.getLogger(__name__)

PACKAGE_NAME_MAX_LENGTH = 100


class ContextLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        kv_pairs = ' '.join('%s=%s' % (key, value) for key, value in self.extra.items())
        return '%s %s' % (msg, kv_pairs), kwargs


class Deduper(object):
    def __init__(self, organization_name, ckan_api, removed_package_log=None, duplicate_package_log=None, run_id=None):
        self.organization_name = organization_name
        self.ckan_api = ckan_api
        self.log = ContextLoggerAdapter(module_log, {'organization': organization_name})
        self.removed_package_log = removed_package_log
        self.duplicate_package_log = duplicate_package_log
        self.stopped = False

        if not run_id:
            run_id = datetime.now().strftime('%Y%m%d%H%M%S')

        self.run_id = run_id

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
        count = itertools.count(start=1)
        for identifier in harvester_identifiers:
            if self.stopped:
                break

            self.log.info('Deduplicating identifier=%s progress=%r',
                          identifier['name'], (next(count), len(harvester_identifiers)))
            try:
                duplicate_count += self.dedupe_harvest_identifier(identifier['name'])
            except CkanApiFailureException, e:
                self.log.error('Failed to dedupe harvest identifier=%s', identifier['name'])
                continue

        self.log.info('Summary duplicate_count=%d', duplicate_count)


    def remove_duplicate(self, duplicate_package, retained_package):
        self.log.info('Removing duplicate package=%r', (duplicate_package['id'], duplicate_package['name']))
        if self.removed_package_log:
            self.removed_package_log.add(duplicate_package)

        if self.duplicate_package_log:
            self.duplicate_package_log.add(duplicate_package, retained_package)

        self.ckan_api.remove_package(duplicate_package['id'])

    def mark_retained_package(self, retained_package):
        '''
        Mark the retained package with a datagov_dedupe property in case we're
        interrupted. This allows us to continue with removing duplicates when we resume.

        This also let's us store the original name so that we can rename the
        retained package at the very end. We need to capture the original
        datasets name before it is removed.
        '''

        # Make the package with its to-be new name
        # We don't rename it yet, because our logs become confusing.
        # Instead, we rename at the end after the logs have been written
        # and all the duplicates removed. We have to record the name now
        # because the oldest package could be removed. If the oldest
        # package is removed and then we're interrupted, we won't know what
        # the reall oldest pacakge was.
        identifier = util.get_package_extra(retained_package, 'identifier')

        self.log.debug('Fetching original dataset for harvest identifier=%s', identifier)
        original_dataset = self.ckan_api.get_oldest_dataset(identifier)

        # Rename the original package to prevent name conflict
        original_name = original_dataset['name']
        if not original_name.endswith('-dedupe-purge'):
            # Add suffix, maintain the max-length limit
            original_dataset['name'] = ('%s-dedupe-purge' % original_name)[:PACKAGE_NAME_MAX_LENGTH]
            self.log.info('Rename original name=%s package=%r',
                          original_dataset['name'], (original_dataset['id'], original_name))
            self.ckan_api.update_package(original_dataset)
        else:
            self.log.warning('Dataset already renamed, continuing without rename package=%r',
                             (original_dataset['id'], original_dataset['name']))

        self.log.info('Marking retained dataset for idempotency package=%r',
                      (retained_package['id'], retained_package['name']))
        util.set_package_extra(retained_package, 'datagov_dedupe',
                               dict(rename_to=original_name))

        # Call the update API
        self.log.debug('Mark retained package in API package=%r',
                       (retained_package['id'], retained_package['name']))
        self.ckan_api.update_package(retained_package)


    def commit_retained_package(self, retained_package):
        '''
        Unmarks the package for deduplication and commits the rename.
        '''
        # Get the new name for the package
        name = util.get_package_extra(retained_package, 'datagov_dedupe')['rename_to']
        retained_package['name'] = name

        # Mark the retained package
        util.set_package_extra(retained_package, 'datagov_dedupe', None)
        util.set_package_extra(retained_package, 'datagov_dedupe_retained', self.run_id)

        self.log.debug('Commit retained package in API package=%r',
                       (retained_package['id'], retained_package['name']))
        self.ckan_api.update_package(retained_package)

    def dedupe_harvest_identifier(self, identifier):
        '''
        Removes duplicate datasets for the given harvest identifier. The
        deduper is meant to be idempotent so that if it is interrupted, it can
        pick up where it left off without losing data.

        1. Get the number of datasets with this identifier.
           a. If there is only one dataset, no duplicates. Continue with next identifier.
        2. Fetch the most recent dataset which is to be retained.
        3. Mark the retained dataset as being processed. This records some
           additional information before we actually start removing duplicates,
           like the original dataset name.
        4. Fetch the datasets for this identifier in batches.
        5. For each dataset:
           a. Check if this is the retained dataset, in which we skip.
           b. Remove the dataset.
        6. Commit the retained dataset as being processed and rename it to the
           original name.

        We make sure the rename of the retained dataset happens last. This
        keeps the logging cleaner, since we don't want to confuse ourselves
        logging information that is changing. This also means the same
        information is logged in dry-run vs read/write.

        Returns the number of duplicate datasets.
        '''

        log = ContextLoggerAdapter(
            module_log,
            {'organization': self.organization_name, 'identifier': identifier},
            )

        log.debug('Fetching number of datasets for harvest identifier')
        harvest_data_count = self.ckan_api.get_dataset_count(self.organization_name, identifier)
        log.info('Found packages count=%d', harvest_data_count)

        # If there is only one or less, there's no duplicates.
        if harvest_data_count <= 1:
            log.debug('No duplicates found for harvest identifier.')
            return 0

        # We want to keep the most recent dataset
        self.log.debug('Fetching most recent dataset for harvest identifier=%s', identifier)
        retained_dataset = self.ckan_api.get_newest_dataset(identifier)

        # Check if the dedupe process has been started on this package
        if not util.get_package_extra(retained_dataset, 'datagov_dedupe'):
            # We mark the retained package as having started the dedupe
            # process. This helps us record information like the original
            # package name so that in case we are interrupted, we can pick up
            # where we left off.
            #
            # If we're interrupted, it's possible the original dataset would've
            # been removed, so we need to make sure we collect all information
            # we need for the final retained update now.
            self.mark_retained_package(retained_dataset)

        # Fetch datasets in batches
        def get_datasets(total, rows=1000):
            '''
            Returns a generator for fetching additional packages in batches of :rows.
            '''
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
            if self.stopped:
                self.log.debug('Deduper is stopped, cleaning up...')
                break

            if dataset['organization']['name'] != self.organization_name:
                log.warning('Dataset harvested by organization but not part of organization pkg_org_name=%s package=%r',
                            dataset['organization']['name'], (dataset['id'], dataset['name']))
                continue

            if dataset['id'] == retained_dataset['id']:
                log.debug('This package is the most recent, not removing package=%s', dataset['id'])
                continue

            duplicate_count += 1
            try:
                self.remove_duplicate(dataset, retained_dataset)
            except CkanApiFailureException, e:
                log.error('Failed to remove dataset status_code=%s package=%r',
                          e.response.status_code, (dataset['id'], dataset['name']))
                continue

        # Rename the retained package
        self.log.info('Committing retained package rename package=%r',
                      (retained_dataset['id'], retained_dataset['name']))
        self.commit_retained_package(retained_dataset)

        return duplicate_count


    def stop(self):
        '''
        Tells the Deduper to stop processing anymore records.
        '''
        self.stopped = True
