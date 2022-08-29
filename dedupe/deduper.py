'''
Duper looks for duplicate packages within a single organization, updates the
retained package and removes the rest.
'''

from __future__ import absolute_import
from datetime import datetime
import itertools
import logging

from .ckan_api import CkanApiFailureException, CkanApiCountException, CkanApiStatusException
from . import util

module_log = logging.getLogger(__name__)

PACKAGE_NAME_MAX_LENGTH = 100


class DeduperStopException(Exception):
    '''Raised when the deduper is asked to stop processing and gracefully exit.'''
    pass


class ContextLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        kv_pairs = ' '.join('%s=%s' % (key, value) for key, value in self.extra.items())
        return '%s %s' % (msg, kv_pairs), kwargs


class Deduper(object):
    def __init__(self,
                 organization_name,
                 ckan_api,
                 removed_package_log=None,
                 duplicate_package_log=None,
                 collection_package_log=None,
                 run_id=None,
                 oldest=True,
                 update_name=False,
                 identifier_type='identifier'):
        self.organization_name = organization_name
        self.ckan_api = ckan_api
        self.log = ContextLoggerAdapter(module_log, {'organization': organization_name})
        self.removed_package_log = removed_package_log
        self.duplicate_package_log = duplicate_package_log
        self.collection_package_log = collection_package_log
        self.stopped = False
        self.oldest = oldest
        self.update_name = update_name
        self.identifier_type = identifier_type

        if not run_id:
            run_id = datetime.now().strftime('%Y%m%d%H%M%S')

        self.run_id = run_id

    def dedupe(self):
        '''
        The main dedupe process.

        Fetches dataset identifiers that match 2 or more packages, indicating a
        duplicate. Then processes each identifier for removal of duplicates.

        First, non-collection duplicate dataset identifiers are fetched and
        deduplicated. Then collection duplicate dataset identifiers are fetched
        and deduplicated. They are processed separately due to differences in
        query parameters.
        '''

        def _fetch_and_dedupe_identifiers(is_collection):
            '''
            Helper method to loop over identifiers and deduplicate them.
            Returns the number of duplicate datasets.
            '''

            # Label the dataset as collection or non-collection, mostly for log output
            label = 'collection' if is_collection else 'non-collection'

            self.log.debug('Fetching %s dataset identifiers with duplicates', label)
            try:
                identifiers = self.ckan_api.get_duplicate_identifiers(self.organization_name,
                                                                      is_collection)
            except CkanApiFailureException as exc:
                self.log.error('Failed to fetch %s dataset identifiers for organization', label)
                self.log.exception(exc)
                # continue onto the next organization
                return

            self.log.info('Found %s dataset identifiers with duplicates count=%d',
                          label,
                          len(identifiers))

            duplicate_count = 0
            count = itertools.count(start=1)
            # Work with the identifer name, since that's all we need and it's a
            # little cleaner.
            for identifier in identifiers:
                if self.stopped:
                    raise DeduperStopException()

                self.log.info('Deduplicating %s=%s progress=%r',
                              self.identifier_type, identifier, (next(count), len(identifiers)))
                try:
                    duplicate_count += self.dedupe_identifier(identifier, is_collection)
                except CkanApiFailureException:
                    self.log.error('Failed to dedupe %s=%s', self.identifier_type, identifier)
                    # Move on to next identifier
                    continue
                except CkanApiCountException:
                    self.log.error('Got an invalid count, this may not be a duplicate or there '
                                   'could be inconsistencies between db and solr. Try running the '
                                   'db_solr_sync job. %s=%s', self.identifier_type, identifier)
                    # Move on to next identifier
                    continue

            self.log.info('Removed duplicates for %s datasets duplicate_count=%d',
                          label,
                          duplicate_count)
            return duplicate_count

        # Total deduplicated datasets for both non-collection and collection datasets
        total_duplicate_count = 0

        # First, process non-collection datasets
        try:
            total_duplicate_count += _fetch_and_dedupe_identifiers(is_collection=False)
        except DeduperStopException:
            self.log.warning('Deduper is stopped, cleaning up...')
            # Just return to end processing early and gracefully
            return

        # Process collection datasets
        try:
            total_duplicate_count += _fetch_and_dedupe_identifiers(is_collection=True)
        except DeduperStopException:
            self.log.warning('Deduper is stopped, cleaning up...')
            # Just return to end processing early and gracefully
            return

        self.log.info('Summary duplicate_count=%d', total_duplicate_count)

    def remove_duplicate(self, duplicate_package, retained_package):
        self.log.info('Removing duplicate package=%r',
                      (duplicate_package['id'], duplicate_package['name']))
        if self.removed_package_log:
            self.removed_package_log.add(duplicate_package)

        if self.duplicate_package_log:
            self.duplicate_package_log.add(duplicate_package, retained_package)

        self.update_collection_datasets(duplicate_package, retained_package)

        try:
            self.ckan_api.remove_package(duplicate_package['id'])
        except CkanApiStatusException:
            self.log.warning('Failed to remove package, skipping: %r',
                             (duplicate_package['id'], duplicate_package['name']))

        if (len(duplicate_package['name']) < len(retained_package['name']) and self.update_name):
            # If the package to be retained has extra random character at
            #  the end of the name, we want to rename it to the "standard"
            #  name to keep the typical URL.
            self.log.info('Renaming kept package from %s to %s',
                          retained_package['name'], duplicate_package['name'])
            retained_package['name'] = duplicate_package['name']
            self.ckan_api.update_package(retained_package)
            if self.removed_package_log:
                self.removed_package_log.add(retained_package)

    def update_collection_datasets(self, duplicate_package, retained_package):
        # Collection records may not have changed, and may be linked to the
        #  dataset that is marked for removal. Update collection records
        #  to point to the dataset that will be retained
        collection_datasets = self.ckan_api.get_datasets_in_collection(duplicate_package['id'])
        if collection_datasets is not None:
            self.log.info('Updating collection records for dataset=%r',
                          (duplicate_package['id'], duplicate_package['name']))
            for cd in collection_datasets:
                self.log.info('Updating record %s', cd['title'])
                for e in cd['extras']:
                    if e['key'] == "collection_package_id":
                        e['value'] = retained_package['id']
                        break
                self.ckan_api.update_package(cd)
                if self.collection_package_log:
                    self.collection_package_log.add(retained_package['id'])
                self.log.info('Updated record with collection id %s', retained_package['id'])

    def mark_retained_package(self, retained_package):
        '''
        Mark the retained package with a datagov_dedupe property in case we're
        interrupted. This allows us to continue with removing duplicates when we resume.

        Note: we're currently not mutating the data in a way that wouldn't be idempotent.

        Note: this isn't really necessary anymore because we're not doing a
        rename which required us to gather the state up front in case we are
        interrupted. We leave this here in case rename behavior needs to be re-added.
        '''
        self.log.info('Marking retained dataset for idempotency package=%r',
                      (retained_package['id'], retained_package['name']))
        util.set_package_extra(retained_package, 'datagov_dedupe',
                               self.run_id)

        # Call the update API
        self.log.debug('Mark retained package in API package=%r',
                       (retained_package['id'], retained_package['name']))
        self.ckan_api.update_package(retained_package)

    def commit_retained_package(self, retained_package):
        '''
        Unmarks the package for deduplication and commits any data changes.
        '''
        # Unmark the retained package
        util.set_package_extra(retained_package, 'datagov_dedupe', None)

        # Add the run_id so there is some record we can look back on.
        util.set_package_extra(retained_package, 'datagov_dedupe_retained', self.run_id)

        self.log.debug('Commit retained package in API package=%r',
                       (retained_package['id'], retained_package['name']))
        self.ckan_api.update_package(retained_package)

    def dedupe_identifier(self, identifier, is_collection=False):
        '''
        Removes duplicate datasets for the given identifier. The
        deduper is meant to be idempotent so that if it is interrupted, it can
        pick up where it left off without losing data.

        1. Get the number of datasets with this identifier.
           a. If there is only one dataset, no duplicates. Continue with next identifier.
        2. Fetch the dataset which is to be retained (oldest or newest depending on
            --newest).
        3. Mark the retained dataset as being processed.
        4. Fetch the datasets for this identifier in batches.
        5. For each dataset:
           a. Check if this is the retained dataset, in which we skip.
           b. Remove the dataset.
        6. Commit the retained dataset as being processed.

        We make sure the commit of the retained dataset happens last. This
        keeps the logging cleaner, since we don't want to confuse ourselves
        logging information that is potentially changing. This also means the
        same information is logged in dry-run vs read/write.

        Returns the number of duplicate datasets.
        '''

        log = ContextLoggerAdapter(
            module_log,
            {'organization': self.organization_name, self.identifier_type: identifier},
        )

        log.debug('Fetching number of datasets for unique identifier')
        dataset_count = self.ckan_api.get_dataset_count(self.organization_name, identifier, is_collection)
        log.info('Found packages count=%d', dataset_count)

        # If there is only one or less, there's no duplicates.
        if dataset_count <= 1:
            log.debug('No duplicates found for identifier.')
            return 0

        sort_order = 'asc' if self.oldest else 'desc'
        # We want to keep the oldest dataset
        self.log.debug('Fetching %s dataset for %s=%s', 'oldest' if self.oldest else 'newest',
                       self.identifier_type, identifier)
        retained_dataset = self.ckan_api.get_dataset(self.organization_name,
                                                     identifier,
                                                     is_collection,
                                                     sort_order=sort_order)

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
                    'Batch fetching datasets for %s offset=%d rows=%d total=%d',
                    self.identifier_type, start, rows, total)
                datasets = self.ckan_api.get_datasets(self.organization_name, identifier, start, rows, is_collection)
                if len(datasets) < 1:
                    log.warning('Got zero datasets from API offset=%d total=%d', start, total)
                    raise StopIteration

                start += len(datasets)
                for dataset in datasets:
                    yield dataset

        # Now we can collect the datasets for removal
        duplicate_count = 0
        for dataset in get_datasets(dataset_count):
            if self.stopped:
                raise DeduperStopException()

            if dataset['organization']['name'] != self.organization_name:
                log.warning('Dataset harvested by organization but not part of organization pkg_org_name=%s package=%r',
                            dataset['organization']['name'], (dataset['id'], dataset['name']))
                continue

            if dataset['id'] == retained_dataset['id']:
                log.debug('This package is the retained dataset, not removing package=%s', dataset['id'])
                continue

            duplicate_count += 1
            try:
                self.remove_duplicate(dataset, retained_dataset)
            except CkanApiFailureException as e:
                log.error('Failed to remove dataset status_code=%s package=%r',
                          e.response.status_code, (dataset['id'], dataset['name']))
                continue

        # Commit the retained package
        self.log.info('Committing retained package package=%r',
                      (retained_dataset['id'], retained_dataset['name']))
        self.commit_retained_package(retained_dataset)

        return duplicate_count

    def stop(self):
        '''
        Tells the Deduper to stop processing anymore records.
        '''
        self.stopped = True
