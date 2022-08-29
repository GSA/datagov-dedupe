
from __future__ import absolute_import
import codecs
import unicodecsv as csv
from datetime import datetime
import json
import logging
import os

from . import util

log = logging.getLogger(__name__)


class RemovedPackageLog(object):
    def __init__(self, filename=None, run_id=None):
        if not run_id:
            run_id = datetime.now().strftime('%Y%m%d%H%M%S')

        if not filename:
            filename = 'removed-packages-%s.log' % run_id

        log.info('Opening removed packages log for writing filename=%s', filename)
        self.log = codecs.open(filename, mode='w', encoding='utf8')

    def add(self, package):
        log.debug('Saving package to removed package log package=%s', package['id'])
        self.log.write(json.dumps(package) + '\n')

        # Persist the write to disk
        self.log.flush()
        os.fsync(self.log.fileno())


class DuplicatePackageLog(object):
    # Order matters here for the report
    fieldnames = [
        'organization',                     # Organization name
        'duplicate_id',                     # Duplicate id (CKAN ID)
        'duplicate_title',                  # Duplicate title (CKAN title)
        'duplicate_name',                   # Duplicate name (CKAN name)
        'duplicate_url',                    # Duplicate URL (site URL + CKAN name)
        'duplicate_metadata_created',       # Duplicate CKAN metadata_created from CKAN
        'duplicate_identifier',             # Duplicate POD metadata identifier (in CKAN extra)
        'duplicate_source_hash',            # Duplicate source_hash (in CKAN extra)
        'duplicate_is_collection',          # Duplicate is a collection dataset
        'duplicate_is_collection_member',   # Duplicate is a collection member
        'duplicate_harvest_source',         # Duplicate harvest_source_id (in CKAN extra)
        'retained_id',                      # Retained id (CKAN id)
        'retained_url',                     # Retained URL (site URL + CKAN name)
        'retained_metadata_created',        # Retained metadata_created
        'retained_harvest_source',          # Retained harvest_source_id (in CKAN extra)
    ]

    def __init__(self, filename=None, api_url=None, run_id=None):
        self.api_url = api_url

        if not run_id:
            run_id = datetime.now().strftime('%Y%m%d%H%M%S')

        if not filename:
            filename = 'duplicate-packages-%s.csv' % run_id

        log.info('Opening duplicate package report for writing filename=%s', filename)
        self.__f = open(filename, mode='wb')
        self.log = csv.DictWriter(self.__f,
                                  encoding='utf-8', fieldnames=DuplicatePackageLog.fieldnames)
        self.log.writeheader()

    def add(self, duplicate_package, retained_package):
        log.debug('Recording duplicate package to report package=%s', duplicate_package['id'])
        self.log.writerow({
            'duplicate_id': duplicate_package['id'],
            'duplicate_identifier': util.get_package_extra(duplicate_package, 'identifier'),
            'duplicate_is_collection': bool(util.get_package_extra(duplicate_package, 'collection_metadata')),
            'duplicate_is_collection_member': bool(util.get_package_extra(duplicate_package, 'collection_package_id')),
            'duplicate_metadata_created': duplicate_package['metadata_created'],
            'duplicate_name': duplicate_package['name'],
            'duplicate_source_hash': util.get_package_extra(duplicate_package, 'source_hash'),
            'duplicate_harvest_source': util.get_package_extra(duplicate_package, 'harvest_source_id'),
            'duplicate_title': duplicate_package['title'],
            'duplicate_url': '%s/dataset/%s' % (self.api_url, duplicate_package['name']),
            'organization': duplicate_package['organization']['name'],
            'retained_id': retained_package['id'],
            'retained_metadata_created': retained_package['metadata_created'],
            'retained_harvest_source': util.get_package_extra(retained_package, 'harvest_source_id'),
            'retained_url': '%s/dataset/%s' % (self.api_url, retained_package['name']),
        })

        # Persist the write to disk
        self.__f.flush()
        os.fsync(self.__f.fileno())
