
import codecs
import csv
from datetime import datetime
import json
import logging
import os

log = logging.getLogger(__name__)


class RemovedPackageLog(object):
    def __init__(self, filename=None):
        if not filename:
            filename = 'removed-packages-%s.log' % datetime.now().strftime('%Y%m%d%H%M%S')

        log.info('Opening removed packages log for writing filename=%s', filename)
        self.log = codecs.open(filename, mode='w', encoding='utf8')

    def add(self, package):
        log.debug('Saving package to removed package log package=%s', package['id'])
        self.log.write(json.dumps(package) + '\n')


class DuplicatePackageLog(object):
    fieldnames = [
        'id',                   # Duplicate id (CKAN ID)
        'title',                # Duplicate title (CKAN title)
        'name',                 # Duplicate name (CKAN name)
        'url',                  # Duplicate URL (site URL + CKAN name)
        'metadata_created',     # Duplicate CKAN metadata_created from CKAN
        'identifier',           # Duplicate POD metadata identifier (in CKAN extra)
        'source_hash',          # Duplicate source_hash (in CKAN extra)
        'retained_id',          # Retained id (CKAN id)
        'retained_url',         # Retained URL (site URL + CKAN name)
    ]

    def __init__(self, filename=None, api_url=None):
        self.api_url = api_url

        if not filename:
            filename = 'duplicate-packages-%s.csv' % datetime.now().strftime('%Y%m%d%H%M%S')

        log.info('Opening duplicate package report for writing filename=%s', filename)
        self.log = csv.DictWriter(codecs.open(filename, mode='w', encoding='utf8'), DuplicatePackageLog.fieldnames)
        self.log.writeheader()

    def add(self, duplicate_package, retained_package):
        log.debug('Recording duplicate package to report package=%s', duplicate_package['id'])
        self.log.writerow({
            'id': duplicate_package['id'],
            'title': duplicate_package['title'],
            'name': duplicate_package['name'],
            'url': '%s/%s' % (self.api_url, duplicate_package['name']),
            'metadata_created': duplicate_package['metadata_created'],
            'identifier': duplicate_package['extra']['identifier'],
            'source_hash': duplicate_package['extra']['source_hash'],
            'retained_id': retained_package['id'],
            'retained_url': '%s/%s' % (self.api_url, retained_package['name']),
        })
