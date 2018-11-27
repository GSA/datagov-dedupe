'''
Duper looks for duplicate packages within a single organization, updates the
most recent duplicate and removes the rest.
'''

import logging

log = logging.getLogger(__name__)


class Deduper(object):
    def __init__(self, organization_name, ckan_api):
        self.organization_name = organization_name
        self.ckan_api = ckan_api
        self.log = logging.LoggerAdapter(log, {'organization': organization_name})

    def dedupe(self):
        pass

    def replace_oldest_dataset_with_newest(self, harvest_identifier):
        oldest_dataset = self.ckan_api.get_oldest_dataset(harvest_identifier)
        newest_dataset = self.ckan_api.get_newest_dataset(harvest_identifier)

        name = oldest_dataset['name']

        # update oldest dataset
        # update neweset dataset

        return newest_dataset
