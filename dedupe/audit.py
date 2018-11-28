
import codecs
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
        log.debug('Saving package to removed package log packge=%s', package['id'])
        self.log.write(json.dumps(package) + '\n')
