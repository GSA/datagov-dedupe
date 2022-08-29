#!/usr/bin/env python

from __future__ import absolute_import
from setuptools import setup, find_packages

setup(name='datagov-dedupe',
      version='1.0',
      description='Data.gov package deduplication script.',
      author='General Services Administration',
      url='https://github.com/GSA/datagov-dedupe',
      packages=find_packages(),
      test_suite='dedupe.tests',
      )
