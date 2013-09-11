#!/usr/bin/env python

import os
from setuptools import setup

docs_require = ['Sphinx']
tests_require = ['nose', 'coverage']

try:
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, 'README.txt')) as r:
        README = r.read()
except IOError:
    README = ''

setup(name='pheme.warehouse',
      version='13.05',
      description="Data warehouse module for PHEME",
      long_description=README,
      license="BSD-3 Clause",
      namespace_packages=['pheme'],
      packages=['pheme.warehouse', ],
      include_package_data=True,
      install_requires=['setuptools', 'pheme.util', 'lxml', 'psycopg2', 'SQLAlchemy'],
      setup_requires=['nose'],
      tests_require=tests_require,
      test_suite="nose.collector",
      extras_require = {'test': tests_require,
                        'docs': docs_require,
                        },
      entry_points=("""
                    [console_scripts]
                    create_warehouse_tables=pheme.warehouse.tables:main
                    deploy_channels=pheme.warehouse.mirth_shell_commands:deploy_channels
                    export_channels=pheme.warehouse.mirth_shell_commands:export_channels
                    transform_channels=pheme.warehouse.mirth_shell_commands:transform_channels
                    process_testfiles_via_mirth=pheme.warehouse.tests.process_testfiles:process_testfiles_via_mirth
                    """),
)
