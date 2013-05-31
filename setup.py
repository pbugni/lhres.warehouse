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

setup(name='lhres.warehouse',
      version='13.05',
      description="Data warehouse module for LHRES",
      long_description=README,
      license="BSD-3 Clause",
      namespace_packages=['lhres'],
      packages=['lhres.warehouse', ],
      include_package_data=True,
      install_requires=['setuptools', 'lhres.util', 'lxml', 'psycopg2', 'SQLAlchemy'],
      setup_requires=['nose'],
      tests_require=tests_require,
      test_suite="nose.collector",
      extras_require = {'test': tests_require,
                        'docs': docs_require,
                        },
      entry_points=("""
                    [console_scripts]
                    create_warehouse_tables=lhres.warehouse.tables:main
                    deploy_channels=lhres.warehouse.mirth_shell_commands:deploy_channels
                    export_channels=lhres.warehouse.mirth_shell_commands:export_channels
                    transform_channels=lhres.warehouse.mirth_shell_commands:transform_channels
                    mirth_channel_transform=lhres.warehouse.mirth_channel_transform:main
                    """),
)
