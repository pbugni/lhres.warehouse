pheme.warehouse
===============

Public Health EHR Message Engine (PHEME), Data Warehouse Module

Module responsible for creation and maintenance of the Data Warehouse used
by the PHEME system.

License
-------

BSD 3 clause license - See LICENSE.txt

Tests
-----
To run the tests, install the system on a test virtual machine, and execute:

  ./setup.py test

NB - this will look for configuration details just like production,
and destructively create and destroy database and other dependencies.
DO NOT run on a production system.
