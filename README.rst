pheme.warehouse
===============

**Public Health EHR Message Engine (PHEME), Data Warehouse Module**

Module responsible for creation and maintenance of the `Data Warehouse`
used by the PHEME system.  Several `Mirth Connect`_ channels process
HL/7 batch files writing portions of their content to a warehouse
schema for consumption by other PHEME modules.

Requirements
------------

* Install `Mirth Connect`_ and provide the ``admin`` user with a
  secure password and set this password in
  ``$MIRTH_HOME/conf/mirth-cli-config.properties`` for
  ``pheme.warehouse`` command line tools proper function.
* `PostgreSQL`_
* `Python`_ 2.7.*

Install
-------

Beyond the requirements listed above, ``pheme.warehouse`` is
dependent on the ``pheme.util`` module.  Although future builds may
automatically pick it up, for now, clone and build it in the same
virtual environment (or native environment) being used for
``pheme.warehouse``::

    git clone https://github.com/pbugni/pheme.util.git
    cd pheme.util
    ./setup.py develop
    cd ..

Then clone and build this module::

    git clone https://github.com/pbugni/pheme.warehouse.git
    cd pheme.warehouse
    ./setup.py develop

A pheme config file (see ``pheme.util.config``) must specify where
`Mirth Connect` was installed::

    [mirth]
    mirth_home=/opt/mirth/current

Transformation of the mirth channels uses values in the pheme config
file.  (NB for the tests to pass, ``create_table_user`` must be the
same as ``database_user``, however for security reasons, this should
not be the case on production systems) Values needed include::

    [mirth]
    create_table_user=system
    create_table_password=password
    database=warehouse
    database_user=user
    database_password=password
    input_dir=/opt/pheme/filedrops
    error_dir=/opt/pheme/error
    output_dir=/opt/pheme/processed

For database creation to work, postgres must be configured to handle
the user named above.  Substitute in the database, user and password
values below to match the settings used in the pheme config file::

    $ sudo -u postgres createdb warehouse
    $ sudo -u postgres createuser system
    Shall the new role be a superuser? (y/n) n
    Shall the new role be allowed to create databases? (y/n) n
    Shall the new role be allowed to create more new roles? (y/n) n
    $ sudo -u postgres psql
    postgres=# alter user system password 'password';
    ALTER ROLE
    postgres=# alter database warehouse owner to system;
    ALTER DATABASE

The `setup.py develop` call above in pheme.warehouse also created an
executable for database schema creation.  NB, this will destroy any
data in the named database (which is why it prompts for `destroy`)::

    create_warehouse_tables

To transform the channels, provide the checked out location of the
channels (i.e. ``pheme.warehouse/channels``) and a temporary directory for
output::

    transform_channels channels /tmp/transformed

Deploy the transformed channels.  See `Mirth Connect` requirement
above and make sure it's running::

    deploy_channels /tmp/transformed

Running
-------

The executable programs provided by ``pheme.warehouse`` are listed
under [console_scripts] within the project's setup.py file.  All take
the standard help options [-h, --help].  Invoke with help for more
information::

    mirth_channel_transform --help

The user running Mirth Connect must also be named in the pheme config
file, so permission to manipulate channels will function.  Extend the
pheme config file (using the appropriate value for username) such as::

    [mirth]
    mirth_system_user=username

Tests
-----

As this test suite destructively interacts with the database, it is
recommended that a test virtual machine be used for testing.  There is
a safeguard in place that prevents running the tests if the pheme
config file has ``in_production`` set to prevent accidental data
destruction.

Tests create and destroy database tables using the values in the pheme
config file [warehouse] section.  The named database must first be
created for successful test execution::

  createdb `configvar warehouse database`

A significant portion of the testing for this module relies on Mirth
processing, which is runtime expensive.  To make feature testing
manageable, hooks exist to persist an rerun a batch of files through
Mirth.  First run requires this processing::

  process_testfiles_via_mirth

Thereafter, tests reuse the persisted data. Rerun
``process_testfiles_via_mirth`` on any channel changes.  For module
level tests execute::

  ./setup.py test

License
-------

BSD 3 clause license - See LICENSE.txt


.. _Mirth Connect: http://www.mirthcorp.com/products/mirth-connect
.. _PostgreSQL: http://www.postgresql.org/
.. _Python: http://www.python.org/download/releases/2.7/
.. _virtualenv: https://pypi.python.org/pypi/virtualenv
