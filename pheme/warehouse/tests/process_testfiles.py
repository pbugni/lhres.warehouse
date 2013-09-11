import argparse
import getpass
import glob
import os
import shlex
import shutil
import subprocess
import time

from pheme.util.config import Config
from pheme.util.pg_access import FilesystemPersistence
from pheme.warehouse.tests.test_warehousedb import setup_module

def require_mirth():
    """Check process space for running instance of Mirth Connect"""
    ret = os.system("ps -ef |grep -v grep |grep com\.mirth\.connect "
                    "> /dev/null")
    if ret:
        raise ValueError("Mirth Connect is required - not found in "
                         "process space")

def wipe_dir_contents(path, as_user):
    """Deletes all filesystem files in a given path"""
    if len(os.listdir(path)) == 0:
        return

    if as_user != getpass.getuser():
        args = shlex.split("sudo -u %s rm" % as_user) +\
            glob.glob(path + '/*')
        subprocess.call(args)
    else:
        for f in os.listdir(path):
            os.remove(os.path.join(path, f))


def copy_file_to_dir(file, dir, as_user):
    """Copy file to directory as requested user"""
    if as_user != getpass.getuser():
        subprocess.call(shlex.split("sudo -u %s cp %s %s" %
                                    (as_user, file, dir)))
    else:
        shutil.copy(file, dir)

class MirthInteraction(object):
    """Abstraction to interact with Mirth Connect for testing"""
    WAIT_INTERVAL = 15
    TIMEOUT = 300

    def __init__(self):
        self.config = Config()

        # obtain list of files to process
        path = os.path.abspath(
            os.path.join(os.path.dirname(__file__),
                         "../../../test_hl7_batchfiles"))
        self.filenames = [os.path.join(path, file) for file in 
                          os.listdir(path)]


    def prepare_filesystem(self):
        # create clean database (includes non produciton sanity check)
        setup_module()

        # wipe previous run(s) files
        mirth_user = self.config.get('mirth', 'mirth_system_user')
        for dir in ('input_dir', 'output_dir', 'error_dir'):
            wipe_dir_contents(self.config.get('warehouse', dir),
                              mirth_user)


    def process_batchfiles(self):
        """Feed the testfiles to mirth - block till done"""
        self.prepare_filesystem()
        require_mirth()
        for batchfile in self.filenames:
            copy_file_to_dir(batchfile, 
                             self.config.get('warehouse', 'input_dir'),
                             self.config.get('mirth', 'mirth_system_user'))

        # wait for all files to appear in error our output dirs
        # providing occasional output and raising if we appear hung
        last_count = 0
        last_time = time.time()
        output_dir = self.config.get('warehouse', 'output_dir')
        error_dir = self.config.get('warehouse', 'error_dir')
        while last_count < len(self.filenames):
            time.sleep(self.WAIT_INTERVAL)
            count = len(os.listdir(output_dir)) + len(os.listdir(error_dir))
            if count > last_count:
                last_count = count
                last_time = time.time()
            if time.time() - self.TIMEOUT > last_time:
                raise RuntimeError("TIMEOUT exceeded waiting on Mirth")
            print "Waiting on mirth to process files...(%d of %d)" %\
                (last_count, len(self.filenames))

    def persist_database(self):
        """Write the database contents to disk"""
        fsp = FilesystemPersistence(\
            database=self.config.get('warehouse', 'database'),
            user=self.config.get('warehouse', 'database_user'),
            password=self.config.get('warehouse', 'database_password'))
        fsp.persist()

    def restore_database(self):
        """Pull previously persisted data into database"""
        fsp = FilesystemPersistence(\
            database=self.config.get('warehouse', 'database'),
            user=self.config.get('warehouse', 'database_user'),
            password=self.config.get('warehouse', 'database_password'))
        fsp.restore()

def process_testfiles_via_mirth():
    """Entry point to process HL7 batchfiles via mirth"""

    description = """The amount of data fed through mirth makes
    testing too slow for interactive development.  Use this function
    to perform the Mirth Connect processing of the available test HL7
    batchfiles, which results in a database dump to make subsequent
    test runs managable.  Any alterations to the channels or
    batchfiles require a fresh execution of this function."""

    ap = argparse.ArgumentParser(description=description)
    ap.add_argument("--restore", action='store_true',
                    help="simply restore the database from "
                    "persistence file, presumably available "
                    "from older run, i.e. do NOT (re-)process "
                    "the testfiles")
    ap.add_argument("--persist", action='store_true',
                    help="simply write persistence file "
                    "as the database stands, i.e. do NOT (re-)process "
                    "the testfiles")
    args = ap.parse_args()

    # still here implies a run - let MirthInteraction do the work
    mi = MirthInteraction()
    if not (args.persist or args.restore):
        mi.process_batchfiles()
    if args.restore:
        mi.restore_database()
    else:
        mi.persist_database()
