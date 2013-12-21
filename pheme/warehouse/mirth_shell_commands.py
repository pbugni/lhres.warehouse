"""Interface to a series of frequently used mirth shell commands.

Project setup.py defines entry points for the functions below.

"""
import argparse
import getpass
import os
import shutil
import tempfile

from pheme.util.config import Config
from pheme.warehouse.mirth_channel_transform import TransformManager


CHANNELS = ('PHEME_hl7_obx_insert',
            'PHEME_http_receiver',
            'PHEME_hl7_visit_insert',
            'PHEME_batchfile_consumer',
            'PHEME_hl7_dx_insert',
            'PHEME_hl7_obr_insert',
            'dump_to_disk',)


class MirthShell(object):
    """Sets up and executes common tasks via the mirth shell

    We have a few common patterns, such as deploying test and
    production versions of the mirth channels.  This manages pulling
    in configuration details and setting up the necessary paths to
    make it scriptable.

    """
    def __init__(self):
        self.config = Config()
        self.mirth_home = self.config.get('mirth', 'mirth_home')
        self.mirth_system_user = self.config.get('mirth', 'mirth_system_user')

    def write_script(self, script_file, imports=[], exports=[],):
        """Generate script for mirth shell to run

        :param script_file: An open file handle to a writable file
        :param imports: The mirth channel(s) XML export to import
        :param exports: The list of (channel name, output path) to
          export.

        Writes the necessary mirth shell instructions to the
        script_file so that the file can be executed via the mirth
        shell.  NB - this takes the liberty of relaxing the 0600 mode
        settings hardcoded in NamedTemporaryFile so mirth shell will
        have read access if running as a different user.

        Codetemplates are also imported or exported (if imports or
        exports are defined) into the same directory as the first named
        import / export in a file named "codetemplates.xml".

        """
        for channel in imports:
            assert(os.path.exists(channel))
            script_file.write("import %s force\n" % channel)
        for channel, output in exports:
            script_file.write("export %s %s\n" % (channel, output))
        if imports:
            codetemplates = os.path.join(os.path.dirname(imports[0]),
                                         "codetemplates.xml")
            script_file.write("importcodetemplates %s\n" % codetemplates)
            script_file.write("deploy\n")
            script_file.write("status\n")
        if exports:
            codetemplates = os.path.join(os.path.dirname(exports[0][1]),
                                         "codetemplates.xml")
            script_file.write("exportcodetemplates %s\n" % codetemplates)
        script_file.flush()
        script_file.seek(0)
        os.chmod(script_file.name, 0644)

    def execute_script(self, script):
        "Execute the given script via the mirth shell"
        try:
            orig_dir = os.getcwd()
            os.chdir(self.mirth_home)
            sudo = ''
            if getpass.getuser() != self.mirth_system_user:
                sudo = 'sudo -H -u %s' % self.mirth_system_user
            command = "%s ./mccommand -s %s" %\
                (sudo, script.name)
            print command
            os.system(command)
        except:
            raise
        finally:
            os.chdir(orig_dir)


def transform_channels():
    """Apply default transform to PHEME channels"""

    doc = """
    Mirth channels can be easily exported in XML format.  This utility
    provides a mechanims to alter an export for subsequent import.
    Useful for altering details such as database name and user
    authentication.

    NB - values defined in the project configuration file will be used
    unless provided as optional arguments.  See
    `pheme.util.config.Config`
    """
    config = Config()
    ap = argparse.ArgumentParser(description=doc)
    ap.add_argument("-d", "--database", dest="db",
                    default=config.get('warehouse', 'database'),
                    help="name of database (overrides "
                    "[warehouse]database)")
    ap.add_argument("-u", "--user", dest="user",
                    default=config.get('warehouse', 'database_user'),
                    help="database user (overrides "
                    "[warehouse]database_user)")
    ap.add_argument("-p", "--password", dest="password",
                    default=config.get('warehouse', 'database_password'),
                    help="database password (overrides [warehouse]"
                    "database_password)")
    ap.add_argument("--input_dir", dest="input_dir",
                    default=config.get('warehouse', 'input_dir'),
                    help="filesystem directory for channel to poll "
                    "(overrides [warehouse]input_dir)")
    ap.add_argument("--output_dir", dest="output_dir",
                    default=config.get('warehouse', 'output_dir'),
                    help="filesystem directory for channel output "
                    "(overrides [warehouse]output_dir)")
    ap.add_argument("--error_dir", dest="error_dir",
                    default=config.get('warehouse', 'error_dir'),
                    help="filesystem directory for channel errors "
                    "(overrides [warehouse]error_dir)")
    ap.add_argument("source_directory",
                    help="directory containing source channel "
                    "definition files")
    ap.add_argument("target_directory",
                    help="directory to write transformed channel "
                    "definition files")
    args = ap.parse_args()
    source_dir = os.path.realpath(args.source_directory)
    target_dir = os.path.realpath(args.target_directory)

    transformer = TransformManager(src=None,
                                   target_dir=target_dir,
                                   options=args)
    for c in CHANNELS:
        transformer.src = os.path.join(source_dir, '%s.xml' % c)
        transformer()
    # no transformation on codetemplates at this time - but the
    # importer expects the codetemplates.xml file to be in the same
    # directory, so copy it over.
    shutil.copy(os.path.join(source_dir, 'codetemplates.xml'), target_dir)


def deploy_channels():
    """Entry point to deploy the channels to mirth on localhost"""
    ap = argparse.ArgumentParser(description="deploy known PHEME channels "
                                 "and code templates to Mirth Connect")
    ap.add_argument("deploy_directory",
                    help="directory containing channel definition files")
    args = ap.parse_args()
    path = os.path.realpath(args.deploy_directory)

    imports = [os.path.join(path, '%s.xml' % c) for c in CHANNELS]
    ms = MirthShell()
    with tempfile.NamedTemporaryFile('w') as script_file:
        ms.write_script(script_file, imports=imports)
        ms.execute_script(script_file)


def export_channels():
    """Entry point to export the PHEME channels to named directory"""
    ap = argparse.ArgumentParser(description="export known PHEME channels "
                                 "and code templates from Mirth Connect")
    ap.add_argument("export_directory",
                    help="directory for exported files")
    args = ap.parse_args()
    path = os.path.realpath(args.export_directory)

    if not os.path.isdir(path):
        raise argparse.ArgumentTypeError("can't access %s" % path)
    exports = []
    for c in CHANNELS:
        exports.append((c, os.path.join(path, '%s.xml' % c)))

    ms = MirthShell()
    with tempfile.NamedTemporaryFile('w') as script_file:
        ms.write_script(script_file, exports=exports)
        ms.execute_script(script_file)
