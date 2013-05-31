"""Interface to a series of frequently used mirth shell commands.

Project setup.py defines entry points for the functions below.

"""
import os
import tempfile

from lhres.util.config import Config
from lhres.warehouse.mirth_channel_transform import Options

CHANNELS = ('MBDS_hl7_obx_insert',
            'MBDS_Upload',
            'MBDS_hl7_visit_insert',
            'MBDS_batch_consumer',
            'MBDS_hl7_dx_insert',
            'MBDS_hl7_obr_insert',)


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

        """
        for channel in imports:
            assert(os.path.exists(channel))
            script_file.write("import %s force\n" % channel)
        for channel, output in exports:
            script_file.write("export %s %s\n" % (channel, output))
        if imports:
            script_file.write("deploy\n")
            script_file.write("status\n")
        script_file.flush()
        script_file.seek(0)
        os.chmod(script_file.name, 0644)

    def execute_script(self, script):
        "Execute the given script via the mirth shell"
        try:
            orig_dir = os.getcwd()
            os.chdir(self.mirth_home)
            command = "sudo -H -u %s ./mccommand -s %s" %\
                (self.mirth_system_user, script.name)
            print command
            os.system(command)
        except:
            raise
        finally:
            os.chdir(orig_dir)


def transform_channels(path='/tmp'):
    """Apply default transform to MBDS channels"""
    channels = [os.path.join(path, '%s.xml' % c) for c in CHANNELS]
    opts = Options()
    opts.src = None
    opts.target_dir = path
    transformer = opts.transformer()
    for c in channels:
        transformer.src = c
        transformer()

def deploy_channels(path='/tmp'):
    """Entry point to deploy the channels to mirth on localhost"""
    imports = [os.path.join(path, '%s.xml' % c) for c in CHANNELS]
    ms = MirthShell()
    with tempfile.NamedTemporaryFile('w') as script_file:
        ms.write_script(script_file, imports=imports)
        ms.execute_script(script_file)

def export_channels(path='/tmp'):
    """Entry point to export the MBDS channels to named directory"""
    if not os.path.isdir(path):
        raise ValueError("required output directory not found")
    exports = []
    for c in CHANNELS:
        exports.append((c, os.path.join(path, '%s.xml' % c)))

    ms = MirthShell()
    with tempfile.NamedTemporaryFile('w') as script_file:
        ms.write_script(script_file, exports=exports)
        ms.execute_script(script_file)
