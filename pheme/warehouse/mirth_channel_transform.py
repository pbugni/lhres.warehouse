#!/usr/bin/env python
from lxml import etree
from optparse import OptionParser
import os
import sys

from pheme.util.config import Config

usage = """%prog [options] src target_directory

Transform an exported mirth channel to another channel for importing.

Mirth channels can be easily exported in XML format.  This utility
provides a mechanims to alter an export for subsequent import.  Useful
for altering details such as database name and user authentication.

NB - values defined in the project configuration file will be used
unless overwritten.  See py:module:`pheme.util.Config`

src              - 'source' mirth channel to transform
target_directory - path where transformed file(s) will be written

Try `%prog --help` for more information.
"""

class Options(object):
    """Gather configuration and runtime parameters """

    def __init__(self):
        config = Config()
        self.db = config.get('warehouse', 'database', '')
        self.user = config.get('warehouse', 'database_user', '')
        self.password = config.get('warehouse', 'database_password', '')
        self.input_dir = config.get('warehouse', 'input_dir', '')
        self.output_dir = config.get('warehouse', 'output_dir', '')
        self.error_dir = config.get('warehouse', 'error_dir', '')
 
    def processArgs(self, argv):
        """ Process any optional arguments and possitional parameters
        """
        parser = OptionParser(usage=usage)
        parser.add_option("-d", "--database", dest="db", 
                          default=self.db, help="name of database")
        parser.add_option("-u", "--user", dest="user",
                          default=self.user, help="database user")
        parser.add_option("-p", "--password", dest="password",
                          default=self.password,
                          help="database password")
        parser.add_option("--input_dir", dest="input_dir",
                          default=self.input_dir,
                          help="filesystem directory for channel to poll")
        parser.add_option("--output_dir", dest="output_dir",
                          default=self.output_dir,
                          help="filesystem directory for channel output")
        parser.add_option("--error_dir", dest="error_dir",
                          default=self.error_dir,
                          help="filesystem directory for channel errors")
        
        (options, args) = parser.parse_args()
        if len(args) != 2:
            parser.error("incorrect number of arguments")
    
        self.src = args[0]
        if not os.access(self.src, os.R_OK):
            parser.error("Can't open source file: '%s'" % self.src)

        self.target_dir = args[1]
        if not os.access(self.target_dir, os.W_OK):
            parser.error("Can't write to target dir: %s" % self.target_dir)

        self.db = parser.values.db
        self.user = parser.values.user
        self.password = parser.values.password

    def transformer(self):
        """Returns the transformer with config/user options"""
        t = Transform(src=self.src,
                      target_dir=self.target_dir)
        # Communicate user options
        for attr in ('db', 'user', 'password', 'input_dir', 
                     'output_dir', 'error_dir'):
            setattr(t, attr, getattr(self, attr))
        return t


class Transform(object):
    """Transform a mirth channel as directed """

    def __init__(self, src, target_dir):
        self.src = src
        self.target_dir = target_dir

    def _targetFile(self):
        """Returns an open file handle ready for writes"""
        basename = os.path.basename(self.src)
        filename = os.path.join(self.target_dir, basename)
        return open(filename, 'w')

    def __call__(self):
        """Perform standard transformer steps"""
        self.parseSourceDocument()
        self.transform()

    def parseSourceDocument(self):
        """Parse the source xml"""
        self.tree = etree.parse(self.src)

    def _adjust_source_connector(self, srcProps):
        #the same source_connector element is used for all types
        #don't break other connector types (i.e. by writing path information
        #for the host name on an HTTP listener)
        connector_type = [prop.text for prop in 
                          srcProps.iter(tag='property')
                          if prop.attrib['name'] == "DataType"][0]
        if connector_type != 'File Reader':
            return
        
        for prop in srcProps.iter(tag='property'):
            if prop.attrib['name'] == 'host':
                prop.text = self.input_dir
            if prop.attrib['name'] == 'moveToDirectory':
                prop.text = self.output_dir
            if prop.attrib['name'] == 'moveToErrorDirectory':
                prop.text = self.error_dir

    def _adjust_destination(self, destProps):
        for prop in destProps.iter(tag='property'):
            if prop.attrib['name'] == 'username':
                prop.text = self.user
            if prop.attrib['name'] == 'password':
                prop.text = self.password
            if prop.attrib['name'] == 'URL':
                prop.text = self._adjust_connection_URL(prop.text)
            if prop.attrib['name'] == 'script' and prop.text:
                if "createDatabaseConnection" in prop.text:
                    prop.text = self._adjust_createDbCall(prop.text)

    def _adjust_source_filter(self, filter):
        for script_string in \
                filter.iterfind("rules/rule/data/entry/string"):
            if script_string.text and \
                   "createDatabaseConnection" in script_string.text:
                script_string.text = self._adjust_createDbCall(\
                    script_string.text)
        for script_string in \
                filter.iterfind("rules/rule/script"):
            if "createDatabaseConnection" in script_string.text:
                script_string.text = self._adjust_createDbCall(\
                    script_string.text)
            
    def _adjust_source_transformer(self, filter):
        for script_string in \
                filter.iterfind("steps/step/"\
                                "data/entry/string"):
            if script_string.text and \
                   "createDatabaseConnection" in script_string.text:
                script_string.text = self._adjust_createDbCall(\
                    script_string.text)
        for script_string in \
                filter.iterfind("steps/step/script"):
            if "createDatabaseConnection" in script_string.text:
                script_string.text = self._adjust_createDbCall(\
                    script_string.text)

    def _adjust_connection_URL(self, text):
        """Replace only the dbname in the connection URL

        Typically something like: jdbc:mysql://localhost:3306/mirthdb

        Returns adjusted connection URL

        """
        dbname = self.db
        parts = text.split('/')

        # Preserve the quotes if present
        if parts[-1].endswith("'"):
            dbname += "'"

        parts[-1] = dbname
        return '/'.join(parts)

    def _adjust_createDbCall(self, text):
        """Alter db connection in javascript snippet

        Look to fix up a call in the middle of a javascript snippet
        like the following, taking care to preserve surrounding
        text: 
          DatabaseConnectionFactory.createDatabaseConnection(
           'com.mysql.jdbc.Driver',
           'jdbc:mysql://localhost:3306/dbName','theUser',
           'thePassword');

        returns adjusted text

        """
        funcStart = text.index("createDatabaseConnection")
        paramsEnd = text.index(")", funcStart)
        if not text.count("(", funcStart, paramsEnd) == 1:
            raise RuntimeError("Nested parens in "
                               "'createDatabaseConnection()' not supported.")
        if "createDatabaseConnection" in text[paramsEnd:]:
            raise RuntimeError("Multiple calls to "
                               "'createDatabaseConnection()' not supported.")

        paramsStart = text.index("(", funcStart)
        params = text[paramsStart+1:paramsEnd].split(",")
        params[1] = self._adjust_connection_URL(params[1])
        params[2] = "'" + self.user + "'"
        params[3] = "'" + self.password + "'"

        # Reassemble the pieces
        return text[:paramsStart+1] + ",".join(params) + text[paramsEnd:]

    def transform(self):
        """Perform the transformation"""
        
        # Adjust the necessary values in the channel source
        srcProps = self.tree.xpath(
            "/channel/sourceConnector/properties")
        assert(len(srcProps) == 1)
        self._adjust_source_connector(srcProps[0])

        srcFilters = self.tree.xpath(
            "/channel/sourceConnector/filter")
        map(self._adjust_source_filter, srcFilters)

        srcTransformers = self.tree.xpath(
            "/channel/sourceConnector/transformer")
        map(self._adjust_source_transformer, srcTransformers)

        # Adjust the necessary values in all the destinations
        destinations = self.tree.xpath(
            "/channel/destinationConnectors/connector/properties")
        map(self._adjust_destination, destinations)

        # Write out the finished product
        file = self._targetFile()
        self.tree.write(file, pretty_print=False)
        print 'wrote new channel export:', file.name


def main():
    options = Options()
    options.processArgs(sys.argv[1:])
    
    transformer = options.transformer()
    transformer()

if __name__ == '__main__':
    main()
