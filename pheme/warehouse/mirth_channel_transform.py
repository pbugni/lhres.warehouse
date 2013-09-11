#!/usr/bin/env python
from lxml import etree
import os
import sys

from pheme.util.config import Config


def transformer_factory(tree, options):
    """Return appropriate channel transform agent for channel

    Attempts are made to use the CommonTransferAgent for all channels,
    but some exceptions exist.  In exceptional cases, use the channel
    name to determine the specialized class for performing the
    transformation.

    :param tree: parsed channel in etree instance

    """
    channel_name = tree.xpath("/channel/name")
    if channel_name[0].text == 'PHEME_http_receiver':
        return PHEME_http_receiverTransferAgent(tree, options)
    return CommonTransferAgent(tree, options)


class CommonTransferAgent(object):
    def __init__(self, channel, options):
        self.tree = channel
        self.options = options

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

        # Return the transformed channel
        return self.tree

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
                prop.text = self.options.input_dir
            if prop.attrib['name'] == 'moveToDirectory':
                prop.text = self.options.output_dir
            if prop.attrib['name'] == 'moveToErrorDirectory':
                prop.text = self.options.error_dir

    def _adjust_destination(self, destProps):
        for prop in destProps.iter(tag='property'):
            if prop.attrib['name'] == 'username':
                prop.text = self.options.user
            if prop.attrib['name'] == 'password':
                prop.text = self.options.password
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
        dbname = self.options.db
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
        params[2] = "'" + self.options.user + "'"
        params[3] = "'" + self.options.password + "'"

        # Reassemble the pieces
        return text[:paramsStart+1] + ",".join(params) + text[paramsEnd:]


class PHEME_http_receiverTransferAgent(CommonTransferAgent):
    def _adjust_destination(self, destProps):
        """PHEME_http_receiver specialized destination transformer

        Use the value of "input_dir" for the destination's output,
        as this channel drops files inplace for the other to pick up.
        """
        super(PHEME_http_receiverTransferAgent, self).\
            _adjust_destination(destProps)
        for prop in destProps.iter(tag='property'):
            if prop.attrib['name'] == 'host':
                prop.text = self.options.input_dir


class TransformManager(object):
    """Transform a mirth channel as directed

    Manages the transformation process, parsing, delegating the
    transformation work to the appropriate transform agent and writing
    out the results.

    """

    def __init__(self, src, target_dir, options):
        self.src = src
        self.target_dir = target_dir
        self.options = options

    def _targetFile(self):
        """Returns an open file handle ready for writes"""
        basename = os.path.basename(self.src)
        filename = os.path.join(self.target_dir, basename)
        return open(filename, 'w')

    def __call__(self):
        """Perform standard transformer steps"""
        self.tree = etree.parse(self.src)

        agent = transformer_factory(self.tree, self.options)
        self.tree = agent.transform()

        # Write out the finished product
        file = self._targetFile()
        self.tree.write(file, pretty_print=False)
        print 'wrote transformed channel:', file.name
