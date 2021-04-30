"""
Opal LimeSurvey import.
"""

import json
import opal.core
import opal.io
import sys


def add_arguments(parser):
    """
    Add data command specific options
    """
    parser.add_argument('--url', '-ur', required=False, help='LimeSurvey SQL database JDBC url (if not provided, plugin defaults will be used).')
    parser.add_argument('--uname', '-un', required=False, help='LimeSurvey SQL database user name (if not provided, plugin defaults will be used).')
    parser.add_argument('--pword', '-pwd', required=False, help='LimeSurvey SQL database user password (if not provided, plugin defaults will be used).')
    parser.add_argument('--prefix', '-pr', required=False, help='Table prefix (if not provided, plugin defaults will be used).')
    parser.add_argument('--properties', '-pp', required=False, help='SQL properties (if not provided, plugin defaults will be used).')

    # non specific import arguments
    opal.io.add_import_arguments(parser)


def do_command(args):
    """
    Execute import data command
    """
    # Build and send request
    try:
        client = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args))
        importer = opal.io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
                                              incremental=args.incremental, limit=args.limit,
                                              identifiers=args.identifiers,
                                              policy=args.policy, merge=args.merge, verbose=args.verbose)
        # print result
        extension_factory = OpalExtensionFactory(url=args.url, uname=args.uname, pword=args.pword, prefix=args.prefix, properties=args.properties)

        response = importer.submit(extension_factory)

        # format response
        res = response.content
        if args.json:
            res = response.pretty_json()

        # output to stdout
        print(res)

    except Exception as e:
        print(e)
        sys.exit(2)
    except pycurl.error as error:
        errno, errstr = error
        print('An error occurred: ', errstr, file=sys.stderr)
        sys.exit(2)


class OpalExtensionFactory(opal.io.OpalImporter.ExtensionFactoryInterface):
    def __init__(self, url, uname, pword, prefix, properties):
        self.url = url
        self.uname = uname
        self.pword = pword
        self.prefix = prefix
        self.properties = properties

    def add(self, factory):
        """
        Add specific datasource factory extension
        """
        extension = {}

        extension['name'] = 'opal-datasource-limesurvey'

        config = {}
        if self.url:
            config['url'] = self.url
        if self.uname:
            config['username'] = self.uname
        if self.pword:
            config['password'] = self.pword
        if self.prefix:
            config['prefix'] = self.prefix
        if self.properties:
            config['properties'] = self.properties
        extension['parameters'] = json.dumps(config)

        factory['Magma.PluginDatasourceFactoryDto.params'] = extension
