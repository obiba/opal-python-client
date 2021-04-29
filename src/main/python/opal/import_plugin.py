"""
Opal datasource plugin import.
"""

import json
import opal.core
import opal.io
import sys


def add_arguments(parser):
    """
    Add import command specific options
    """
    parser.add_argument('--name', '-n', required=True, help='Opal datasource plugin name')
    parser.add_argument('--config', '-c', required=False, help='A JSON file containing the import configuration')

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
        extension_factory = OpalExtensionFactory(name=args.name, config=args.config)

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
    def __init__(self, name, config):
        self.name = name
        self.config = json.loads(open(config).read())

    def add(self, factory):
        """
        Add specific datasource factory extension
        """
        extension = {}

        if self.name:
            extension['name'] = self.name
        if self.config:
            extension['parameters'] = json.dumps(self.config)

        factory['Magma.PluginDatasourceFactoryDto.params'] = extension
