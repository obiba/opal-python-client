"""
Opal Xml import.
"""

import opal.core
import opal.io
import sys


def add_arguments(parser):
    """
    Add data command specific options
    """
    parser.add_argument('--path', '-pa', required=True, help='Zip of XML files to import from the Opal filesystem.')
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
        extension_factory = OpalExtensionFactory(path=args.path)

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
    def __init__(self, path):
        self.path = path

    def add(self, factory):
        """
        Add specific datasource factory extension
        """
        factory['Magma.FsDatasourceFactoryDto.params'] = {'file': self.path}
