"""
Opal Xml import.
"""

import obiba_opal.core as core
import obiba_opal.io as io


def add_arguments(parser):
    """
    Add data command specific options
    """
    parser.add_argument('--path', '-pa', required=True, help='Zip of XML files to import from the Opal filesystem.')
    # non specific import arguments
    io.add_import_arguments(parser)


def do_command(args):
    """
    Execute import data command
    """
    # Build and send request
    client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
    importer = io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
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

class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
    def __init__(self, path):
        self.path = path

    def add(self, factory):
        """
        Add specific datasource factory extension
        """
        factory['Magma.FsDatasourceFactoryDto.params'] = {'file': self.path}
