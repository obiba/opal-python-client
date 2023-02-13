"""
Opal SPSS data import (Using R).
"""

import obiba_opal.core as core
import obiba_opal.io as io


def add_arguments(parser):
    """
    Add data command specific options
    """
    parser.add_argument('--path', '-pa', required=True,
                        help='SPSS file, optionally compressed, to import from the Opal filesystem.')
    parser.add_argument('--locale', '-l', required=False, help='SPSS file locale (e.g. fr, en...).')
    parser.add_argument('--type', '-ty', required=False, help='Entity type (e.g. Participant)')
    parser.add_argument('--idVariable', '-iv', required=False,
                        help='SPSS variable that provides the entity ID. If not specified, first variable values are considered to be the entity identifiers.')

    # non specific import arguments
    io.add_import_arguments(parser)


def do_command(args):
    """
    Execute import data command
    """
    # Build and send request
    # Check input filename extension
    if not (args.path.endswith('.sav')) and not (args.path.endswith('.zsav')):
        raise Exception('Input must be a SPSS file (.sav or .zsav).')

    client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
    importer = io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
                                            incremental=args.incremental, limit=args.limit,
                                            identifiers=args.identifiers,
                                            policy=args.policy, merge=args.merge, verbose=args.verbose)
    # print result
    extension_factory = OpalExtensionFactory(path=args.path,
                                                locale=args.locale, entityType=args.type, idVariable=args.idVariable)

    response = importer.submit(extension_factory)

    # format response
    res = response.content
    if args.json:
        res = response.pretty_json()

    # output to stdout
    print(res)

class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
    def __init__(self, path, locale, entityType, idVariable):
        self.path = path
        self.locale = locale
        self.entityType = entityType
        self.idVariable = idVariable

    def add(self, factory):
        """
        Add specific datasource factory extension
        """
        extension = {
            'file': self.path,
            'symbol': self.path[self.path.rfind("/") + 1:self.path.rfind(".")]
        }

        if self.locale:
            extension['locale'] = self.locale
        if self.entityType:
            extension['entityType'] = self.entityType
        if self.idVariable:
            extension['idColumn'] = self.idVariable

        factory['Magma.RHavenDatasourceFactoryDto.params'] = extension
