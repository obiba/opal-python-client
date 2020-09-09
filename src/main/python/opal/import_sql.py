"""
Opal Xml import.
"""

import sys
import opal.core
import opal.io


def add_arguments(parser):
    """
    Add data command specific options
    """
    parser.add_argument('--database', '-db', required=True, help='Name of the SQL database.')
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
        extension_factory = OpalExtensionFactory(database=args.database)

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
    def __init__(self, database):
        self.database = database

    def add(self, factory):
        """
        Add specific datasource factory extension
        """
        jdbc_factory = factory.Extensions[opal.protobuf.Magma_pb2.JdbcDatasourceFactoryDto.params]
        jdbc_factory.database = self.database