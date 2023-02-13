"""
Opal data import.
"""

import obiba_opal.core as core
import obiba_opal.io as io
import sys


def add_arguments(parser):
    """
    Add data command specific options
    """
    parser.add_argument('--ropal', '-ro', required=True, help='Remote Opal server base url')
    parser.add_argument('--ruser', '-ru', required=False, help='Remote user name (exclusive from using token)')
    parser.add_argument('--rpassword', '-rp', required=False, help='Remote user password (exclusive from using token)')
    parser.add_argument('--rtoken', '-rt', required=False,
                        help='Remote personal access token (exclusive from user credentials)')
    parser.add_argument('--rdatasource', '-rd', required=True, help='Remote datasource name')
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
    # remote opal client factory
    if (args.rtoken and args.ruser) or (not args.rtoken and not args.ruser):
        print("Either specify token OR user credentials (user name and password)")
    else:
        extension_factory = OpalExtensionFactory(ropal=args.ropal, rdatasource=args.rdatasource, ruser=args.ruser,
                                                    rpassword=args.rpassword, rtoken=args.rtoken)
        response = importer.submit(extension_factory)

        # format response
        res = response.content
        if args.json:
            res = response.pretty_json()

        # output to stdout
        print(res)

class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
    def __init__(self, ropal, rdatasource, ruser, rpassword, rtoken):
        self.ropal = ropal
        self.rdatasource = rdatasource
        self.ruser = ruser
        self.rpassword = rpassword
        self.rtoken = rtoken

    def add(self, factory):
        """
        Add specific datasource factory extension
        """
        rest_factory = {
            'remoteDatasource': self.rdatasource,
            'url': self.ropal
        }
        if self.rtoken:
            rest_factory['token'] = self.rtoken
        else:
            rest_factory['username'] = self.ruser
            rest_factory['password'] = self.rpassword

        factory['Magma.RestDatasourceFactoryDto.params'] = rest_factory
