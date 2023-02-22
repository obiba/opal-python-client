"""
Data export in SPSS (using R).
"""

import obiba_opal.core as core
import obiba_opal.io as io


class ExportRSPSSCommand:

    @classmethod
    def add_arguments(self, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Datasource name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--output', '-out', required=True, help='Output file name (.sav or .zsav (compressed format))')
        parser.add_argument('--id-name', '-in', required=False, help='Name of the ID column name')
        parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
        parser.add_argument('--no-multilines', '-nl', action='store_true',
                            help='Do not write value sequences as multiple lines')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(self, args):
        """
        Execute export data command
        """
        # Build and send request
        # Check output filename extension
        if not (args.output.endswith('.sav')) and not (args.output.endswith('.zsav')):
            raise Exception('Output must be a SPSS file (.sav or .zsav).')

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            exporter = io.OpalExporter.build(client=client, datasource=args.datasource, tables=args.tables, entityIdNames = args.id_name,
                                                    identifiers=args.identifiers, output=args.output, incremental=False,
                                                    multilines=(not args.no_multilines), verbose=args.verbose)
            # print result
            response = None
            if args.output.endswith('.sav'):
                response = exporter.submit('RSPSS')
            else:
                response = exporter.submit('RZSPSS')

            # format response
            core.Formatter.print_json(response.from_json(), args.json)
        finally:
            client.close()