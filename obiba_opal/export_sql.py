"""
Data export to a SQL database.
"""

import obiba_opal.core as core
import obiba_opal.io as io


class ExportSQLCommand:

    @classmethod
    def add_arguments(self, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Datasource name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--database', '-db', required=True, help='Name of the SQL database')
        parser.add_argument('--incremental', '-i', action='store_true', help='Incremental export')
        parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(self, args):
        """
        Execute export data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            exporter = io.OpalExporter.build(client=client, datasource=args.datasource, tables=args.tables,
                                                    identifiers=args.identifiers, output=args.database,
                                                    incremental=args.incremental, verbose=args.verbose)
            response = exporter.submit('jdbc')
            # format response
            core.Formatter.print_json(response.from_json(), args.json)
        finally:
            client.close()