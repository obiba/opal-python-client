"""
Data copy.
"""

import obiba_opal.core as core
import obiba_opal.io as io

class CopyTableCommand:
    """
    Execute a copy table command.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--project', '-pr', required=True, help='Source project name')
        parser.add_argument('--tables', '-t', nargs='+', required=False,
                            help='List of table names to be copied (default is all)')
        parser.add_argument('--destination', '-d', required=True, help='Destination project name')
        parser.add_argument('--name', '-na', required=False,
                            help='New table name (required if source and destination are the same, ignored if more than one table is to be copied)')
        parser.add_argument('--incremental', '-i', action='store_true', help='Incremental copy')
        parser.add_argument('--nulls', '-nu', action='store_true', help='Copy the null values')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(self, args):
        """
        Execute copy data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = CopyTableCommand(client, args.verbose).copy_tables(args.project, args.tables, args.destination, args.name, args.incremental, args.nulls)
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def copy_tables(self, project: str, tables: list, destination: str, name: str, incremental: bool, nulls: bool) -> dict:
        """
        Execute copy data command

        :param project: The project name
        :param tables: List of table names to be copied (default is all)
        :param destination: Destination project name
        :param name: New table name (required if source and destination are the same, ignored if more than one table is to be copied)
        :param incremental: Incremental copy
        :param nulls: Copy the null values
        """
        tables_ = tables
        if not tables:
            tables_ = self._retrieve_datasource_tables(project)
        copier = io.OpalCopier.build(client=self.client, datasource=project, tables=tables_,
                                    destination=destination, name=name,
                                    incremental=incremental, nulls=nulls,
                                    verbose=self.verbose)
        response = copier.submit()
        return response.from_json()
    
    def _retrieve_datasource_tables(self, project: str) -> list:
        request = self.client.new_request()
        if self.verbose:
            request.verbose()
        response = request.fail_on_error().get().resource(
            core.UriBuilder(['datasource', project, 'tables']).build()).send().from_json()

        tables = []
        for table in response:
            tables.append(str(table['name']))

        return tables
