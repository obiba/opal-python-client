"""
Delete some tables.
"""

import obiba_opal.core as core
import obiba_opal.perm as perm

class DeleteTableService:
    """
    Delete some tables in a project.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
        """
        Add command specific options
        """
        parser.add_argument('--project', '-pr', required=True, help='Project name to which the tables belong')
        parser.add_argument('--tables', '-t', nargs='+', required=False,
                            help='List of table names which will be deleted (default is all)')

    @classmethod
    def do_command(self, args):
        """
        Execute delete command
        """
        # Build and send requests
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            DeleteTableService(client, args.verbose).delete_tables(args.project, args.tables)
        finally:
            client.close()

    def delete_tables(self, project: str, tables: list = None):
        """
        Execute delete table command

        :param client: Opal connection object
        :param project: The project name
        :param tables: List of table names to be deleted (default is all)
        :param verbose: Verbose requests
        """
        tables_ = tables
        if not tables:
            tables_ = self._retrieve_datasource_tables(project)
        
        for table in tables_:
            request = self.client.new_request()
            if self.verbose:
                request.verbose()
            # send request
            try:
                response = request.delete().resource(core.UriBuilder(['datasource', project, 'table', table]).build()).send()
                # format response
                if self.verbose and response.code != 200:
                    print(response.content)
            except Exception as e:
                print(Exception, e)
    
    def _retrieve_datasource_tables(self, project: str) -> list:
        request = self.client.new_request()
        request.fail_on_error()
        if self.verbose:
            request.verbose()
        response = request.get().resource(core.UriBuilder(['datasource', project, 'tables']).build()).send()

        tables = []
        for table in response.from_json():
            tables.append(str(table['name']))

        return tables
