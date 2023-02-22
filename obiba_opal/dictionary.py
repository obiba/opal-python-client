"""
Opal data dictionary.
"""

import obiba_opal.core as core

class DictionaryService:
    """
    Dictionary command, to get meta-data.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
        """
        Add variable command specific options
        """
        parser.add_argument('name',
                            help='Fully qualified name of a datasource/project or a table or a variable, for instance: opal-data or opal-data.questionnaire or opal-data.questionnaire:Q1. Wild cards can also be used, for instance: "*", "opal-data.*", etc.')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(self, args):
        """
        Execute variable command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = DictionaryService(client, args.verbose)._get_dictionary(args.name)
        
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_datasources(self) -> list:
        """
        Get the list of datasources.
        """
        return self._get_dictionary('*')

    def get_datasource(self, project: str) -> dict:
        """
        Get a datasource.

        :param project: The project name associated to the datasource
        """
        return self._get_dictionary(project)
    
    def get_tables(self, project: str) -> list:
        """
        Get the tables of a datasource.
        
        :param project: The project name associated to the datasource
        """
        return self._get_dictionary('%s.*' % project)

    def get_table(self, project: str, table: str) -> dict:
        """
        Get a table of a datasource.

        :param project: The project name associated to the datasource
        :param table: The table name
        """
        return self._get_dictionary('%s.%s' % (project, table))

    def get_variables(self, project: str, table: str) -> list:
        """
        Get the variables of a table in a datasource.

        :param project: The project name associated to the datasource
        :param table: The table name
        """
        return self._get_dictionary('%s.%s:*' % (project, table))

    def get_variable(self, project: str, table: str, variable: str) -> list:
        """
        Get a variable of a table in a datasource.

        :param project: The project name associated to the datasource
        :param table: The table name
        :param variable: The variable name
        """
        return self._get_dictionary('%s.%s:%s' % (project, table, variable))
    
    def _get_dictionary(self, name: str) -> any:
        """
        Get dictionary items by their full name, with wild-card support.

        :param name: Fully qualified name of a datasource/project or a table or a variable, for instance: opal-data or opal-data.questionnaire or opal-data.questionnaire:Q1. Wild cards can also be used, for instance: "*", "opal-data.*", etc.
        """
        request = self.client.new_request()
        request.fail_on_error().accept_json()

        if self.verbose:
            request.verbose()

        # send request
        request.get().resource(core.MagmaNameResolver(name).get_ws())
        response = request.send()
        return response.from_json()
