"""
Opal dictionary annotations extraction.
"""

import argparse
import csv
import obiba_opal.core as core
import sys


class ExportAnnotationsService:
    """
    Export dictionary annotations for later import.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
        """
        Add command specific options
        """
        parser.add_argument('name',
                            help='Fully qualified name of a datasource/project or a table or a variable, for instance: opal-data or opal-data.questionnaire or opal-data.questionnaire:Q1. Wild cards can also be used, for instance: "opal-data.*", etc.')
        parser.add_argument('--output', '-out', help='CSV/TSV file to output (default is stdout)',
                            type=argparse.FileType('w'), default=sys.stdout)
        parser.add_argument('--locale', '-l', required=False,
                            help='Exported locale (default is none)')
        parser.add_argument('--separator', '-s', required=False,
                            help='Separator char for CSV/TSV format (default is the tabulation character)')
        parser.add_argument('--taxonomies', '-tx', nargs='+', required=False,
                            help='The list of taxonomy names of interest (default is any that are found in the variable attributes)')

    @classmethod
    def do_command(self, args):
        """
        Execute command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        sep = args.separator if args.separator else '\t'
        ExportAnnotationsService(client, args.verbose)._export_annotations(args.name, args.output, sep = sep, taxonomies = args.taxonomies, locale = args.locale)

    def export_project_annotations(self, project: str, output, sep: str = '\t', taxonomies: list = None, locale: str = None):
        self._export_annotations(project, output, sep, taxonomies, locale)

    def export_table_annotations(self, project: str, table: str, output, sep: str = '\t', taxonomies: list = None, locale: str = None):
        self._export_annotations('%s.%s' % (project, table), output, sep, taxonomies, locale)

    def export_variable_annotations(self, project: str, table: str, variable: str, output, sep: str = '\t', taxonomies: list = None, locale: str = None):
        self._export_annotations('%s.%s:%s' % (project, table, variable), output, sep, taxonomies, locale)

    def _export_annotations(self, name: str, output, sep: str = '\t', taxonomies: list = None, locale: str = None):
        writer = csv.writer(output, delimiter=sep)
        writer.writerow(['project', 'table', 'variable', 'namespace', 'name', 'value'])
        self._handle_item(writer, name, taxonomies, locale)

    def _handle_item(self, writer, name: str, taxonomies: list = None, locale: str = None):
        # print 'Handling ' + name
        request = self.client.new_request()
        request.fail_on_error().accept_json()

        if self.verbose:
            request.verbose()

        # send request
        resolver = core.MagmaNameResolver(name)
        request.get().resource(resolver.get_ws())
        response = request.send()

        if resolver.is_datasources():
            raise Exception('Wildcard not allowed for datasources/projects')

        res = response.from_json()
        if resolver.is_datasource():
            self._handle_datasource(writer, res, taxonomies, locale)
        if resolver.is_table():
            self._handle_table(writer, res, taxonomies, locale)
        if resolver.is_variables():
            for variable in res:
                self._handle_variable(writer, resolver.datasource, resolver.table, variable, taxonomies, locale)
        if resolver.is_variable():
            self._handle_variable(writer, resolver.datasource, resolver.table, res, taxonomies, locale)

    def _handle_datasource(self, writer, datasourceObject, taxonomies: list = None, locale: str = None):
        for table in datasourceObject['table']:
            self._handle_item(writer, datasourceObject['name'] + '.' + table + ':*', taxonomies, locale)

    def _handle_table(self, writer, tableObject, taxonomies: list = None, locale: str = None):
        self._handle_item(writer, tableObject['datasourceName'] + '.' + tableObject['name'] + ':*', taxonomies, locale)

    def _handle_variable(self, writer, datasource, table, variableObject, taxonomies: list = None, locale: str = None):
        if 'attributes' in variableObject:
            for attribute in variableObject['attributes']:
                do_search = 'namespace' in attribute and 'locale' in attribute \
                            and locale in attribute['locale'] \
                    if locale \
                    else 'namespace' in attribute and 'locale' not in attribute
                if do_search:
                    if not taxonomies or attribute['namespace'] in taxonomies:
                        row = [datasource, table, variableObject['name'], attribute['namespace'], attribute['name'],
                            attribute['value']]
                        writer.writerow(row)