"""
Opal data dictionary and annotations.
"""

import obiba_opal.core as core
import argparse
import csv
import sys
import pprint
import urllib.parse


class DictionaryService:
    """
    Dictionary command, to get meta-data.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add variable command specific options
        """
        parser.add_argument(
            "name",
            help="Fully qualified name of a datasource/project or a table or "
            "a variable, for instance: opal-data or opal-data.questionnaire "
            "or opal-data.questionnaire:Q1. Wild cards can also be used, "
            'for instance: "*", "opal-data.*", etc.',
        )
        parser.add_argument(
            "--json",
            "-j",
            action="store_true",
            help="Pretty JSON formatting of the response",
        )
        parser.add_argument(
            "--excel",
            "-xls",
            required=False,
            help="Full path of the target data dictionary Excel file.",
        )

    @classmethod
    def do_command(cls, args):
        """
        Execute variable command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = DictionaryService(client, args.verbose)

            if args.excel:
                res = service._get_dictionary_as_excel(args.name)
                with open(args.excel, mode="wb") as excelFile:
                    excelFile.write(res)
            else:
                res = service._get_dictionary()

                # format response
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_datasources(self) -> list:
        """
        Get the list of datasources.
        """
        return self._get_dictionary("*")

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
        return self._get_dictionary(f"{project}.*")

    def get_table(self, project: str, table: str) -> dict:
        """
        Get a table of a datasource.

        :param project: The project name associated to the datasource
        :param table: The table name
        """
        return self._get_dictionary(f"{project}.{table}")

    def get_variables(self, project: str, table: str) -> list:
        """
        Get the variables of a table in a datasource.

        :param project: The project name associated to the datasource
        :param table: The table name
        """
        return self._get_dictionary(f"{project}.{table}:*")

    def get_variable(self, project: str, table: str, variable: str) -> list:
        """
        Get a variable of a table in a datasource.

        :param project: The project name associated to the datasource
        :param table: The table name
        :param variable: The variable name
        """
        return self._get_dictionary(f"{project}.{table}:{variable}")

    def delete_tables(self, project: str, tables: list = None):
        """
        Delete provided or all tables.

        :param client: Opal connection object
        :param project: The project name
        :param tables: List of table names to be deleted (default is all)
        :param verbose: Verbose requests
        """
        tables_ = tables
        if not tables:
            tables_ = self.get_tables(project)
            tables_ = [x["name"] for x in tables_]

        for table in tables_:
            request = self.client.new_request()
            if self.verbose:
                request.verbose()
            request.fail_on_error().delete().resource(
                core.UriBuilder(["datasource", project, "table", table]).build()
            ).send()

    def _get_dictionary(self, name: str) -> any:
        """
        Get dictionary items by their full name, with wild-card support.

        :param name: Fully qualified name of a datasource/project or a table
                    or a variable, for instance: opal-data or
                    opal-data.questionnaire or opal-data.questionnaire:Q1.
                    Wild cards can also be used, for instance: "*",
                    "opal-data.*", etc.
        """
        request = self.client.new_request()
        request.fail_on_error().accept_json()

        if self.verbose:
            request.verbose()

        # send request
        request.get().resource(core.MagmaNameResolver(name).get_ws())
        response = request.send()
        return response.from_json()

    def _get_dictionary_as_excel(self, name: str) -> any:
        """
        Get dictionary items by their full name, with wild-card support.

        :param name: Fully qualified name of a datasource/project or a table
                    or a variable, for instance: opal-data or
                    opal-data.questionnaire or opal-data.questionnaire:Q1.
                    Wild cards can also be used, for instance: "*",
                    "opal-data.*", etc.
        """
        request = self.client.new_request()
        request.fail_on_error().accept("application/vnd.ms-excel")

        if self.verbose:
            request.verbose()

        # send request

        resolver = core.MagmaNameResolver(name)

        if not resolver.is_variables():
            raise Exception(
                "Excel data dictionaries must be for all variables, use '<datasource>.<table>:*' format for resource."
            )

        request.get().resource(f"{resolver.get_ws()}/excel")
        response = request.send()

        return response.content


class ExportAnnotationsService:
    """
    Export dictionary annotations for later import.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        parser.add_argument(
            "name",
            help="Fully qualified name of a datasource/project or a table or "
            "a variable, for instance: opal-data or opal-data.questionnaire "
            "or opal-data.questionnaire:Q1. Wild cards can also be used, "
            'for instance: "opal-data.*", etc.',
        )
        parser.add_argument(
            "--output",
            "-out",
            help="CSV/TSV file to output (default is stdout)",
            type=argparse.FileType("w"),
            default=sys.stdout,
        )
        parser.add_argument("--locale", "-l", required=False, help="Exported locale (default is none)")
        parser.add_argument(
            "--separator",
            "-s",
            required=False,
            help="Separator char for CSV/TSV format (default is the tabulation character)",
        )
        parser.add_argument(
            "--taxonomies",
            "-tx",
            nargs="+",
            required=False,
            help="The list of taxonomy names of interest (default is any that are found in the variable attributes)",
        )

    @classmethod
    def do_command(cls, args):
        """
        Execute command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        sep = args.separator if args.separator else "\t"
        ExportAnnotationsService(client, args.verbose)._export_annotations(
            args.name,
            args.output,
            sep=sep,
            taxonomies=args.taxonomies,
            locale=args.locale,
        )

    def export_project_annotations(
        self,
        project: str,
        output,
        sep: str = "\t",
        taxonomies: list = None,
        locale: str = None,
    ):
        self._export_annotations(project, output, sep, taxonomies, locale)

    def export_table_annotations(
        self,
        project: str,
        table: str,
        output,
        sep: str = "\t",
        taxonomies: list = None,
        locale: str = None,
    ):
        self._export_annotations(f"{project}.{table}", output, sep, taxonomies, locale)

    def export_variable_annotations(
        self,
        project: str,
        table: str,
        variable: str,
        output,
        sep: str = "\t",
        taxonomies: list = None,
        locale: str = None,
    ):
        self._export_annotations(f"{project}.{table}:{variable}", output, sep, taxonomies, locale)

    def _export_annotations(
        self,
        name: str,
        output,
        sep: str = "\t",
        taxonomies: list = None,
        locale: str = None,
    ):
        writer = csv.writer(output, delimiter=sep)
        writer.writerow(["project", "table", "variable", "namespace", "name", "value"])
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
            raise Exception("Wildcard not allowed for datasources/projects")

        res = response.from_json()
        if resolver.is_datasource():
            self._handle_datasource(writer, res, taxonomies, locale)
        if resolver.is_table():
            self._handle_table(writer, res, taxonomies, locale)
        if resolver.is_variables():
            for variable in res:
                self._handle_variable(
                    writer,
                    resolver.datasource,
                    resolver.table,
                    variable,
                    taxonomies,
                    locale,
                )
        if resolver.is_variable():
            self._handle_variable(writer, resolver.datasource, resolver.table, res, taxonomies, locale)

    def _handle_datasource(self, writer, datasourceObject, taxonomies: list = None, locale: str = None):
        for table in datasourceObject["table"]:
            self._handle_item(
                writer,
                datasourceObject["name"] + "." + table + ":*",
                taxonomies,
                locale,
            )

    def _handle_table(self, writer, tableObject, taxonomies: list = None, locale: str = None):
        self._handle_item(
            writer,
            tableObject["datasourceName"] + "." + tableObject["name"] + ":*",
            taxonomies,
            locale,
        )

    def _handle_variable(
        self,
        writer,
        datasource,
        table,
        variableObject,
        taxonomies: list = None,
        locale: str = None,
    ):
        if "attributes" in variableObject:
            for attribute in variableObject["attributes"]:
                do_search = (
                    "namespace" in attribute and "locale" in attribute and locale in attribute["locale"]
                    if locale
                    else "namespace" in attribute and "locale" not in attribute
                )
                if do_search and (not taxonomies or attribute["namespace"] in taxonomies):
                    row = [
                        datasource,
                        table,
                        variableObject["name"],
                        attribute["namespace"],
                        attribute["name"],
                        attribute["value"],
                    ]
                    writer.writerow(row)


class ImportAnnotationsService:
    """
    Import dictionary annotations from previous export.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        parser.add_argument(
            "--input",
            "-in",
            help='CSV/TSV input file, typically the output of the "export-annot" command (default is stdin)',
            type=argparse.FileType("r"),
            default=sys.stdin,
        )
        parser.add_argument(
            "--locale",
            "-l",
            required=False,
            help="Destination annotation locale (default is none)",
        )
        parser.add_argument(
            "--separator",
            "-s",
            required=False,
            help="Separator char for CSV/TSV format (default is the tabulation character)",
        )
        parser.add_argument(
            "--destination",
            "-d",
            required=False,
            help="Destination datasource name (default is the one(s) specified in the input file)",
        )
        parser.add_argument(
            "--tables",
            "-t",
            nargs="+",
            required=False,
            help="The list of tables which variables are to be annotated "
            "(defaults to all that are found in the input file)",
        )
        parser.add_argument(
            "--taxonomies",
            "-tx",
            nargs="+",
            required=False,
            help="The list of taxonomy names of interest (default is any that is found in the input file)",
        )

    @classmethod
    def do_command(cls, args):
        """
        Execute command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        service = ImportAnnotationsService(client, args.verbose)
        sep = args.separator if args.separator else "\t"
        service.import_annotations(
            args.input,
            sep=sep,
            tables=args.tables,
            taxonomies=args.taxonomies,
            destination=args.destination,
            locale=args.locale,
        )

    def import_annotations(
        self,
        input,
        sep: str = "\t",
        tables: list = None,
        taxonomies: list = None,
        destination: str = None,
        locale: str = None,
    ):
        reader = csv.reader(input, delimiter=sep)
        next(reader)  # skip header
        value_map = {}
        for row in reader:
            self._append_row(value_map, row, tables=tables, taxonomies=taxonomies)
        if self.verbose:
            pp = pprint.PrettyPrinter(indent=2)
            pp.pprint(value_map)
        for datasource in value_map:
            for table in value_map[datasource]:
                if not tables or table in tables:
                    for namespace in value_map[datasource][table]:
                        for name in value_map[datasource][table][namespace]:
                            for value in value_map[datasource][table][namespace][name]:
                                ds = destination if destination else datasource
                                variables = value_map[datasource][table][namespace][name][value]
                                self._annotate(ds, table, namespace, name, value, variables, locale)

    def _annotate(self, datasource, table, namespace, name, value, variables, locale: str = None):
        request = self.client.new_request()
        request.fail_on_error().accept_json()
        params = {"namespace": namespace, "name": name, "value": value}

        if locale:
            params["locale"] = locale

        builder = core.UriBuilder(
            ["datasource", datasource, "table", table, "variables", "_attribute"],
            params=params,
        )
        form = "&".join([urllib.parse.urlencode({"variable": x}) for x in variables])
        if self.verbose:
            request.verbose()

        request.put().resource(builder.build()).content_type_form_urlencoded().content(form).send()

    def _append_row(self, dictionary, row, tables=None, taxonomies=None):
        if row[0] not in dictionary:
            dictionary[row[0]] = {}
        self._append_table(dictionary, row, tables, taxonomies)

    def _append_table(self, dictionary, row, tables=None, taxonomies=None):
        if not tables or row[1] in tables:
            if row[1] not in dictionary[row[0]]:
                dictionary[row[0]][row[1]] = {}
            if not taxonomies or row[3] in taxonomies:
                self._append_taxonomy(dictionary, row)

    def _append_taxonomy(self, dictionary, row):
        if row[3] not in dictionary[row[0]][row[1]]:
            dictionary[row[0]][row[1]][row[3]] = {}
        self._append_vocabulary(dictionary, row)

    def _append_vocabulary(self, dictionary, row):
        if row[4] not in dictionary[row[0]][row[1]][row[3]]:
            dictionary[row[0]][row[1]][row[3]][row[4]] = {}
        self._append_value(dictionary, row)

    def _append_value(self, dictionary, row):
        if row[5] not in dictionary[row[0]][row[1]][row[3]][row[4]]:
            dictionary[row[0]][row[1]][row[3]][row[4]][row[5]] = []
        if row[2] not in dictionary[row[0]][row[1]][row[3]][row[4]][row[5]]:
            dictionary[row[0]][row[1]][row[3]][row[4]][row[5]].append(row[2])
