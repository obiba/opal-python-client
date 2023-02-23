"""
Data export in many different formats.
"""

import json
import obiba_opal.core as core
import obiba_opal.io as io


class ExportPluginCommand:
    """
    Data export to a datasource plugin.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Project name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--name', '-n', required=True, help='Opal datasource plugin name')
        parser.add_argument('--config', '-c', required=True, help='A JSON file containing the export configuration')
        parser.add_argument('--incremental', '-i', action='store_true', help='Incremental export')
        parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute export data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        config = json.loads(open(args.config).read())
        try:
            res = cls(client, args.verbose) \
                .export(args.name, args.datasource, args.tables, config, args.identifiers, args.incremental)
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def export(self, name: str, project: str, tables: list, config: str, identifiers: str = None, incremental: bool = False) -> dict:
        """
        Export tables using a plugin.

        :param name: The name of the plugin.
        :param project: The project name
        :param tables: The table names to export
        :param config: The plugin configuration dictionary
        :param identifiers: The name of the ID mapping
        :param incremental: Incremental export
        """
        configStr = json.dumps(config)
        exporter = io.OpalExporter.build(client=self.client, datasource=project , tables=tables, 
                                         identifiers=identifiers, output=configStr,
                                         incremental=incremental,
                                         verbose=self.verbose)
        response = exporter.submit(name)
        return response.from_json()

class ExportCSVCommand:
    """
    Export some tables in CSV format.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose
    
    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Project name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--output', '-out', required=True, help='Output directory name')
        parser.add_argument('--incremental', '-i', action='store_true', help='Incremental export')
        parser.add_argument('--id-name', '-in', required=False, help='Name of the ID column name')
        parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
        parser.add_argument('--no-multilines', '-nl', action='store_true',
                            help='Do not write value sequences as multiple lines')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute export data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = cls(client, args.verbose) \
                .export(args.datasource, args.tables, args.output, args.id_name, args.identifiers, args.incremental, not args.no_multilines)
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def export(self, project: str, tables: list, output: str, id_name: str = None, identifiers: str = None, incremental: bool = False, multilines: bool = True) -> dict:
        """
        Export tables in CSV files.

        :param project: The project name
        :param tables: The table names to export
        :param output: The output directory path
        :param id_name: The name of the ID column name
        :param identifiers: The name of the ID mapping
        :param incremental: Incremental export
        :param multilines: Write value sequences as multiple lines
        """
        exporter = io.OpalExporter.build(client=self.client, datasource=project , tables=tables, entityIdNames = id_name,
                                         identifiers=identifiers, output=output,
                                         incremental=incremental,
                                         multilines=multilines, verbose=self.verbose)
        response = exporter.submit('csv')
        return response.from_json()

class ExportRDSCommand:
    """
    Data export in RDS (using R).
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Project name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--output', '-out', required=True, help='Output file name (.rds)')
        parser.add_argument('--id-name', '-in', required=False, help='Name of the ID column name')
        parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
        parser.add_argument('--no-multilines', '-nl', action='store_true',
                            help='Do not write value sequences as multiple lines')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute export data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = cls(client, args.verbose) \
                .export(args.datasource, args.tables, args.output, args.id_name, args.identifiers, args.incremental, not args.no_multilines)
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def export(self, project: str, tables: list, output: str, id_name: str = None, identifiers: str = None, incremental: bool = False, multilines: bool = True) -> dict:
        """
        Export tables in a RDS file.

        :param project: The project name
        :param tables: The table names to export
        :param output: The output file path (.rds)
        :param id_name: The name of the ID column name
        :param identifiers: The name of the ID mapping
        :param incremental: Incremental export
        :param multilines: Write value sequences as multiple lines
        """
        if not (output.endswith('.rds')):
            raise Exception('Output must be a RDS file (.rds).')
        
        exporter = io.OpalExporter.build(client=self.client, datasource=project , tables=tables, entityIdNames = id_name,
                                         identifiers=identifiers, output=output,
                                         incremental=incremental,
                                         multilines=multilines, verbose=self.verbose)
        response = exporter.submit('RDS')
        return response.from_json()

class ExportRSASCommand:
    """
    Data export in SAS (using R).
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Project name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--output', '-out', required=True,
                            help='Output file name (.sas7bdat or .xpt (Transport format))')
        parser.add_argument('--id-name', '-in', required=False, help='Name of the ID column name')
        parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
        parser.add_argument('--no-multilines', '-nl', action='store_true',
                            help='Do not write value sequences as multiple lines')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute export data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = cls(client, args.verbose) \
                .export(args.datasource, args.tables, args.output, args.id_name, args.identifiers, args.incremental, not args.no_multilines)
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def export(self, project: str, tables: list, output: str, id_name: str = None, identifiers: str = None, incremental: bool = False, multilines: bool = True) -> dict:
        """
        Export tables in a SAS file.

        :param project: The project name
        :param tables: The table names to export
        :param output: The output file path (.sas7bdat or .xpt)
        :param id_name: The name of the ID column name
        :param identifiers: The name of the ID mapping
        :param incremental: Incremental export
        :param multilines: Write value sequences as multiple lines
        """
        if not (output.endswith('.sas7bdat')) and not (output.endswith('.xpt')):
            raise Exception('Output must be a SAS file (.sas7bdat or .xpt).')
    
        exporter = io.OpalExporter.build(client=self.client, datasource=project , tables=tables, entityIdNames = id_name,
                                         identifiers=identifiers, output=output,
                                         incremental=incremental,
                                         multilines=multilines, verbose=self.verbose)
        response = None
        if output.endswith('.sas7bdat'):
            response = exporter.submit('RSAS')
        else:
            response = exporter.submit('RXPT')
        return response.from_json()


class ExportRSPSSCommand:
    """
    Data export in SPSS (using R).
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Project name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--output', '-out', required=True, help='Output file name (.sav or .zsav (compressed format))')
        parser.add_argument('--id-name', '-in', required=False, help='Name of the ID column name')
        parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
        parser.add_argument('--no-multilines', '-nl', action='store_true',
                            help='Do not write value sequences as multiple lines')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute export data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = cls(client, args.verbose) \
                .export(args.datasource, args.tables, args.output, args.id_name, args.identifiers, args.incremental, not args.no_multilines)
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def export(self, project: str, tables: list, output: str, id_name: str = None, identifiers: str = None, incremental: bool = False, multilines: bool = True) -> dict:
        """
        Export tables in a SPSS file.

        :param project: The project name
        :param tables: The table names to export
        :param output: The output file path (.sav or .zsav)
        :param id_name: The name of the ID column name
        :param identifiers: The name of the ID mapping
        :param incremental: Incremental export
        :param multilines: Write value sequences as multiple lines
        """
        if not (output.endswith('.sav')) and not (output.endswith('.zsav')):
            raise Exception('Output must be a SPSS file (.sav or .zsav).')

        exporter = io.OpalExporter.build(client=self.client, datasource=project , tables=tables, entityIdNames = id_name,
                                         identifiers=identifiers, output=output,
                                         incremental=incremental,
                                         multilines=multilines, verbose=self.verbose)
        response = None
        if output.endswith('.sav'):
            response = exporter.submit('RSPSS')
        else:
            response = exporter.submit('RZSPSS')
        return response.from_json()


class ExportRSTATACommand:
    """
    Data export in SAS (using R).
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Project name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--output', '-out', required=True, help='Output file name (.dta)')
        parser.add_argument('--id-name', '-in', required=False, help='Name of the ID column name')
        parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
        parser.add_argument('--no-multilines', '-nl', action='store_true',
                            help='Do not write value sequences as multiple lines')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute export data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = cls(client, args.verbose) \
                .export(args.datasource, args.tables, args.output, args.id_name, args.identifiers, args.incremental, not args.no_multilines)
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def export(self, project: str, tables: list, output: str, id_name: str = None, identifiers: str = None, incremental: bool = False, multilines: bool = True) -> dict:
        """
        Export tables in a STATA file.

        :param project: The project name
        :param tables: The table names to export
        :param output: The output file path (.dta)
        :param id_name: The name of the ID column name
        :param identifiers: The name of the ID mapping
        :param incremental: Incremental export
        :param multilines: Write value sequences as multiple lines
        """
        if not (output.endswith('.dta')):
            raise Exception('Output must be a Stata file (.dta).')

        exporter = io.OpalExporter.build(client=self.client, datasource=project , tables=tables, entityIdNames = id_name,
                                         identifiers=identifiers, output=output,
                                         incremental=incremental,
                                         multilines=multilines, verbose=self.verbose)
        response = exporter.submit('RSTATA')
        return response.from_json()


class ExportSQLCommand:
    """
    Data export to a SQL database.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Project name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--database', '-db', required=True, help='Name of the SQL database')
        parser.add_argument('--incremental', '-i', action='store_true', help='Incremental export')
        parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute export data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = cls(client, args.verbose) \
                .export(args.datasource, args.tables, args.database, args.identifiers, args.incremental)
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()
    
    def export(self, project: str, tables: list, database: str, identifiers: str = None, incremental: bool = False):
        """
        Export tables in a SQL database.

        :param project: The project name
        :param tables: The table names to export
        :param database: The SQL database name
        :param identifiers: The name of the ID mapping
        :param incremental: Incremental export
        """
        exporter = io.OpalExporter.build(client=self.client, datasource=project , tables=tables,
                                                    identifiers=identifiers, output=database,
                                                    incremental=incremental, verbose=self.verbose)
        response = exporter.submit('jdbc')
        return response.from_json()

class ExportXMLCommand:
    """
    Data export in XML.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Project name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--output', '-out', required=True, help='Output zip file name that will be exported')
        parser.add_argument('--incremental', '-i', action='store_true', help='Incremental export')
        parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute export data command
        """
        # Check output filename extension
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = cls(client, args.verbose) \
                .export(args.datasource, args.tables, args.output, args.identifiers, args.incremental)
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def export(self, project: str, tables: list, output: str, identifiers: str = None, incremental: bool = False) -> dict:
        """
        Export tables in an Opal archive file.

        :param project: The project name
        :param tables: The table names to export
        :param output: The output file path (.zip)
        :param id_name: The name of the ID column name
        :param identifiers: The name of the ID mapping
        :param incremental: Incremental export
        :param multilines: Write value sequences as multiple lines
        """
        if not (output.endswith('.zip')):
            raise Exception('Output must be a zip file.')

        exporter = io.OpalExporter.build(client=self.client, datasource=project , tables=tables,
                                         identifiers=identifiers, output=output,
                                         incremental=incremental,
                                         verbose=self.verbose)
        response = exporter.submit('xml')
        return response.from_json()
        

class ExportVCFCommand:
    """
    Export some VCF/BCF files.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        parser.add_argument('--project', '-pr', required=True,
                            help='Project name from which genotypes data will be exported')
        parser.add_argument('--vcf', '-vcf', nargs='+', required=True, help='List of VCF/BCF file names')
        parser.add_argument('--destination', '-d', required=True, help='Destination folder (in Opal file system)')
        parser.add_argument('--filter-table', '-f', required=False,
                            help='Participant table name to be used to filter the samples by participant ID (only relevant if there is a sample-participant mapping defined)')
        parser.add_argument('--no-case-controls', '-nocc', action='store_true',
                            help='Do not include case control samples (only relevant if there is a sample-participant mapping defined)')

    @classmethod
    def do_command(cls, args):
        """
        Execute delete command
        """
        # Build and send requests
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = ExportVCFCommand(client, args.verbose) \
                .export(args.project, args.vcf, args.destination, not args.no_case_controls, args.filter_table)
        finally:
            client.close()

    def export(self, project: str, vcf: list, destination: str, case_controls: bool = True, filter_table: str = None) -> dict:
        """
        Export VCF/BCF files.

        :param project: The project name
        :param vcf: The list of VCF/BCF file names
        :param destination: The output folder path
        :param case_controls: Include case control samples (only relevant if there is a sample-participant mapping defined)
        :param filter_table: Participant table name to be used to filter the samples by participant ID (only relevant if there is a sample-participant mapping defined)
        """
        request = self.client.new_request()
        request.fail_on_error().accept_json().content_type_json()
        if self.verbose:
            request.verbose()

        options = {
            'project': project,
            'names': vcf,
            'destination': destination,
            'caseControl': case_controls
        }
        if filter_table:
            options['table'] = filter_table

        # send request
        uri = core.UriBuilder(['project', project, 'commands', '_export_vcf']).build()
        response = request.resource(uri).post().content(json.dumps(options)).send()
        return response.from_json()