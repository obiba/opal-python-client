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
    def add_arguments(self, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Datasource name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--name', '-n', required=True, help='Opal datasource plugin name')
        parser.add_argument('--config', '-c', required=True, help='A JSON file containing the export configuration')
        parser.add_argument('--incremental', '-i', action='store_true', help='Incremental export')
        parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(self, args):
        """
        Execute export data command
        """
        # Build and send request
        configStr = json.dumps(json.loads(open(args.config).read()))
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            exporter = io.OpalExporter.build(client=client, datasource=args.datasource, tables=args.tables,
                                                    identifiers=args.identifiers, output=configStr,
                                                    incremental=args.incremental, verbose=args.verbose)
            response = exporter.submit(args.name)
            # format response
            core.Formatter.print_json(response.from_json(), args.json)
        finally:
            client.close()

class ExportCSVCommand:
    """
    Export some tables in CSV format.
    """

    @classmethod
    def add_arguments(self, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Datasource name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--output', '-out', required=True, help='Output directory name')
        parser.add_argument('--incremental', '-i', action='store_true', help='Incremental export')
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
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            exporter = io.OpalExporter.build(client=client, datasource=args.datasource, tables=args.tables, entityIdNames = args.id_name,
                                                    identifiers=args.identifiers, output=args.output,
                                                    incremental=args.incremental,
                                                    multilines=(not args.no_multilines), verbose=args.verbose)
            response = exporter.submit('csv')
            # format response
            core.Formatter.print_json(response.from_json(), args.json)
        finally:
            client.close()


class ExportRDSCommand:
    """
    Data export in RDS (using R).
    """

    @classmethod
    def add_arguments(self, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Datasource name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--output', '-out', required=True, help='Output file name (.dta)')
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
        if not (args.output.endswith('.rds')):
            raise Exception('Output must be a RDS file (.rds).')

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            exporter = io.OpalExporter.build(client=client, datasource=args.datasource, tables=args.tables, entityIdNames = args.id_name,
                                                    identifiers=args.identifiers, output=args.output, incremental=False,
                                                    multilines=(not args.no_multilines), verbose=args.verbose)
            response = exporter.submit('RDS')
            # format response
            core.Formatter.print_json(response.from_json(), args.json)
        finally:
            client.close()


class ExportRSASCommand:
    """
    Data export in SAS (using R).
    """

    @classmethod
    def add_arguments(self, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Datasource name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--output', '-out', required=True,
                            help='Output file name (.sas7bdat or .xpt (Transport format))')
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
        if not (args.output.endswith('.sas7bdat')) and not (args.output.endswith('.xpt')):
            raise Exception('Output must be a SAS file (.sas7bdat or .xpt).')

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            exporter = io.OpalExporter.build(client=client, datasource=args.datasource, tables=args.tables, entityIdNames = args.id_name,
                                                    identifiers=args.identifiers, output=args.output, incremental=False,
                                                    multilines=(not args.no_multilines), verbose=args.verbose)
            response = None
            if args.output.endswith('.sas7bdat'):
                response = exporter.submit('RSAS')
            else:
                response = exporter.submit('RXPT')
            # format response
            core.Formatter.print_json(response.from_json(), args.json)
        finally:
            client.close()


class ExportRSPSSCommand:
    """
    Data export in SPSS (using R).
    """

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


class ExportRSTATACommand:
    """
    Data export in SAS (using R).
    """

    @classmethod
    def add_arguments(self, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Datasource name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--output', '-out', required=True, help='Output file name (.dta)')
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
        if not (args.output.endswith('.dta')):
            raise Exception('Output must be a Stata file (.dta).')

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            exporter = io.OpalExporter.build(client=client, datasource=args.datasource, tables=args.tables, entityIdNames = args.id_name,
                                                    identifiers=args.identifiers, output=args.output, incremental=False,
                                                    multilines=(not args.no_multilines), verbose=args.verbose)
            response = exporter.submit('RSTATA')
            # format response
            core.Formatter.print_json(response.from_json(), args.json)
        finally:
            client.close()


class ExportSQLCommand:
    """
    Data export to a SQL database.
    """

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

class ExportXMLCommand:
    """
    Data export in XML.
    """

    @classmethod
    def add_arguments(self, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--datasource', '-d', required=True, help='Datasource name')
        parser.add_argument('--tables', '-t', nargs='+', required=True, help='The list of tables to be exported')
        parser.add_argument('--output', '-out', required=True, help='Output zip file name that will be exported')
        parser.add_argument('--incremental', '-i', action='store_true', help='Incremental export')
        parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(self, args):
        """
        Execute export data command
        """
        # Check output filename extension
        if not (args.output.endswith('.zip')):
            raise Exception('Output must be a zip file.')

        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            exporter = io.OpalExporter.build(client=client, datasource=args.datasource, tables=args.tables,
                                                    identifiers=args.identifiers, output=args.output,
                                                    incremental=args.incremental,
                                                    verbose=args.verbose)
            response = exporter.submit('xml')
            # format response
            core.Formatter.print_json(response.from_json(), args.json)
        finally:
            client.close()


class ExportVCFCommand:
    """
    Export some VCF/BCF files.
    """

    @classmethod
    def add_arguments(self, parser):
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
    def do_command(self, args):
        """
        Execute delete command
        """
        # Build and send requests
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            request = client.new_request()
            request.fail_on_error().accept_json().content_type_json()
            if args.verbose:
                request.verbose()

            options = {
                'project': args.project,
                'names': args.vcf,
                'destination': args.destination,
                'caseControl': not args.no_case_controls
            }
            if args.filter_table:
                options['table'] = args.filter_table

            # send request
            uri = core.UriBuilder(['project', args.project, 'commands', '_export_vcf']).build()
            request.resource(uri).post().content(json.dumps(options)).send()
        finally:
            client.close()