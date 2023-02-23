"""
Opal datasource plugin import.
"""

import json
import obiba_opal.core as core
import obiba_opal.io as io

class ImportPluginCommand:
    """
    Import from a plugin.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add import command specific options
        """
        parser.add_argument('--name', '-n', required=True, help='Opal datasource plugin name')
        parser.add_argument('--config', '-c', required=False, help='A JSON file containing the import configuration. If not provided, the plugin will apply default values (or will fail).')

        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            importer = io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
                                                    incremental=args.incremental, limit=args.limit,
                                                    identifiers=args.identifiers,
                                                    policy=args.policy, merge=args.merge, verbose=args.verbose)
            # print result
            extension_factory = cls.OpalExtensionFactory(name=args.name, config=args.config)

            response = importer.submit(extension_factory)

            # format response
            res = response.content
            if args.json:
                res = response.pretty_json()

            # output to stdout
            print(res)
        finally:
            client.close()

    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(self, name, config):
            self.name = name
            self.config = json.loads(open(config).read())

        def add(self, factory):
            """
            Add specific datasource factory extension
            """
            extension = {}

            if self.name:
                extension['name'] = self.name
            if self.config:
                extension['parameters'] = json.dumps(self.config)

            factory['Magma.PluginDatasourceFactoryDto.params'] = extension


class ImportCSVCommand:
    """
    Import CSV file data.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--path', '-pa', required=True, help='CSV file to import from the Opal filesystem.')
        parser.add_argument('--characterSet', '-c', required=False, help='Character set.')
        parser.add_argument('--separator', '-s', required=False, help='Field separator.')
        parser.add_argument('--quote', '-q', required=False, help='Quotation mark character.')
        parser.add_argument('--firstRow', '-f', type=int, required=False, help='From row.')
        parser.add_argument('--valueType', '-vt', required=False,
                            help='Default value type (text, integer, decimal, boolean etc.). When not specified, "text" is the default.')
        parser.add_argument('--type', '-ty', required=True, help='Entity type (e.g. Participant)')

        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            importer = io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
                                                    incremental=args.incremental, limit=args.limit,
                                                    identifiers=args.identifiers,
                                                    policy=args.policy, merge=args.merge, verbose=args.verbose)
            # print result
            extension_factory = cls.OpalExtensionFactory(characterSet=args.characterSet, separator=args.separator,
                                                        quote=args.quote,
                                                        firstRow=args.firstRow, path=args.path, valueType=args.valueType,
                                                        type=args.type,
                                                        tables=args.tables,
                                                        destination=args.destination)

            response = importer.submit(extension_factory)

            # format response
            res = response.content
            if args.json:
                res = response.pretty_json()

            # output to stdout
            print(res)
        finally:
            client.close()


    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(self, characterSet, separator, quote, firstRow, path, valueType, type, tables, destination):
            self.characterSet = characterSet
            self.separator = separator
            self.quote = quote
            self.firstRow = firstRow
            self.path = path
            self.valueType = valueType
            self.type = type
            self.tables = tables
            self.destination = destination

        def add(self, factory):
            """
            Add specific datasource factory extension
            """
            csv_factory = {}

            if self.characterSet:
                csv_factory.characterSet = self.characterSet

            if self.separator:
                csv_factory.separator = self.separator

            if self.quote:
                csv_factory.quote = self.quote

            if self.firstRow:
                csv_factory.firstRow = self.firstRow

            if self.valueType:
                csv_factory.defaultValueType = self.valueType

            table = {
                'data': self.path,
                'entityType': self.type
            }

            if self.tables:
                table['name'] = self.tables[0]
            else:
                # Take filename as the table name
                name = self.path.split("/")

                index = name[-1].find('.csv')
                if index > 0:
                    table['name'] = name[-1][:index]
                else:
                    table['name'] = name[-1]
            table['refTable'] = self.destination + "." + table['name']

            csv_factory['tables'] = [table]

            factory['Magma.CsvDatasourceFactoryDto.params'] = csv_factory

class ImportLimeSurveyCommand:
    """
    Import from LimeSurvey.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--url', '-ur', required=False, help='LimeSurvey SQL database JDBC url (if not provided, plugin defaults will be used).')
        parser.add_argument('--uname', '-un', required=False, help='LimeSurvey SQL database user name (if not provided, plugin defaults will be used).')
        parser.add_argument('--pword', '-pwd', required=False, help='LimeSurvey SQL database user password (if not provided, plugin defaults will be used).')
        parser.add_argument('--prefix', '-pr', required=False, help='Table prefix (if not provided, plugin defaults will be used).')
        parser.add_argument('--properties', '-pp', required=False, help='SQL properties (if not provided, plugin defaults will be used).')

        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            importer = io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
                                                    incremental=args.incremental, limit=args.limit,
                                                    identifiers=args.identifiers,
                                                    policy=args.policy, merge=args.merge, verbose=args.verbose)
            # print result
            extension_factory = cls.OpalExtensionFactory(url=args.url, uname=args.uname, pword=args.pword, prefix=args.prefix, properties=args.properties)

            response = importer.submit(extension_factory)

            # format response
            res = response.content
            if args.json:
                res = response.pretty_json()

            # output to stdout
            print(res)
        finally:
            client.close()

    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(self, url, uname, pword, prefix, properties):
            self.url = url
            self.uname = uname
            self.pword = pword
            self.prefix = prefix
            self.properties = properties

        def add(self, factory):
            """
            Add specific datasource factory extension
            """
            extension = {}

            extension['name'] = 'opal-datasource-limesurvey'

            config = {}
            if self.url:
                config['url'] = self.url
            if self.uname:
                config['username'] = self.uname
            if self.pword:
                config['password'] = self.pword
            if self.prefix:
                config['prefix'] = self.prefix
            if self.properties:
                config['properties'] = self.properties
            extension['parameters'] = json.dumps(config)

            factory['Magma.PluginDatasourceFactoryDto.params'] = extension

class ImportOpalCommand:
    """
    Import from an Opal server.
    """

    @classmethod
    def add_arguments(cls, parser):
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

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            importer = io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
                                                    incremental=args.incremental, limit=args.limit,
                                                    identifiers=args.identifiers,
                                                    policy=args.policy, merge=args.merge, verbose=args.verbose)
            # remote opal client factory
            if (args.rtoken and args.ruser) or (not args.rtoken and not args.ruser):
                print("Either specify token OR user credentials (user name and password)")
            else:
                extension_factory = cls.OpalExtensionFactory(ropal=args.ropal, rdatasource=args.rdatasource, ruser=args.ruser,
                                                            rpassword=args.rpassword, rtoken=args.rtoken)
                response = importer.submit(extension_factory)

                # format response
                res = response.content
                if args.json:
                    res = response.pretty_json()

                # output to stdout
                print(res)
        finally:
            client.close()

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


class ImportRDSCommand:
    """
    Import from a RDS file.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--path', '-pa', required=True, help='RDS file to import from the Opal filesystem.')
        parser.add_argument('--type', '-ty', required=False, help='Entity type (e.g. Participant)')
        parser.add_argument('--idVariable', '-iv', required=False,
                            help='R tibble column that provides the entity ID. If not specified, first column values are considered to be the entity identifiers.')

        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        # Check input filename extension
        if not (args.path.endswith('.rds')):
            raise Exception('Input must be a RDS file (.rds).')

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            importer = io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
                                                    incremental=args.incremental, limit=args.limit,
                                                    identifiers=args.identifiers,
                                                    policy=args.policy, merge=args.merge, verbose=args.verbose)
            # print result
            extension_factory = cls.OpalExtensionFactory(path=args.path, entityType=args.type, idVariable=args.idVariable)

            response = importer.submit(extension_factory)

            # format response
            res = response.content
            if args.json:
                res = response.pretty_json()

            # output to stdout
            print(res)
        finally:
            client.close()

    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(self, path, entityType, idVariable):
            self.path = path
            self.entityType = entityType
            self.idVariable = idVariable

        def add(self, factory):
            """
            Add specific datasource factory extension
            """
            extension = {
                'file': self.path,
                'symbol': self.path[self.path.rfind("/") + 1:self.path.rfind(".")]
            }

            if self.entityType:
                extension['entityType'] = self.entityType
            if self.idVariable:
                extension['idColumn'] = self.idVariable

            factory['Magma.RHavenDatasourceFactoryDto.params'] = extension


class ImportRSASCommand:
    """
    Import from a SAS file using R.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--path', '-pa', required=True,
                            help='SAS/SAS Transport file to import from the Opal filesystem.')
        parser.add_argument('--locale', '-l', required=False, help='SAS file locale (e.g. fr, en...).')
        parser.add_argument('--type', '-ty', required=False, help='Entity type (e.g. Participant)')
        parser.add_argument('--idVariable', '-iv', required=False,
                            help='SAS variable that provides the entity ID. If not specified, first variable values are considered to be the entity identifiers.')

        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        # Check input filename extension
        if not (args.path.endswith('.sas7bdat')) and not (args.path.endswith('.xpt')):
            raise Exception('Input must be a SAS file (.sas7bdat or .xpt).')

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            importer = io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
                                                    incremental=args.incremental, limit=args.limit,
                                                    identifiers=args.identifiers,
                                                    policy=args.policy, merge=args.merge, verbose=args.verbose)
            # print result
            extension_factory = cls.OpalExtensionFactory(path=args.path,
                                                        locale=args.locale, entityType=args.type, idVariable=args.idVariable)

            response = importer.submit(extension_factory)

            # format response
            res = response.content
            if args.json:
                res = response.pretty_json()

            # output to stdout
            print(res)
        finally:
            client.close()

    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(self, path, locale, entityType, idVariable):
            self.path = path
            self.locale = locale
            self.entityType = entityType
            self.idVariable = idVariable

        def add(self, factory):
            """
            Add specific datasource factory extension
            """
            extension = {
                'file': self.path,
                'symbol': self.path[self.path.rfind("/") + 1:self.path.rfind(".")]
            }

            if self.locale:
                extension['locale'] = self.locale
            if self.entityType:
                extension['entityType'] = self.entityType
            if self.idVariable:
                extension['idColumn'] = self.idVariable

            factory['Magma.RHavenDatasourceFactoryDto.params'] = extension


class ImportRSPSSCommand:
    """
    Import from a SPSS file using R.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--path', '-pa', required=True,
                            help='SPSS file, optionally compressed, to import from the Opal filesystem.')
        parser.add_argument('--locale', '-l', required=False, help='SPSS file locale (e.g. fr, en...).')
        parser.add_argument('--type', '-ty', required=False, help='Entity type (e.g. Participant)')
        parser.add_argument('--idVariable', '-iv', required=False,
                            help='SPSS variable that provides the entity ID. If not specified, first variable values are considered to be the entity identifiers.')

        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        # Check input filename extension
        if not (args.path.endswith('.sav')) and not (args.path.endswith('.zsav')):
            raise Exception('Input must be a SPSS file (.sav or .zsav).')

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            importer = io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
                                                    incremental=args.incremental, limit=args.limit,
                                                    identifiers=args.identifiers,
                                                    policy=args.policy, merge=args.merge, verbose=args.verbose)
            # print result
            extension_factory = cls.OpalExtensionFactory(path=args.path,
                                                        locale=args.locale, entityType=args.type, idVariable=args.idVariable)

            response = importer.submit(extension_factory)

            # format response
            res = response.content
            if args.json:
                res = response.pretty_json()

            # output to stdout
            print(res)
        finally:
            client.close()

    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(self, path, locale, entityType, idVariable):
            self.path = path
            self.locale = locale
            self.entityType = entityType
            self.idVariable = idVariable

        def add(self, factory):
            """
            Add specific datasource factory extension
            """
            extension = {
                'file': self.path,
                'symbol': self.path[self.path.rfind("/") + 1:self.path.rfind(".")]
            }

            if self.locale:
                extension['locale'] = self.locale
            if self.entityType:
                extension['entityType'] = self.entityType
            if self.idVariable:
                extension['idColumn'] = self.idVariable

            factory['Magma.RHavenDatasourceFactoryDto.params'] = extension


class ImportRSTATACommand:
    """
    Import from a STATA file using R.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--path', '-pa', required=True, help='Stata file to import from the Opal filesystem.')
        parser.add_argument('--locale', '-l', required=False, help='Stata file locale (e.g. fr, en...).')
        parser.add_argument('--type', '-ty', required=False, help='Entity type (e.g. Participant)')
        parser.add_argument('--idVariable', '-iv', required=False,
                            help='Stata variable that provides the entity ID. If not specified, first variable values are considered to be the entity identifiers.')

        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        # Check input filename extension
        if not (args.path.endswith('.dta')):
            raise Exception('Input must be a Stata file (.dta).')

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            importer = io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
                                                    incremental=args.incremental, limit=args.limit,
                                                    identifiers=args.identifiers,
                                                    policy=args.policy, merge=args.merge, verbose=args.verbose)
            # print result
            extension_factory = cls.OpalExtensionFactory(path=args.path,
                                                        locale=args.locale, entityType=args.type, idVariable=args.idVariable)

            response = importer.submit(extension_factory)

            # format response
            res = response.content
            if args.json:
                res = response.pretty_json()

            # output to stdout
            print(res)
        finally:
            client.close()

    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(self, path, locale, entityType, idVariable):
            self.path = path
            self.locale = locale
            self.entityType = entityType
            self.idVariable = idVariable

        def add(self, factory):
            """
            Add specific datasource factory extension
            """
            extension = {
                'file': self.path,
                'symbol': self.path[self.path.rfind("/") + 1:self.path.rfind(".")]
            }

            if self.locale:
                extension['locale'] = self.locale
            if self.entityType:
                extension['entityType'] = self.entityType
            if self.idVariable:
                extension['idColumn'] = self.idVariable

            factory['Magma.RHavenDatasourceFactoryDto.params'] = extension


class ImportSQLCommand:
    """
    Import from a SQL database.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--database', '-db', required=True, help='Name of the SQL database.')
        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            importer = io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
                                                    incremental=args.incremental, limit=args.limit,
                                                    identifiers=args.identifiers,
                                                    policy=args.policy, merge=args.merge, verbose=args.verbose)
            # print result
            extension_factory = cls.OpalExtensionFactory(database=args.database)

            response = importer.submit(extension_factory)

            # format response
            res = response.content
            if args.json:
                res = response.pretty_json()

            # output to stdout
            print(res)
        finally:
            client.close()

    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(self, database):
            self.database = database

        def add(self, factory):
            """
            Add specific datasource factory extension
            """
            factory['Magma.JdbcDatasourceFactoryDto.params'] = {'database': self.database}


class ImportXMLCommand:
    """
    Import from an Opal archive (zipped XML files).
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--path', '-pa', required=True, help='Zip of XML files to import from the Opal filesystem.')
        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            importer = io.OpalImporter.build(client=client, destination=args.destination, tables=args.tables,
                                                    incremental=args.incremental, limit=args.limit,
                                                    identifiers=args.identifiers,
                                                    policy=args.policy, merge=args.merge, verbose=args.verbose)
            # print result
            extension_factory = cls.OpalExtensionFactory(path=args.path)

            response = importer.submit(extension_factory)

            # format response
            res = response.content
            if args.json:
                res = response.pretty_json()

            # output to stdout
            print(res)
        finally:
            client.close()

    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(self, path):
            self.path = path

        def add(self, factory):
            """
            Add specific datasource factory extension
            """
            factory['Magma.FsDatasourceFactoryDto.params'] = {'file': self.path}

class ImportVCFCommand:
    """
    VCF/BCF files import.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        parser.add_argument('--project', '-pr', required=True,
                            help='Project name into which genotypes data will be imported')
        parser.add_argument('--vcf', '-vcf', nargs='+', required=True,
                            help='List of VCF/BCF (optionally compressed) file paths (in Opal file system)')

    @classmethod
    def do_command(cls, args):
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
                'files': args.vcf
            }
            # send request
            uri = core.UriBuilder(['project', args.project, 'commands', '_import_vcf']).build()
            request.resource(uri).post().content(json.dumps(options)).send()
        finally:
            client.close()


class ImportIDService:
    """
    Import identifiers in the identifiers database.
    """
    
    @classmethod
    def add_arguments(cls, parser):
        """
        Add import_ids command specific options
        """
        parser.add_argument('--type', '-t', required=True, help='Entity type (e.g. Participant).')

    @classmethod
    def do_command(cls, args):
        """
        Execute import command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            request = client.new_request()
            request.fail_on_error()

            if args.verbose:
                request.verbose()

            request.content_type_text_plain()
            print('Enter identifiers (one identifier per line, Ctrl-D to end input):')
            request.content(sys.stdin.read())

            # send request
            uri = core.UriBuilder(['identifiers', 'mappings', 'entities', '_import']).query('type', args.type).build()
            request.post().resource(uri).send()
        finally:
            client.close()


class ImportIDMapService:
    """
    Import identifiers mapping into the identifiers database.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add import_idsmap command specific options
        """
        parser.add_argument('--type', '-t', required=True, help='Entity type (e.g. Participant).')
        parser.add_argument('--map', '-m', required=True, help='Mapping name.')
        parser.add_argument('--separator', '-s', required=False, help='Field separator (default is ,).')

    @classmethod
    def do_command(cls, args):
        """
        Execute import command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            request = client.new_request()
            request.fail_on_error()

            if args.verbose:
                request.verbose()

            request.content_type_text_plain()
            print('Enter identifiers (one identifiers mapping per line, Ctrl-D to end input):')
            request.content(sys.stdin.read())

            # send request
            builder = core.UriBuilder(['identifiers', 'mapping', args.map, '_import']).query('type', args.type)
            if args.separator:
                builder.query('separator', args.separator)
            uri = builder.build()
            request.post().resource(uri).send()
        finally:
            client.close()