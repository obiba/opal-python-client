"""
Opal datasource plugin import.
"""

import obiba_opal.core as core
import obiba_opal.io as io
import sys
import json


class ImportPluginCommand:
    """
    Import from a plugin.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add import command specific options
        """
        parser.add_argument("--name", "-n", required=True, help="Opal datasource plugin name")
        parser.add_argument(
            "--config",
            "-c",
            required=False,
            help="A JSON file containing the import configuration. If not "
            "provided, the plugin will apply default values (or will fail).",
        )

        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command.
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            with open(args.config) as f:
                config = json.loads(f.read())
            res = cls(client, args.verbose).import_data(
                args.name,
                config,
                args.destination,
                args.tables,
                args.incremental,
                args.limit,
                args.identifiers,
                args.policy,
                args.merge,
            )
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def import_data(
        self,
        name: str,
        config: dict,
        destination: str,
        tables: list = None,
        incremental: bool = None,
        limit: int = None,
        identifiers: str = None,
        policy: str = None,
        merge: bool = None,
    ) -> dict:
        """
        Import tables using a plugin.

        :param name: The plugin name
        :param config: The plugin configuration
        :param destination: The destination project
        :param tables: The tables names to be imported (default is all)
        :param incremental: Incremental import (new and updated value sets)
        :param limit: Import limit (maximum number of value sets)
        :param identifiers: The name of the ID mapping
        :param policy: The ID mapping policy: "required" (each identifiers must
                      be mapped prior importation, default), "ignore" (ignore
                      unknown identifiers), "generate" (generate a system
                      identifier for each unknown identifier)
        :param merge: Merge imported data dictionary with the destination one
                     (default is false, i.e. data dictionary is overridden)
        """
        importer = io.OpalImporter.build(
            client=self.client,
            destination=destination,
            tables=tables,
            incremental=incremental,
            limit=limit,
            identifiers=identifiers,
            policy=policy,
            merge=merge,
            verbose=self.verbose,
        )
        extension_factory = self.OpalExtensionFactory(name, config)

        response = importer.submit(extension_factory)
        return response.from_json()

    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(self, name, config):
            self.name = name
            self.config = config

        def add(self, factory):
            """
            Add specific datasource factory extension
            """
            extension = {}

            if self.name:
                extension["name"] = self.name
            if self.config:
                extension["parameters"] = json.dumps(self.config)

            factory["Magma.PluginDatasourceFactoryDto.params"] = extension


class ImportCSVCommand:
    """
    Import CSV file data.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument(
            "--path",
            "-pa",
            required=True,
            help="CSV file to import from the Opal filesystem.",
        )
        parser.add_argument("--characterSet", "-c", required=False, help="Character set.")
        parser.add_argument("--separator", "-s", required=False, help="Field separator.")
        parser.add_argument("--quote", "-q", required=False, help="Quotation mark character.")
        parser.add_argument("--firstRow", "-f", type=int, required=False, help="From row.")
        parser.add_argument(
            "--valueType",
            "-vt",
            required=False,
            help="Default value type (text, integer, decimal, boolean etc.). "
            'When not specified, "text" is the default.',
        )
        parser.add_argument("--type", "-ty", required=True, help="Entity type (e.g. Participant)")

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
            res = cls(client, args.verbose).import_data(
                args.path,
                args.destination,
                args.characterSet,
                args.separator,
                args.quote,
                args.firstRow,
                args.valueType,
                args.type,
                args.tables,
                args.incremental,
                args.limit,
                args.identifiers,
                args.policy,
                args.merge,
            )
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def import_data(
        self,
        path: str,
        destination: str,
        characterSet: str = None,
        separator: str = None,
        quote: str = None,
        firstRow: int = None,
        valueType: str = None,
        type: str = None,
        tables: list = None,
        incremental: bool = None,
        limit: int = None,
        identifiers: str = None,
        policy: str = None,
        merge: bool = None,
    ) -> dict:
        """
        Import tables from a CSV file.

        :param characterSet: The cheracter set
        :param separator: The separator char
        :param quote: The quote char
        :param firstRow: From row
        :param path: File to import in Opal file system
        :param valueType: Default value type (text, integer, decimal, boolean
                         etc.). When not specified, "text" is the default
        :param policy: The ID mapping policy: "required" (each identifiers must
                      be mapped prior importation, default), "ignore" (ignore
                      unknown identifiers), "generate" (generate a system
                      identifier for each unknown identifier)
        :param merge: Merge imported data dictionary with the destination one
                     (default is false, i.e. data dictionary is overridden)
        """
        importer = io.OpalImporter.build(
            self.client,
            destination=destination,
            tables=tables,
            incremental=incremental,
            limit=limit,
            identifiers=identifiers,
            policy=policy,
            merge=merge,
            verbose=self.verbose,
        )
        extension_factory = self.OpalExtensionFactory(
            characterSet=characterSet,
            separator=separator,
            quote=quote,
            firstRow=firstRow,
            path=path,
            valueType=valueType,
            type=type,
            tables=tables,
            destination=destination,
        )
        response = importer.submit(extension_factory)
        return response.from_json()

    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(
            self,
            characterSet,
            separator,
            quote,
            firstRow,
            path,
            valueType,
            type,
            tables,
            destination,
        ):
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
                csv_factory["characterSet"] = self.characterSet

            if self.separator:
                csv_factory["separator"] = self.separator

            if self.quote:
                csv_factory["quote"] = self.quote

            if self.firstRow:
                csv_factory["firstRow"] = self.firstRow

            if self.valueType:
                csv_factory["defaultValueType"] = self.valueType

            table = {"data": self.path, "entityType": self.type}

            if self.tables:
                table["name"] = self.tables[0]
            else:
                # Take filename as the table name
                name = self.path.split("/")

                index = name[-1].find(".csv")
                if index > 0:
                    table["name"] = name[-1][:index]
                else:
                    table["name"] = name[-1]
            table["refTable"] = self.destination + "." + table["name"]

            csv_factory["tables"] = [table]

            factory["Magma.CsvDatasourceFactoryDto.params"] = csv_factory


class ImportLimeSurveyCommand:
    """
    Import from LimeSurvey.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument(
            "--url",
            "-ur",
            required=False,
            help="LimeSurvey SQL database JDBC url (if not provided, plugin defaults will be used).",
        )
        parser.add_argument(
            "--uname",
            "-un",
            required=False,
            help="LimeSurvey SQL database user name (if not provided, plugin defaults will be used).",
        )
        parser.add_argument(
            "--pword",
            "-pwd",
            required=False,
            help="LimeSurvey SQL database user password (if not provided, plugin defaults will be used).",
        )
        parser.add_argument(
            "--prefix",
            "-pr",
            required=False,
            help="Table prefix (if not provided, plugin defaults will be used).",
        )
        parser.add_argument(
            "--properties",
            "-pp",
            required=False,
            help="SQL properties (if not provided, plugin defaults will be used).",
        )

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
            res = cls(client, args.verbose).import_data(
                args.url,
                args.uname,
                args.pword,
                args.prefix,
                args.properties,
                args.destination,
                args.tables,
                args.incremental,
                args.limit,
                args.identifiers,
                args.policy,
                args.merge,
            )
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def import_data(
        self,
        url: str,
        uname: str,
        pword: str,
        prefix: str,
        properties: str,
        destination: str,
        tables: list = None,
        incremental: bool = None,
        limit: int = None,
        identifiers: str = None,
        policy: str = None,
        merge: bool = None,
    ) -> dict:
        """
        Import tables from a LimeSurvey database.

        :param url: LimeSurvey SQL database JDBC url (if not provided, plugin
            defaults will be used)
        :param uname: LimeSurvey SQL database user name (if not provided, plugin
            defaults will be used)
        :param pword: LimeSurvey SQL database user password (if not provided,
            plugin defaults will be used)
        :param prefix: Table prefix (if not provided, plugin defaults will be used)
        :param properties: SQL properties (if not provided, plugin defaults will
            be used)
        :param destination: The destination project
        :param tables: The tables names to be imported (default is all)
        :param incremental: Incremental import (new and updated value sets)
        :param limit: Import limit (maximum number of value sets)
        :param identifiers: The name of the ID mapping
        :param policy: The ID mapping policy: "required" (each identifiers must
            be mapped prior importation, default), "ignore" (ignore unknown
            identifiers), "generate" (generate a system identifier for each
            unknown identifier)
        :param merge: Merge imported data dictionary with the destination one
            (default is false, i.e. data dictionary is overridden)
        """
        importer = io.OpalImporter.build(
            self.client,
            destination=destination,
            tables=tables,
            incremental=incremental,
            limit=limit,
            identifiers=identifiers,
            policy=policy,
            merge=merge,
            verbose=self.verbose,
        )
        extension_factory = self.OpalExtensionFactory(url, uname, pword, prefix, properties)
        response = importer.submit(extension_factory)
        return response.from_json()

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

            extension["name"] = "opal-datasource-limesurvey"

            config = {}
            if self.url:
                config["url"] = self.url
            if self.uname:
                config["username"] = self.uname
            if self.pword:
                config["password"] = self.pword
            if self.prefix:
                config["prefix"] = self.prefix
            if self.properties:
                config["properties"] = self.properties
            extension["parameters"] = json.dumps(config)

            factory["Magma.PluginDatasourceFactoryDto.params"] = extension


class ImportOpalCommand:
    """
    Import from an Opal server.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument("--ropal", "-ro", required=True, help="Remote Opal server base url")
        parser.add_argument(
            "--ruser",
            "-ru",
            required=False,
            help="Remote user name (exclusive from using token)",
        )
        parser.add_argument(
            "--rpassword",
            "-rp",
            required=False,
            help="Remote user password (exclusive from using token)",
        )
        parser.add_argument(
            "--rtoken",
            "-rt",
            required=False,
            help="Remote personal access token (exclusive from user credentials)",
        )
        parser.add_argument("--rdatasource", "-rd", required=True, help="Remote datasource name")
        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        if (args.rtoken and args.ruser) or (not args.rtoken and not args.ruser):
            raise ValueError("Either specify token OR user credentials (user name and password)")
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = cls(client, args.vebose).import_data(
                args.ropal,
                args.rdatasource,
                args.ruser,
                args.rpassword,
                args.rtoken,
                args.destination,
                args.tables,
                args.incremental,
                args.limit,
                args.identifiers,
                args.policy,
                args.merge,
            )
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def import_data(
        self,
        ropal: str,
        rdatasource: str,
        ruser: str,
        rpassword: str,
        rtoken: str,
        destination: str,
        tables: list = None,
        incremental: bool = None,
        limit: int = None,
        identifiers: str = None,
        policy: str = None,
        merge: bool = None,
    ):
        """
        Import tables from a Opal server.

        :param ropal: Remote Opal server base url
        :param rdatasource: Remote project's datasource name
        :param ruser: Remote user name (exclusive from using token)
        :param rpassword: Remote user password (exclusive from using token)
        :param rtoken: Remote personal access token (exclusive from user
            credentials)
        :param destination: The destination project
        :param tables: The tables names to be imported (default is all)
        :param incremental: Incremental import (new and updated value sets)
        :param limit: Import limit (maximum number of value sets)
        :param identifiers: The name of the ID mapping
        :param policy: The ID mapping policy: "required" (each identifiers must
            be mapped prior importation, default), "ignore" (ignore unknown
            identifiers), "generate" (generate a system identifier for each
            unknown identifier)
        :param merge: Merge imported data dictionary with the destination one
            (default is false, i.e. data dictionary is overridden)
        """
        importer = io.OpalImporter.build(
            self.client,
            destination=destination,
            tables=tables,
            incremental=incremental,
            limit=limit,
            identifiers=identifiers,
            policy=policy,
            merge=merge,
            verbose=self.verbose,
        )
        # remote opal client factory
        extension_factory = self.OpalExtensionFactory(ropal, rdatasource, ruser, rpassword, rtoken)
        response = importer.submit(extension_factory)
        return response.from_json()

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
            rest_factory = {"remoteDatasource": self.rdatasource, "url": self.ropal}
            if self.rtoken:
                rest_factory["token"] = self.rtoken
            else:
                rest_factory["username"] = self.ruser
                rest_factory["password"] = self.rpassword

            factory["Magma.RestDatasourceFactoryDto.params"] = rest_factory


class ImportRDSCommand:
    """
    Import from a RDS file.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument(
            "--path",
            "-pa",
            required=True,
            help="RDS file to import from the Opal filesystem.",
        )
        parser.add_argument("--type", "-ty", required=False, help="Entity type (e.g. Participant)")
        parser.add_argument(
            "--idVariable",
            "-iv",
            required=False,
            help="R tibble column that provides the entity ID. If not specified, first column values "
            "are considered to be the entity identifiers.",
        )

        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        # Check input filename extension
        if not (args.path.endswith(".rds")):
            raise Exception("Input must be a RDS file (.rds).")

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = cls(client, args.verbose).import_data(
                args.path,
                args.destination,
                args.type,
                args.idVariable,
                args.tables,
                args.incremental,
                args.limit,
                args.identifiers,
                args.policy,
                args.merge,
            )
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def import_data(
        self,
        path: str,
        destination: str,
        entityType: str = None,
        idVariable: str = None,
        tables: list = None,
        incremental: bool = None,
        limit: int = None,
        identifiers: str = None,
        policy: str = None,
        merge: bool = None,
    ):
        """
        Import tables from a RDS file.

        :param path: File to import in Opal file system
        :param entityType: Entity type (e.g. Participant)
        :param idVariable: R tibble column that provides the entity ID. If not specified, first
            column values are considered to be the entity identifiers
        :param destination: The destination project
        :param tables: The tables names to be imported (default is all)
        :param incremental: Incremental import (new and updated value sets)
        :param limit: Import limit (maximum number of value sets)
        :param identifiers: The name of the ID mapping
        :param policy: The ID mapping policy: "required" (each identifiers must be mapped prior
            importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a
            system identifier for each unknown identifier)
        :param merge: Merge imported data dictionary with the destination one (default is false,
            i.e. data dictionary is overridden)
        """
        importer = io.OpalImporter.build(
            self.client,
            destination=destination,
            tables=tables,
            incremental=incremental,
            limit=limit,
            identifiers=identifiers,
            policy=policy,
            merge=merge,
            verbose=self.verbose,
        )
        extension_factory = self.OpalExtensionFactory(path, entityType, idVariable)
        response = importer.submit(extension_factory)
        return response.from_json()

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
                "file": self.path,
                "symbol": self.path[self.path.rfind("/") + 1 : self.path.rfind(".")],
            }

            if self.entityType:
                extension["entityType"] = self.entityType
            if self.idVariable:
                extension["idColumn"] = self.idVariable

            factory["Magma.RHavenDatasourceFactoryDto.params"] = extension


class ImportRSASCommand:
    """
    Import from a SAS file using R.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument(
            "--path",
            "-pa",
            required=True,
            help="SAS/SAS Transport file to import from the Opal filesystem.",
        )
        parser.add_argument("--locale", "-l", required=False, help="SAS file locale (e.g. fr, en...).")
        parser.add_argument("--type", "-ty", required=False, help="Entity type (e.g. Participant)")
        parser.add_argument(
            "--idVariable",
            "-iv",
            required=False,
            help="SAS variable that provides the entity ID. If not specified, first variable values are considered to "
            "be the entity identifiers.",
        )

        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        # Check input filename extension
        if not (args.path.endswith(".sas7bdat")) and not (args.path.endswith(".xpt")):
            raise Exception("Input must be a SAS file (.sas7bdat or .xpt).")

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = cls(client, args.verbose).import_data(
                args.path,
                args.destination,
                args.locale,
                args.type,
                args.idVariable,
                args.tables,
                args.incremental,
                args.limit,
                args.identifiers,
                args.policy,
                args.merge,
            )
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def import_data(
        self,
        path: str,
        destination: str,
        locale: str = None,
        entityType: str = None,
        idVariable: str = None,
        tables: list = None,
        incremental: bool = None,
        limit: int = None,
        identifiers: str = None,
        policy: str = None,
        merge: bool = None,
    ):
        """
        Import tables from a SAS file.

        :param path: File to import in Opal file system
        :param locale: SAS file locale (e.g. fr, en...)
        :param entityType: Entity type (e.g. Participant)
        :param idVariable: R tibble column that provides the entity ID. If not specified, first column
            values are considered to be the entity identifiers
        :param destination: The destination project
        :param tables: The tables names to be imported (default is all)
        :param incremental: Incremental import (new and updated value sets)
        :param limit: Import limit (maximum number of value sets)
        :param identifiers: The name of the ID mapping
        :param policy: The ID mapping policy: "required" (each identifiers must be mapped prior
            importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a system
            identifier for each unknown identifier)
        :param merge: Merge imported data dictionary with the destination one (default is false, i.e. data
            dictionary is overridden)
        """
        importer = io.OpalImporter.build(
            self.client,
            destination,
            tables,
            incremental,
            limit,
            identifiers,
            policy,
            merge,
            self.verbose,
        )
        extension_factory = self.OpalExtensionFactory(path, locale, entityType, idVariable)

        response = importer.submit(extension_factory)
        return response.from_json()

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
                "file": self.path,
                "symbol": self.path[self.path.rfind("/") + 1 : self.path.rfind(".")],
            }

            if self.locale:
                extension["locale"] = self.locale
            if self.entityType:
                extension["entityType"] = self.entityType
            if self.idVariable:
                extension["idColumn"] = self.idVariable

            factory["Magma.RHavenDatasourceFactoryDto.params"] = extension


class ImportRSPSSCommand:
    """
    Import from a SPSS file using R.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument(
            "--path",
            "-pa",
            required=True,
            help="SPSS file, optionally compressed, to import from the Opal filesystem.",
        )
        parser.add_argument("--locale", "-l", required=False, help="SPSS file locale (e.g. fr, en...).")
        parser.add_argument("--type", "-ty", required=False, help="Entity type (e.g. Participant)")
        parser.add_argument(
            "--idVariable",
            "-iv",
            required=False,
            help="SPSS variable that provides the entity ID. If not specified, first variable values "
            "are considered to be the entity identifiers.",
        )

        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        # Check input filename extension
        if not (args.path.endswith(".sav")) and not (args.path.endswith(".zsav")):
            raise Exception("Input must be a SPSS file (.sav or .zsav).")

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = cls(client, args.verbose).import_data(
                args.path,
                args.destination,
                args.locale,
                args.type,
                args.idVariable,
                args.tables,
                args.incremental,
                args.limit,
                args.identifiers,
                args.policy,
                args.merge,
            )
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def import_data(
        self,
        path: str,
        destination: str,
        locale: str = None,
        entityType: str = None,
        idVariable: str = None,
        tables: list = None,
        incremental: bool = None,
        limit: int = None,
        identifiers: str = None,
        policy: str = None,
        merge: bool = None,
    ):
        """
        Import tables from a SPSS file.

        :param path: File to import in Opal file system
        :param locale: SPSS file locale (e.g. fr, en...)
        :param entityType: Entity type (e.g. Participant)
        :param idVariable: R tibble column that provides the entity ID. If not specified, first column
            values are considered to be the entity identifiers
        :param destination: The destination project
        :param tables: The tables names to be imported (default is all)
        :param incremental: Incremental import (new and updated value sets)
        :param limit: Import limit (maximum number of value sets)
        :param identifiers: The name of the ID mapping
        :param policy: The ID mapping policy: "required" (each identifiers must be mapped prior
            importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a
            system identifier for each unknown identifier)
        :param merge: Merge imported data dictionary with the destination one (default is false, i.e. data
            dictionary is overridden)
        """
        importer = io.OpalImporter.build(
            self.client,
            destination,
            tables,
            incremental,
            limit,
            identifiers,
            policy,
            merge,
            self.verbose,
        )
        extension_factory = self.OpalExtensionFactory(path, locale, entityType, idVariable)

        response = importer.submit(extension_factory)
        return response.from_json()

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
                "file": self.path,
                "symbol": self.path[self.path.rfind("/") + 1 : self.path.rfind(".")],
            }

            if self.locale:
                extension["locale"] = self.locale
            if self.entityType:
                extension["entityType"] = self.entityType
            if self.idVariable:
                extension["idColumn"] = self.idVariable

            factory["Magma.RHavenDatasourceFactoryDto.params"] = extension


class ImportRSTATACommand:
    """
    Import from a STATA file using R.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument(
            "--path",
            "-pa",
            required=True,
            help="Stata file to import from the Opal filesystem.",
        )
        parser.add_argument("--locale", "-l", required=False, help="Stata file locale (e.g. fr, en...).")
        parser.add_argument("--type", "-ty", required=False, help="Entity type (e.g. Participant)")
        parser.add_argument(
            "--idVariable",
            "-iv",
            required=False,
            help="Stata variable that provides the entity ID. If not specified, first variable "
            "values are considered to be the entity identifiers.",
        )

        # non specific import arguments
        io.add_import_arguments(parser)

    @classmethod
    def do_command(cls, args):
        """
        Execute import data command
        """
        # Build and send request
        # Check input filename extension
        if not (args.path.endswith(".dta")):
            raise Exception("Input must be a Stata file (.dta).")

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = cls(client, args.verbose).import_data(
                args.path,
                args.destination,
                args.locale,
                args.type,
                args.idVariable,
                args.tables,
                args.incremental,
                args.limit,
                args.identifiers,
                args.policy,
                args.merge,
            )
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def import_data(
        self,
        path: str,
        destination: str,
        locale: str = None,
        entityType: str = None,
        idVariable: str = None,
        tables: list = None,
        incremental: bool = None,
        limit: int = None,
        identifiers: str = None,
        policy: str = None,
        merge: bool = None,
    ):
        """
        Import tables from a STATA file.

        :param path: File to import in Opal file system
        :param locale: STATA file locale (e.g. fr, en...)
        :param entityType: Entity type (e.g. Participant)
        :param idVariable: R tibble column that provides the entity ID. If not specified, first
            column values are considered to be the entity identifiers
        :param destination: The destination project
        :param tables: The tables names to be imported (default is all)
        :param incremental: Incremental import (new and updated value sets)
        :param limit: Import limit (maximum number of value sets)
        :param identifiers: The name of the ID mapping
        :param policy: The ID mapping policy: "required" (each identifiers must be mapped prior
            importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a
            system identifier for each unknown identifier)
        :param merge: Merge imported data dictionary with the destination one (default is false, i.e.
            data dictionary is overridden)
        """
        importer = io.OpalImporter.build(
            self.client,
            destination,
            tables,
            incremental,
            limit,
            identifiers,
            policy,
            merge,
            self.verbose,
        )
        extension_factory = self.OpalExtensionFactory(path, locale, entityType, idVariable)

        response = importer.submit(extension_factory)
        return response.from_json()

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
                "file": self.path,
                "symbol": self.path[self.path.rfind("/") + 1 : self.path.rfind(".")],
            }

            if self.locale:
                extension["locale"] = self.locale
            if self.entityType:
                extension["entityType"] = self.entityType
            if self.idVariable:
                extension["idColumn"] = self.idVariable

            factory["Magma.RHavenDatasourceFactoryDto.params"] = extension


class ImportSQLCommand:
    """
    Import from a SQL database.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument("--database", "-db", required=True, help="Name of the SQL database.")
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
            res = cls(client, args.verbose).import_data(
                args.database,
                args.destination,
                args.tables,
                args.incremental,
                args.limit,
                args.identifiers,
                args.policy,
                args.merge,
            )
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def import_data(
        self,
        database: str,
        destination: str,
        tables: list = None,
        incremental: bool = None,
        limit: int = None,
        identifiers: str = None,
        policy: str = None,
        merge: bool = None,
    ):
        """
        Import tables from a SQL database.

        :param database: The database name as declared in Opal. See ProjectService.get_databases()
            for a list of databases with 'import' usage.
        :param destination: The destination project
        :param tables: The tables names to be imported (default is all)
        :param incremental: Incremental import (new and updated value sets)
        :param limit: Import limit (maximum number of value sets)
        :param identifiers: The name of the ID mapping
        :param policy: The ID mapping policy: "required" (each identifiers must be mapped prior
            importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a
            system identifier for each unknown identifier)
        :param merge: Merge imported data dictionary with the destination one (default is false, i.e.
            data dictionary is overridden)
        """
        importer = io.OpalImporter.build(
            self.client,
            destination,
            tables,
            incremental,
            limit,
            identifiers,
            policy,
            merge,
            self.verbose,
        )
        extension_factory = self.OpalExtensionFactory(database)

        response = importer.submit(extension_factory)
        return response.from_json()

    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(self, database):
            self.database = database

        def add(self, factory):
            """
            Add specific datasource factory extension
            """
            factory["Magma.JdbcDatasourceFactoryDto.params"] = {"database": self.database}


class ImportXMLCommand:
    """
    Import from an Opal archive (zipped XML files).
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument(
            "--path",
            "-pa",
            required=True,
            help="Zip of XML files to import from the Opal filesystem.",
        )
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
            res = cls(client, args.verbose).import_data(
                args.path,
                args.destination,
                args.tables,
                args.incremental,
                args.limit,
                args.identifiers,
                args.policy,
                args.merge,
            )
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def import_data(
        self,
        path: str,
        destination: str,
        tables: list = None,
        incremental: bool = None,
        limit: int = None,
        identifiers: str = None,
        policy: str = None,
        merge: bool = None,
    ):
        """
        Import tables from a Opal archive file.

        :param path: File to import in Opal file system
        :param destination: The destination project
        :param tables: The tables names to be imported (default is all)
        :param incremental: Incremental import (new and updated value sets)
        :param limit: Import limit (maximum number of value sets)
        :param identifiers: The name of the ID mapping
        :param policy: The ID mapping policy: "required" (each identifiers must be mapped prior
            importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a
            system identifier for each unknown identifier)
        :param merge: Merge imported data dictionary with the destination one (default is false, i.e.
            data dictionary is overridden)
        """
        importer = io.OpalImporter.build(
            self.client,
            destination,
            tables,
            incremental,
            limit,
            identifiers,
            policy,
            merge,
            self.verbose,
        )
        extension_factory = self.OpalExtensionFactory(path)

        response = importer.submit(extension_factory)
        return response.from_json()

    class OpalExtensionFactory(io.OpalImporter.ExtensionFactoryInterface):
        def __init__(self, path):
            self.path = path

        def add(self, factory):
            """
            Add specific datasource factory extension
            """
            factory["Magma.FsDatasourceFactoryDto.params"] = {"file": self.path}


class ImportVCFCommand:
    """
    VCF/BCF files import.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        parser.add_argument(
            "--project",
            "-pr",
            required=True,
            help="Project name into which genotypes data will be imported",
        )
        parser.add_argument(
            "--vcf",
            "-vcf",
            nargs="+",
            required=True,
            help="List of VCF/BCF (optionally compressed) file paths (in Opal file system)",
        )

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

            options = {"project": args.project, "files": args.vcf}
            # send request
            uri = core.UriBuilder([
                "project",
                args.project,
                "commands",
                "_import_vcf",
            ]).build()
            request.resource(uri).post().content(json.dumps(options)).send()
        finally:
            client.close()


class ImportIDService:
    """
    Import identifiers in the identifiers database.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add import_ids command specific options
        """
        parser.add_argument("--type", "-t", required=True, help="Entity type (e.g. Participant).")

    @classmethod
    def do_command(cls, args):
        """
        Execute import command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            print("Enter identifiers (one identifier per line, Ctrl-D to end input):")
            ids = sys.stdin.read()
            cls(client, args.verbose).import_ids(ids.split("\n"), args.type)
        finally:
            client.close()

    def import_ids(self, ids: list, type: str):
        """
        Import a list of identifiers in the IDs database.

        :param ids: The list of identifiers
        :param type: Entity type (e.g. Participant)
        """
        request = self.client.new_request()
        request.fail_on_error()
        if self.verbose:
            request.verbose()
        request.content_type_text_plain()
        request.content("\n".join(ids))

        # send request
        uri = core.UriBuilder(["identifiers", "mappings", "entities", "_import"]).query("type", type).build()
        request.post().resource(uri).send()


class ImportIDMapService:
    """
    Import identifiers mapping into the identifiers database.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add import_idsmap command specific options
        """
        parser.add_argument("--type", "-t", required=True, help="Entity type (e.g. Participant).")
        parser.add_argument("--map", "-m", required=True, help="Mapping name.")
        parser.add_argument("--separator", "-s", required=False, help="Field separator (default is ,).")

    @classmethod
    def do_command(cls, args):
        """
        Execute import command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            print("Enter identifiers (one identifiers mapping per line, Ctrl-D to end input):")
            ids = sys.stdin.read()
            cls(client, args.verbose).import_ids(ids.split("\n"), args.type, args.map, args.separator)
        finally:
            client.close()

    def import_ids(self, ids: list, type: str, map: str, separator: str = ","):
        """
        Import a list of identifiers mappings (each item is a string of separated IDs) in the IDs database.

        :param ids: The list of identifiers
        :param type: Entity type (e.g. Participant)
        :param map: The mapping name
        :param separator: Field separator
        """
        request = self.client.new_request()
        request.fail_on_error()

        if self.verbose:
            request.verbose()

        request.content_type_text_plain()
        request.content("\n".join(ids))

        # send request
        builder = core.UriBuilder(["identifiers", "mapping", map, "_import"]).query("type", type)
        if separator:
            builder.query("separator", separator)
        uri = builder.build()
        request.post().resource(uri).send()
