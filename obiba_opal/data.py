"""
Opal data and entities.
"""

import obiba_opal.core as core
import sys
import os


class DataService:
    """
    Extract identifiers, value sets and values from a table.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
        """
        Add data command specific options
        """
        parser.add_argument(
            "name",
            help="Fully qualified name of a table or a variable, for instance: "
            "opal-data.questionnaire or opal-data.questionnaire:Q1.",
        )
        parser.add_argument(
            "--id",
            "-i",
            required=False,
            help="Entity identifier. If missing the list of entities is returned.",
        )
        parser.add_argument(
            "--raw",
            "-r",
            action="store_true",
            help="Get raw value, output to stdout, useful for downloading "
            "a binary value",
        )
        parser.add_argument(
            "--pos",
            "-po",
            required=False,
            help="Position of the value to query in case of a repeatable "
            "variable (starting at 0).",
        )
        parser.add_argument(
            "--json",
            "-j",
            action="store_true",
            help="Pretty JSON formatting of the response",
        )

    @classmethod
    def do_command(self, args):
        """
        Execute data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            fd = None
            if args.raw:
                fd = sys.stdout.fileno()
            res = DataService(client, args.verbose)._get_data(
                args.name, args.id, args.pos, fd
            )
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_entities(self, project: str, table: str) -> list:
        """
        Get the entities of a table in a project.

        :param project: The project name
        :param table: The table name
        """
        return self._get_data(f"{project}.{table}")

    def get_valueset(self, project: str, table: str, id: str) -> dict:
        """
        Get the value set of an entity in a project's table.

        :param project: The project name
        :param table: The table name
        :param id: The entity identifier
        """
        return self._get_data(f"{project}.{table}", id)

    def get_value(
        self, project: str, table: str, variable: str, id: str, pos: str = None, fd=None
    ) -> dict:
        """
        Get the variable value of an entity in a project's table.

        :param project: The project name
        :param table: The table name
        :param id: The entity identifier
        :param pos: Position of the value to query in case of a repeatable
                   variable (starting at 0)
        :param fd: Get raw value into the provided file descriptor
                  (see os.fdopen()), useful for downloading a binary value
        """
        return self._get_data(f"{project}.{table}:{variable}", id, pos, fd)

    def _get_data(self, name: str, id: str = None, pos: str = None, fd=None) -> any:
        """
        Execute data command

        :param name: Fully qualified name of a table or a variable, for
                    instance: opal-data.questionnaire or
                    opal-data.questionnaire:Q1
        :param id: Entity identifier. If missing the list of entities is
                  returned
        :param pos: Position of the value to query in case of a repeatable
                   variable (starting at 0)
        :param fd: Get raw value into the provided file descriptor
                  (see os.fdopen()), useful for downloading a binary value
        """
        request = self.client.new_request()
        if self.verbose:
            request.verbose()

        resolver = core.MagmaNameResolver(name)
        raw = resolver.is_variable() and id and fd is not None
        ws = self._make_ws(resolver, id, pos, raw)
        request.fail_on_error().get().resource(ws)
        if raw:
            fp = os.fdopen(fd, "wb")
            response = request.accept("*/*").send(fp)
            fp.flush()
            return None
        else:
            response = request.send()
            return response.from_json()

    def _make_ws(
        self,
        resolver: core.MagmaNameResolver,
        id: str = None,
        pos: str = None,
        raw: bool = False,
    ):
        """
        Build the web service resource path
        """
        ws = resolver.get_table_ws()
        if id:
            ws = f"{ws}/valueSet/{id}"
            if resolver.is_variable():
                ws = f"{ws}/variable/{resolver.variable}"
                if raw:
                    ws = f"{ws}/value"
                if pos:
                    ws = ws + f"?pos={pos}"
        else:
            ws = f"{ws}/entities"
        return ws


class EntityService:
    """
    Get information about entities.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
        """
        Add variable command specific options
        """
        parser.add_argument("id", help="Identifier of the entity.")
        parser.add_argument(
            "--type",
            "-ty",
            required=False,
            help="Type of the entity. Default type is Participant.",
        )
        parser.add_argument(
            "--tables",
            "-ta",
            action="store_true",
            help="Get the list of tables in which the entity with given "
            "identifier exists.",
        )
        parser.add_argument(
            "--json",
            "-j",
            action="store_true",
            help="Pretty JSON formatting of the response",
        )

    @classmethod
    def do_command(self, args):
        """
        Execute data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = None
            if args.tables:
                res = EntityService(client, args.verbose).get_entity_tables(
                    args.id, args.type
                )
            else:
                res = EntityService(client, args.verbose).get_entity(args.id, args.type)

            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_entity(self, id: str, type: str = None) -> dict:
        request = self.client.new_request()
        if self.verbose:
            request.verbose()
        # send request
        response = (
            request.fail_on_error()
            .accept_json()
            .get()
            .resource(self._make_ws(id, type, False))
            .send()
        )
        return response.from_json()

    def get_entity_tables(self, id: str, type: str = None) -> list:
        request = self.client.new_request()
        if self.verbose:
            request.verbose()
        # send request
        response = (
            request.fail_on_error()
            .accept_json()
            .get()
            .resource(self._make_ws(id, type, True))
            .send()
        )
        return response.from_json()

    def _make_ws(self, id: str, type: str = None, tables: bool = False):
        """
        Build the web service resource path
        """
        ws = f"/entity/{id}/type/"
        ws = ws + type if type else ws + "Participant"
        if tables:
            ws = ws + "/tables"
        return ws
