"""
Execute SQL on a project's tables.
"""

import obiba_opal.core as core
import urllib.parse


class SQLService:
    """
    Execute SQL queries.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        parser.add_argument(
            "--project",
            "-pr",
            required=False,
            help="Source project name, that will be used to resolve the table names in the FROM statement. If not "
            "provided, the fully qualified table names must be specified in the query (escaped by backquotes: "
            "`<project>.<table>`).",
        )
        parser.add_argument("--query", "-q", required=True, help="SQL query")
        parser.add_argument(
            "--format",
            "-f",
            required=False,
            help='The format of the output, can be "json" or "csv". Default is "csv".',
        )
        parser.add_argument(
            "--id-name",
            "-in",
            required=False,
            help='Name of the ID column name. Default is "_id".',
        )
        parser.add_argument(
            "--json",
            "-j",
            action="store_true",
            help="Pretty JSON formatting of the response",
        )

    @classmethod
    def do_command(cls, args):
        """
        Execute the SQL query on the project
        """

        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            if args.project:
                builder = core.UriBuilder(["datasource", args.project, "_sql"])
            else:
                builder = core.UriBuilder(["datasources", "_sql"])
            if args.format == "json" and args.id_name:
                builder.query("id", args.id_name)
            uri = builder.build()
            request = client.new_request()
            if args.verbose:
                request.verbose()
            request.fail_on_error()

            if args.format == "json":
                request.accept_json().content_type_text_plain()
                response = request.post().resource(uri).content(args.query).send()
                # output to stdout
                if args.json:
                    print(response.pretty_json())
                else:
                    print(str(response))
            else:
                request.accept_text_csv().content_type_form_urlencoded()
                body = "query=" + urllib.parse.quote(args.query)
                if args.id_name:
                    body = body + "&id=" + urllib.parse.quote(args.id_name)
                response = request.post().resource(uri).content(body).send()
                # output to stdout
                print(str(response))
        finally:
            client.close()


class SQLHistoryService:
    """
    Get and filter SQL service history.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        parser.add_argument(
            "--project",
            "-pr",
            required=False,
            help="Project name used as the SQL execution context to filter. If not specified, history from any context "
            "is returned. If '*' is specified, history of SQL execution without context is returned.",
        )
        parser.add_argument(
            "--offset",
            "-os",
            required=False,
            help="Number of history items to skip. Default is 0 (note that the items are ordered by most recent "
            "first).",
        )
        parser.add_argument(
            "--limit",
            "-lm",
            required=False,
            help="Maximum number of history items to return. Default is 100.",
        )
        parser.add_argument(
            "--subject",
            "-sb",
            required=False,
            help="Filter by user name, only administrators can retrieve SQL history of other users. If '*' is "
            "specified, history of all users is retrieved. Default is the current user name.",
        )
        parser.add_argument(
            "--json",
            "-j",
            action="store_true",
            help="Pretty JSON formatting of the response",
        )

    @classmethod
    def do_command(cls, args):
        """
        SQL query history
        """

        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            if args.subject and args.subject != args.user:
                builder = core.UriBuilder([
                    "system",
                    "subject-profile",
                    args.subject,
                    "sql-history",
                ])
            else:
                builder = core.UriBuilder([
                    "system",
                    "subject-profile",
                    "_current",
                    "sql-history",
                ])
            if args.project:
                builder.query("datasource", args.project)
            if args.offset:
                builder.query("offset", args.offset)
            if args.limit:
                builder.query("limit", args.limit)
            uri = builder.build()
            request = client.new_request()
            if args.verbose:
                request.verbose()
            request.fail_on_error()
            response = request.accept_json().get().resource(uri).send()
            # output to stdout
            if args.json:
                print(response.pretty_json())
            else:
                print(str(response))
        finally:
            client.close()
