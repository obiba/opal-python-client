"""
Delete some tables.
"""

import obiba_opal.core as core
import obiba_opal.perm as perm


def add_arguments(parser):
    """
    Add command specific options
    """
    parser.add_argument('--project', '-pr', required=True, help='Project name to which the tables belong')
    parser.add_argument('--tables', '-t', nargs='+', required=False,
                        help='List of table names which will be deleted (default is all)')


def do_ws(args, table):
    """
    Build the web service resource path
    """

    if args.add:
        return core.UriBuilder(['project', args.project, 'permissions', 'table', table]) \
            .query('type', args.type.upper()) \
            .query('permission', perm.map_permission(args.permission)) \
            .query('principal', args.subject) \
            .build()

    if args.delete:
        return core.UriBuilder(['project', args.project, 'permissions', 'table', table]) \
            .query('type', args.type.upper()) \
            .query('principal', args.subject) \
            .build()


def retrieve_datasource_tables(args):
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error()
    if args.verbose:
        request.verbose()
    response = request.get().resource(
        core.UriBuilder(['datasource', args.project, 'tables']).build()).send().from_json()

    tables = []
    for table in response:
        tables.append(str(table['name']))

    return tables


def do_command(args):
    """
    Execute delete command
    """
    # Build and send requests
    tables = args.tables
    if not tables:
        tables = retrieve_datasource_tables(args)

    for table in tables:
        request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()

        if args.verbose:
            request.verbose()

        request.delete()

        # send request
        try:
            response = request.resource(
                core.UriBuilder(['datasource', args.project, 'table', table]).build()).send()
        except Exception as e:
            print(Exception, e)

        # format response
        if response.code != 200:
            print(response.content)
