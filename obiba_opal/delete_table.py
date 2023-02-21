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


def retrieve_datasource_tables(client: core.OpalClient, project: str, verbose: bool = False):
    request = client.new_request()
    request.fail_on_error()
    if verbose:
        request.verbose()
    response = request.get().resource(core.UriBuilder(['datasource', project, 'tables']).build()).send()

    tables = []
    for table in response.from_json():
        tables.append(str(table['name']))

    return tables


def delete_tables(client: core.OpalClient, project: str, tables: list, verbose: bool = False):
    """
    Execute delete table command

    :param client: Opal connection object
    :param project: The project name
    :param tables: List of table names to be deleted (default is all)
    :param verbose: Verbose requests
    """
    tables_ = tables
    if not tables:
        tables_ = retrieve_datasource_tables(client, args)
    
    for table in tables_:
        request = client.new_request()
        if verbose:
            request.verbose()
        # send request
        try:
            response = request.delete().resource(core.UriBuilder(['datasource', project, 'table', table]).build()).send()
            # format response
            if verbose and response.code != 200:
                print(response.content)
        except Exception as e:
            print(Exception, e)


def do_command(args):
    """
    Execute delete command
    """
    # Build and send requests
    client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
    try:
        delete_tables(client, args.project, args.tables, args.verbose)
    finally:
        client.close()
