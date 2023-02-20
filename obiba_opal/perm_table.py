"""
Apply permissions on a set of tables.
"""

import obiba_opal.core as core
import obiba_opal.perm as perm

PERMISSIONS = {
    'view': 'TABLE_READ',
    'view-value': 'TABLE_VALUES',
    'edit': 'TABLE_EDIT',
    'edit-values': 'TABLE_VALUES_EDIT',
    'administrate': 'TABLE_ALL'
}

def add_arguments(parser):
    """
    Add command specific options
    """
    perm.add_permission_arguments(parser, list(PERMISSIONS.keys()))
    parser.add_argument('--project', '-pr', required=True, help='Project name to which the tables belong')
    parser.add_argument('--tables', '-t', nargs='+', required=False,
                        help='List of table names on which the permission is to be set (default is all)')

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
    Execute permission command
    """
    # Build and send requests
    perm.validate_args(args, PERMISSIONS)

    tables = args.tables
    if not tables:
        tables = retrieve_datasource_tables(args)

    for table in tables:
        request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()

        if args.verbose:
            request.verbose()

        # send request
        if args.delete:
            request.delete()
        else:
            request.post()

        response = request.resource(
            perm.do_ws(args, ['project', args.project, 'permissions', 'table', table], PERMISSIONS)).send()
        
        # format response
        if response.code != 200:
            print(response.content)
