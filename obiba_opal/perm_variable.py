"""
Apply permissions on a set of variables.
"""

import obiba_opal.core as core
import obiba_opal.perm as perm

PERMISSIONS = {
    'view': 'VARIABLE_READ',
}


def add_arguments(parser):
    """
    Add command specific options
    """
    perm.add_permission_arguments(parser, list(PERMISSIONS.keys()))
    parser.add_argument('--project', '-pr', required=True, help='Project name to which the tables belong')
    parser.add_argument('--table', '-t', required=True, help='Table name to which the variables belong')
    parser.add_argument('--variables', '-va', nargs='+', required=True,
                        help='List of variable names on which the permission is to be set')


def do_command(args):
    """
    Execute permission command
    """
    # Build and send requests
    perm.validate_args(args, PERMISSIONS)

    for variable in args.variables:
        request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()

        if args.verbose:
            request.verbose()

        # send request
        if args.delete:
            request.delete()
        else:
            request.post()

        response = request.resource(
            perm.do_ws(args,
                            ['project', args.project, 'permissions', 'table', args.table, 'variable', variable],
                            PERMISSIONS)).send()

        # format response
        if response.code != 200:
            print(response.content)
