"""
Apply permissions on a project.
"""

import obiba_opal.core as core
import obiba_opal.perm as perm

PERMISSIONS = {
    'administrate': 'PROJECT_ALL'
}


def add_arguments(parser):
    """
    Add command specific options
    """
    perm.add_permission_arguments(parser, list(PERMISSIONS.keys()))
    parser.add_argument('--project', '-pr', required=True, help='Project name')


def do_command(args):
    """
    Execute permission command
    """
    # Build and send requests
    perm.validate_args(args, PERMISSIONS)

    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()

    if args.verbose:
        request.verbose()

    # send request
    if args.delete:
        request.delete()
    else:
        request.post()

    try:
        response = request.resource(
            perm.do_ws(args, ['project', args.project, 'permissions', 'project'], PERMISSIONS)).send()
    except Exception as e:
        print(Exception, e)

    # format response
    if response.code != 200:
        print(response.content)
