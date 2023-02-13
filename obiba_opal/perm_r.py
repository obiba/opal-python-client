"""
Apply R permissions.
"""

import obiba_opal.core as core
import obiba_opal.perm as perm

PERMISSIONS = {
    'use': 'R_USE'
}


def add_arguments(parser):
    """
    Add command specific options
    """
    perm.add_permission_arguments(parser, list(PERMISSIONS.keys()))


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

    response = request.resource(perm.do_ws(args, ['system', 'permissions', 'r'], PERMISSIONS)).send()

    # format response
    if response.code != 200:
        print(response.content)
