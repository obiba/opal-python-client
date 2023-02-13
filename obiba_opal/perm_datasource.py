"""
Apply permissions on a datasource.
"""

import opal.core
import opal.perm
import sys

PERMISSIONS = {
    'view-value': 'DATASOURCE_VIEW',
    'add-table': 'TABLE_ADD',
    'administrate': 'DATASOURCE_ALL'
}


def add_arguments(parser):
    """
    Add command specific options
    """
    opal.perm.add_permission_arguments(parser, list(PERMISSIONS.keys()))
    parser.add_argument('--project', '-pr', required=True, help='Project name to which the tables belong')


def do_command(args):
    """
    Execute permission command
    """
    # Build and send requests
    try:
        opal.perm.validate_args(args, PERMISSIONS)

        request = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args)).new_request()

        if args.verbose:
            request.verbose()

        # send request
        if args.delete:
            request.delete()
        else:
            request.post()

        try:
            response = request.resource(
                opal.perm.do_ws(args, ['project', args.project, 'permissions', 'datasource'], PERMISSIONS)).send()
        except Exception as e:
            print(Exception, e)

        # format response
        if response.code != 200:
            print(response.content)

    except Exception as e:
        print(e)
        sys.exit(2)

    except pycurl.error as error:
        errno, errstr = error
        print('An error occurred: ', errstr, file=sys.stderr)
        sys.exit(2)
