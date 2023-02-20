"""
Apply permissions on a set of resources.
"""

import obiba_opal.core as core
import obiba_opal.perm as perm

PERMISSIONS = {
    'view': 'RESOURCE_VIEW',
    'administrate': 'RESOURCE_ALL'
}

def add_arguments(parser):
    """
    Add command specific options
    """
    perm.add_permission_arguments(parser, list(PERMISSIONS.keys()))
    parser.add_argument('--project', '-pr', required=True, help='Project name to which the resources belong')
    parser.add_argument('--resources', '-r', nargs='+', required=False,
                        help='List of resource names on which the permission is to be set (default is all)')

def retrieve_project_resources(args):
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error()
    if args.verbose:
        request.verbose()
    response = request.get().resource(
        core.UriBuilder(['project', args.project, 'resources']).build()).send().from_json()

    resources = []
    for resource in response:
        resources.append(str(resource['name']))

    return resources

def do_command(args):
    """
    Execute permission command
    """
    # Build and send requests
    perm.validate_args(args, PERMISSIONS)

    resources = args.resources
    if not resources:
        resources = retrieve_project_resources(args)

    for resource in resources:
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
                opal.perm.do_ws(args, ['project', args.project, 'permissions', 'resource', resource], PERMISSIONS)).send()
        except Exception as e:
            print(Exception, e)

        # format response
        if response.code != 200:
            print(response.content)
