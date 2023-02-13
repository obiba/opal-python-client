"""
Opal groups management.
"""

import obiba_opal.core as core
import sys


def add_arguments(parser):
    """
    Add data command specific options
    """
    parser.add_argument('--name', '-n', required=False,
                        help='Group name.')
    parser.add_argument('--fetch', '-fe', action='store_true', required=False,
                        help='Fetch one or multiple group(s).')
    parser.add_argument('--delete', '-de', action='store_true', required=False,
                        help='Delete a group.')
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')


def do_ws(args):
    """
    Build the web service resource path
    """

    if args.name and args.fetch or args.delete:
        ws = "/system/group/" + args.name
    else:
        ws = "/system/groups"

    return ws


def do_command(args):
    """
    Execute group command
    """
    # Build and send request
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error()

    if args.verbose:
        request.verbose()

    if args.fetch:
        # send request
        response = request.get().resource(do_ws(args)).send()
    elif args.delete:
        if not args.name:
            raise Exception('A group name is required.')

        response = request.delete().resource(do_ws(args)).send()

    # format response
    res = response.content
    if args.json:
        res = response.pretty_json()

    # output to stdout
    print(res)
