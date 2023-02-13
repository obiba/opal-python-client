"""
Opal system data
"""

import obiba_opal.core as core
import sys


def add_arguments(parser):
    """
    Add system command specific options
    """
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    parser.add_argument('--version', action='store_true', required=False,
                        help='Opal version number')
    parser.add_argument('--env', action='store_true', required=False,
                        help='Opal java execution environment (JVM related statistic properties')
    parser.add_argument('--status', action='store_true', required=False,
                        help='Opal application status (JVM related dynamic properties)')
    parser.add_argument('--conf', action='store_true', required=False,
                        help='Opal application configuration')


def do_ws(args):
    """
    Build the web service resource path
    """
    if args.version:
        args.json = False
        return "/system/version"
    if args.env:
        return "/system/env"
    if args.status:
        return "/system/status"
    if args.conf:
        return "/system/conf"
    return "/system/conf/general"


def do_command(args):
    """
    Execute SYSTEM command
    """
    # Build and send request
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error()

    request.accept_json()

    if args.verbose:
        request.verbose()

    # send request
    request.get().resource(do_ws(args))
    response = request.send()

    # format response
    res = response.content

    if args.json:
        res = response.pretty_json()

    # output to stdout
    print(res)
