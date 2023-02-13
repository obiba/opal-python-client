"""
Opal data dictionary.
"""

import obiba_opal.core as core


def add_arguments(parser):
    """
    Add variable command specific options
    """
    parser.add_argument('name',
                        help='Fully qualified name of a datasource/project or a table or a variable, for instance: opal-data or opal-data.questionnaire or opal-data.questionnaire:Q1. Wild cards can also be used, for instance: "*", "opal-data.*", etc.')
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')


def do_command(args):
    """
    Execute variable command
    """
    # Build and send request
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error().accept_json()

    if args.verbose:
        request.verbose()

    # send request
    request.get().resource(core.MagmaNameResolver(args.name).get_ws())
    response = request.send()

    # format response
    res = response.content
    if args.json:
        res = response.pretty_json()

    # output to stdout
    print(res)
