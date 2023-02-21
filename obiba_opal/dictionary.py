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


def get_dictionary(client: core.OpalClient, name: str, verbose: bool = False):
    """
    Execute variable command

    :param client: Opal connection object
    :param name: Fully qualified name of a datasource/project or a table or a variable, for instance: opal-data or opal-data.questionnaire or opal-data.questionnaire:Q1. Wild cards can also be used, for instance: "*", "opal-data.*", etc.
    :param verbose: Verbose requests
    """
    request = client.new_request()
    request.fail_on_error().accept_json()

    if verbose:
        request.verbose()

    # send request
    request.get().resource(core.MagmaNameResolver(name).get_ws())
    response = request.send()
    return response.from_json()


def do_command(args):
    """
    Execute variable command
    """
    # Build and send request
    client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
    try:
        res = get_dictionary(client, args.name, args.verbose)
    
        # format response
        core.Formatter.print_json(res, args.json)
    finally:
        client.close()