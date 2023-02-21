"""
Opal data.
"""

import obiba_opal.core as core
import sys
import os

def add_arguments(parser):
    """
    Add data command specific options
    """
    parser.add_argument('name',
                        help='Fully qualified name of a table or a variable, for instance: opal-data.questionnaire or opal-data.questionnaire:Q1.')
    parser.add_argument('--id', '-i', required=False,
                        help='Entity identifier. If missing the list of entities is returned.')
    parser.add_argument('--raw', '-r', action='store_true', help='Get raw value, output to stdout, useful for downloading a binary value')
    parser.add_argument('--pos', '-po', required=False,
                        help='Position of the value to query in case of a repeatable variable (starting at 0).')
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')


def do_ws(name: str, id: str = None, pos: str = None, raw: bool = False):
    """
    Build the web service resource path
    """
    resolver = core.MagmaNameResolver(name)
    ws = resolver.get_table_ws()
    if id:
        ws = '%s/valueSet/%s' % (ws, id)
        if resolver.is_variable():
            ws = '%s/variable/%s' % (ws, resolver.variable)
            if raw:
                ws = '%s/value' % ws
            if pos:
                ws = ws + '%s?pos=%s' % (ws, pos)
    else:
        ws = '%s/entities' % ws
    return ws

def get_data(client: core.OpalClient, name: str, id: str = None, pos: str = None, raw: bool = False, verbose: bool = False):
    """
    Execute data command

    :param client: Opal connection object
    :param name: Fully qualified name of a table or a variable, for instance: opal-data.questionnaire or opal-data.questionnaire:Q1
    :param id: Entity identifier. If missing the list of entities is returned
    :param pos: Position of the value to query in case of a repeatable variable (starting at 0)
    :param raw: Get raw value, output on stdout, useful for downloading a binary value
    :param verbose: Verbose requests
    """
    request = client.new_request()
    if verbose:
        request.verbose()
    
    ws = do_ws(name, id, pos, raw)
    request.fail_on_error().get().resource(ws)
    if raw:
        fp = os.fdopen(sys.stdout.fileno(), 'wb')
        response = request.accept('*/*').send(fp)
        fp.flush()
        return None
    else:
        response = request.send()
        return response.from_json()


def do_command(args):
    """
    Execute data command
    """
    # Build and send request
    client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
    try:
        res = get_data(client, args.name, args.id, args.pos, args.raw, args.verbose)
        # format response
        core.Formatter.print_json(res, args.json)
    finally:
        client.close()