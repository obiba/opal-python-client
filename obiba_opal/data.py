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
    parser.add_argument('--raw', '-r', action='store_true', help='Raw value')
    parser.add_argument('--pos', '-po', required=False,
                        help='Position of the value to query in case of a repeatable variable (starting at 0).')
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')


def do_ws(args):
    """
    Build the web service resource path
    """
    resolver = core.MagmaNameResolver(args.name)
    ws = resolver.get_table_ws()
    if args.id:
        ws = ws + '/valueSet/' + args.id
        if resolver.is_variable():
            ws = ws + '/variable/' + resolver.variable
            if args.raw:
                ws = ws + '/value'
            if args.pos:
                ws = ws + '?pos=' + args.pos
    else:
        ws = ws + '/entities'
    return ws


def do_command(args):
    """
    Execute data command
    """
    # Build and send request
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error()

    if args.verbose:
        request.verbose()

    # send request
    if args.raw:
        fp = os.fdopen(sys.stdout.fileno(), 'wb')
        response = request.get().resource(do_ws(args)).accept('*/*').send(fp)
        fp.flush()
    else:
        response = request.get().resource(do_ws(args)).send()

        # format response
        res = response.content
        if args.json:
            res = response.pretty_json()

        # output to stdout
        print(res)
