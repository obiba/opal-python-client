"""
Execute SQL on a project's tables.
"""

import json
import opal.core
import opal.io
import re
import sys


def add_arguments(parser):
    """
    Add command specific options
    """
    parser.add_argument('--project', '-pr', required=True, help='Source project name')
    parser.add_argument('--query', '-q', required=True, help='SQL query')
    parser.add_argument('--id-name', '-in', required=False, help='Name of the ID column name. Default is "_id".')
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')


def do_command(args):
    """
    Execute the SQL query on the project
    """

    # Build and send request
    try:
        client = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args))
        builder = opal.core.UriBuilder(['datasource', args.project, '_sql'])
        if args.id_name:
            builder.query('id', args.id_name)
        uri = builder.build()
        request = client.new_request()
        request.fail_on_error().accept_json().content_type_text_plain()
        if args.verbose:
            request.verbose()
        response = request.post().resource(uri).content(args.query).send()

        # format response
        res = response.content
        if args.json:
            res = response.pretty_json()

        # output to stdout
        print(res)

    except Exception as e:
        print(e)
        sys.exit(2)
    except pycurl.error as error:
        errno, errstr = error
        print('An error occurred: ', errstr, file=sys.stderr)
        sys.exit(2)
