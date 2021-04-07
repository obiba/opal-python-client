"""
Get SQL execution history.
"""

import json
import opal.core
import opal.io
import re
import sys
import urllib.parse


def add_arguments(parser):
    """
    Add command specific options
    """
    parser.add_argument('--project', '-pr', required=False, help='Project name used as the SQL execution context to filter. If not specified, history from any context is returned. If \'*\' is specified, history of SQL execution without context is returned.')
    parser.add_argument('--offset', '-os', required=False, help='Number of history items to skip. Default is 0 (note that the items are ordered by most recent first).')
    parser.add_argument('--limit', '-lm', required=False, help='Maximum number of history items to return. Default is 100.')
    parser.add_argument('--subject', '-sb', required=False, help='Filter by user name, only administrators can retrieve SQL history of other users. If \'*\' is specified, history of all users is retrieved. Default is the current user name.')
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')


def do_command(args):
    """
    SQL query history
    """

    # Build and send request
    try:
        client = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args))
        if args.subject and args.subject != args.user:
            builder = opal.core.UriBuilder(['system', 'subject-profile', args.subject, 'sql-history'])
        else:
            builder = opal.core.UriBuilder(['system', 'subject-profile', '_current', 'sql-history'])
        if args.project:
            builder.query('datasource', args.project)
        if args.offset:
            builder.query('offset', args.offset)
        if args.limit:
            builder.query('limit', args.limit)
        uri = builder.build()
        request = client.new_request()
        if args.verbose:
            request.verbose()
        request.fail_on_error()
        response = request.accept_json().get().resource(uri).send()
        # output to stdout
        if args.json:
            print(response.pretty_json())
        else:
            print(response.content)

    except Exception as e:
        print(e)
        sys.exit(2)
    except pycurl.error as error:
        errno, errstr = error
        print('An error occurred: ', errstr, file=sys.stderr)
        sys.exit(2)
