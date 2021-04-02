"""
Execute SQL on a project's tables.
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
    parser.add_argument('--project', '-pr', required=True, help='Source project name')
    parser.add_argument('--query', '-q', required=True, help='SQL query')
    parser.add_argument('--format', '-f', required=False, help='The format of the output, can be "json" or "csv". Default is "csv".')
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
        if args.format == 'json' and args.id_name:
            builder.query('id', args.id_name)
        uri = builder.build()
        request = client.new_request()
        if args.verbose:
            request.verbose()
        request.fail_on_error()

        if args.format == 'json':
            request.accept_json().content_type_text_plain()
            response = request.post().resource(uri).content(args.query).send()
            # output to stdout
            if args.json:
                print(response.pretty_json())
            else:
                print(response.content)
        else:
            request.accept_text_csv().content_type_form_urlencoded()
            body = 'query=' + urllib.parse.quote(args.query)
            if args.id_name:
                body = body + '&id=' + urllib.parse.quote(args.id_name)
            response = request.post().resource(uri).content(body).send()
            # output to stdout
            print(response.content)

    except Exception as e:
        print(e)
        sys.exit(2)
    except pycurl.error as error:
        errno, errstr = error
        print('An error occurred: ', errstr, file=sys.stderr)
        sys.exit(2)
