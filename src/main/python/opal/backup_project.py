"""
Backup a project: launch a backup task.
"""

import re
import sys
import opal.core
import opal.io

def add_arguments(parser):
    """
    Add command specific options
    """
    parser.add_argument('--project', '-pr', required=True, help='Source project name')
    parser.add_argument('--archive', '-ar', required=True, help='Archive directory path in the Opal file system')
    parser.add_argument('--views-as-tables', '-vt', action='store_true', help='Treat views as tables, i.e. export data instead of keeping derivation scripts (default is false)')
    parser.add_argument('--force', '-f', action='store_true', help='Force overwriting an existing backup folder')
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

def do_command(args):
    """
    Prepare the backup parameters and launch the backup task on the project
    """

    # Build and send request
    try:
        # backup options
        options = opal.protobuf.Commands_pb2.BackupCommandOptionsDto()
        options.archive = args.archive
        if args.views_as_tables:
            options.viewsAsTables = args.views_as_tables
        if args.force:
            options.override = args.force

        client = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args))
        uri = opal.core.UriBuilder(['project', args.project, 'commands', '_backup']).build()
        request = client.new_request()
        request.fail_on_error().accept_json().content_type_protobuf()
        if args.verbose:
            request.verbose()
        response = request.post().resource(uri).content(options.SerializeToString()).send()

        # get job status
        job_resource = re.sub(r'http.*\/ws', r'', response.headers['Location'])
        request = client.new_request()
        request.fail_on_error().accept_json()
        if args.verbose:
            request.verbose()
        response = request.get().resource(job_resource).send()
        # format response
        res = response.content
        if args.json:
            res = response.pretty_json()

        # output to stdout
        print res

    except Exception, e:
        print e
        sys.exit(2)
    except pycurl.error, error:
        errno, errstr = error
        print >> sys.stderr, 'An error occurred: ', errstr
        sys.exit(2)