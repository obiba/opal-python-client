"""
Restore a project: launch a restore task.
"""

import re
import sys
import opal.core
import opal.io

def add_arguments(parser):
    """
    Add command specific options
    """
    parser.add_argument('--project', '-pr', required=True, help='Destination project name (must exist)')
    parser.add_argument('--archive', '-ar', required=True, help='Archive directory or zip file path in the Opal file system')
    parser.add_argument('--arpassword', '-arp', required=False, help='Password to decrypt zip archive (optional)')
    parser.add_argument('--force', '-f', action='store_true', help='Force overwriting existing items (table, view, resource, report). Files override is not checked')
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

def do_command(args):
    """
    Prepare the restore parameters and launch the restore task on the project
    """

    # Build and send request
    try:
        # restore options
        options = opal.protobuf.Commands_pb2.RestoreCommandOptionsDto()
        options.archive = args.archive
        if args.force:
            options.override = args.force
        if args.arpassword:
            options.password = args.arpassword

        client = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args))
        uri = opal.core.UriBuilder(['project', args.project, 'commands', '_restore']).build()
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