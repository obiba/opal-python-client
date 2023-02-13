"""
Import some VCF/BCF files.
"""

import json
import obiba_opal.core as core


def add_arguments(parser):
    """
    Add command specific options
    """
    parser.add_argument('--project', '-pr', required=True,
                        help='Project name into which genotypes data will be imported')
    parser.add_argument('--vcf', '-vcf', nargs='+', required=True,
                        help='List of VCF/BCF (optionally compressed) file paths (in Opal file system)')


def do_command(args):
    """
    Execute delete command
    """
    # Build and send requests
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error().accept_json().content_type_json()
    if args.verbose:
        request.verbose()

    options = {
        'project': args.project,
        'files': args.vcf
    }
    # send request
    uri = core.UriBuilder(['project', args.project, 'commands', '_import_vcf']).build()
    request.resource(uri).post().content(json.dumps(options)).send()
