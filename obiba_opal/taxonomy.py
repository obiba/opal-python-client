"""
Opal taxonomy management.
"""

import opal.core
import pycurl
import sys


class OpalTaxonomyResource:
    """
    Taxonomy reference
    """

    def __init__(self, name):
        self.name = name

    def get_ws(self):
        return '/system/conf/taxonomy/' + self.name

    def get_download_ws(self):
        return '/system/conf/taxonomy/' + self.name + '/_download'

def add_arguments(parser):
    """
    Add file command specific options
    """
    parser.add_argument('--download', '-dl', required=False, help='Download a taxonomy by name (YAML format).')
    parser.add_argument('--import-file', '-if', required=False, help='Import a taxonomy from the provided Opal file path (YAML format).')
    parser.add_argument('--delete', '-dt', required=False, help='Delete a taxonomy by name.')
    parser.add_argument('--force', '-f', action='store_true', help='Skip confirmation.')
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')


def do_command(args):
    """
    Execute taxonomy command
    """
    # Build and send request
    try:
        request = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args)).new_request()
        request.fail_on_error().accept_json()

        if args.verbose:
            request.verbose()


        # send request
        if args.download:
            taxo = OpalTaxonomyResource(args.download)
            response = request.get().resource(taxo.get_download_ws()).accept('text/plain').send()
        elif args.import_file:
            response = request.post().resource(opal.core.UriBuilder(['system', 'conf', 'taxonomies', 'import', '_file']).query('file',args.import_file).build()).send()
        elif args.delete:
            taxo = OpalTaxonomyResource(args.delete)
            # confirm
            if args.force:
                response = request.delete().resource(taxo.get_ws()).send()
            else:
                confirmed = input('Delete the taxonomy "' + args.delete + '"? [y/N]: ')
                if confirmed == 'y':
                    response = request.delete().resource(taxo.get_ws()).send()
                else:
                    print('Aborted.')
                    sys.exit(0)
        else:
            response = request.get().resource('/system/conf/taxonomies/summaries').send()

        # format response
        res = response.content
        if args.json and not args.download and not args.delete and not args.import_file:
            res = response.pretty_json()

        # output to stdout
        print(res)
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(2)
    except pycurl.error as error:
        print(response)
        errno, errstr = error
        print('An error occurred: ', errstr, file=sys.stderr)
        sys.exit(2)
