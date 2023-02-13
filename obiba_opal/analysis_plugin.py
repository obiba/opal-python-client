"""
Opal analysis plugin.
"""

import json
import opal.core
import opal.io
import sys


def do_ws(args):
    """
    Build the web service resource path
    """
    ws = "/project/%s/commands/_analyse" % args.project

    return ws


def add_arguments(parser):
    """
    Add analyse command specific options
    """
    parser.add_argument('--project', '-pr', required=True, help='Project name')
    parser.add_argument('--config', '-c', required=True, help='A JSON file containing the analysis configuration')


def do_command(args):
    """
    Execute analysis
    """
    # Build and send request
    try:
        dto = OpalAnalysisDtoFactory.create(args.project, args.config)
        request = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args)).new_request()
        request.fail_on_error().accept_json().content_type_json()
        request.post().resource(do_ws(args)).content(json.dumps(dto)).send()

    except Exception as e:
        print(e)
        sys.exit(2)
    except pycurl.error as error:
        errno, errstr = error
        print('An error occurred: ', errstr, file=sys.stderr)
        sys.exit(2)


class OpalAnalysisDtoFactory():

    @classmethod
    def create(self, project, config):
        """
        Create an analysis option DTO
        """
        dto = {'project': project}
        configJson = json.loads(open(config, 'r').read())
        if type(configJson) is list:
            dto['analyses'] = configJson
        else:
            dto['analyses'] = [configJson]
        return dto
