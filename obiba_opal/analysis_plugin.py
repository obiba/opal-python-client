"""
Opal analysis plugin.
"""

import json
import obiba_opal.core as core


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
    dto = OpalAnalysisDtoFactory.create(args.project, args.config)
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error().accept_json().content_type_json()
    request.post().resource(do_ws(args)).content(json.dumps(dto)).send()

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
