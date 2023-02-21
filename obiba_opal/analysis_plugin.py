"""
Opal analysis plugin.
"""

import json
import obiba_opal.core as core
import re


def add_arguments(parser):
    """
    Add analyse command specific options
    """
    parser.add_argument('--project', '-pr', required=True, help='Project name')
    parser.add_argument('--config', '-c', required=True, help='A local JSON file containing the analysis configuration')
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')


def do_command(args):
    """
    Execute analysis
    """
    res = analyse(core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)), args.project, args.config, args.verbose)
    # format response
    core.Formatter.print_json(res, args.json)


def analyse(client: core.OpalClient, project: str, config: str, verbose: bool = False):
    """
    Execute analysis

    :param client: Opal connection object
    :param project: The project name
    :param config: A local JSON file containing the analysis configuration
    :param verbose: Verbose requests
    """
    dto = OpalAnalysisDtoFactory.create(project, config)
    request = client.new_request()
    request.fail_on_error().accept_json().content_type_json()
    ws = "/project/%s/commands/_analyse" % project
    response = request.post().resource(ws).content(json.dumps(dto)).send()

    # get job status
    location = response.get_location()
    job_resource = re.sub(r'http.*\/ws', r'', location)
    request = client.new_request()
    request.fail_on_error().accept_json()
    if verbose:
        request.verbose()
    response = request.get().resource(job_resource).send()
    return response.from_json()


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
