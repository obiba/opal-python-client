"""
Opal analysis plugin.
"""

import json
import obiba_opal.core as core
import re


class AnalysisCommand:
    """
    Execute analyses using R analysis plugins.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
        """
        Add analyse command specific options
        """
        parser.add_argument('--project', '-pr', required=True, help='Project name')
        parser.add_argument('--config', '-c', required=True, help='A local JSON file containing the analysis configuration')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(self, args):
        """
        Execute analysis
        """
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = AnalysisCommand(client, args.verbose).analyse(args.project, args.config)
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close

    def analyse(self, project: str, config: str) -> dict:
        """
        Execute analysis

        :param project: The project name
        :param config: A local JSON file containing the analysis configuration
        """
        dto = self._create_dto(project, config)
        request = self.client.new_request()
        request.fail_on_error().accept_json().content_type_json()
        ws = "/project/%s/commands/_analyse" % project
        response = request.post().resource(ws).content(json.dumps(dto)).send()

        # get job status
        location = response.get_location()
        job_resource = re.sub(r'http.*\/ws', r'', location)
        request = self.client.new_request()
        request.fail_on_error().accept_json()
        if self.verbose:
            request.verbose()
        response = request.get().resource(job_resource).send()
        return response.from_json()

    def _create_dto(self, project, config):
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
