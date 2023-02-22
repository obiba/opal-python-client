"""
Backup a project: launch a backup task.
"""

import json
import obiba_opal.core as core
import re

class BackupProjectCommand:
    """
    Execute project backup command.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
        """
        Add command specific options
        """
        parser.add_argument('--project', '-pr', required=True, help='Source project name')
        parser.add_argument('--archive', '-ar', required=True, help='Archive directory path in the Opal file system')
        parser.add_argument('--views-as-tables', '-vt', action='store_true',
                            help='Treat views as tables, i.e. export data instead of keeping derivation scripts (default is false)')
        parser.add_argument('--force', '-f', action='store_true', help='Force overwriting an existing backup folder')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(self, args):
        """
        Prepare the backup parameters and launch the backup task on the project
        """
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = BackupProjectCommand(client, args.verbose).backup_project(args.project, args.archive, args.views_as_tables, args.force)
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def backup_project(self, project: str, archive: str, views_as_tables: bool = False, force: bool = False) -> dict:
        """
        Prepare the backup parameters and launch the backup task on the project

        :param project: The project name
        :param archive: The archive directory path in the Opal file system
        :param views_as_tables: Treat views as tables, i.e. export data instead of keeping derivation scripts
        :param force: Force overwriting an existing backup folder
        """
        # Build and send request
        # backup options
        options = {'archive': archive}
        if views_as_tables:
            options['viewsAsTables'] = views_as_tables
        if force:
            options['override'] = force

        uri = core.UriBuilder(['project', project, 'commands', '_backup']).build()
        request = self.client.new_request()
        request.fail_on_error().accept_json().content_type_json()
        if self.verbose:
            request.verbose()
        response = request.post().resource(uri).content(json.dumps(options)).send()

        # get job status
        location = response.get_location()
        job_resource = re.sub(r'http.*\/ws', r'', location)
        request = self.client.new_request()
        request.fail_on_error().accept_json()
        if self.verbose:
            request.verbose()
        response = request.get().resource(job_resource).send()
        return response.from_json()
