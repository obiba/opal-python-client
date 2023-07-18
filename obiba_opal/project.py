"""
Opal project management.
"""

import obiba_opal.core as core
import json
import sys
import re

class ProjectService:
    """
    Project management.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        parser.add_argument('--name', '-n', required=False,
                            help='Project name. Not specifying the project name, will get the list of the projects.')
        parser.add_argument('--database', '-db', required=False,
                            help='Project database name. If not provided only views can be added.')
        parser.add_argument('--title', '-t', required=False, help='Project title.')
        parser.add_argument('--description', '-dc', required=False, help='Project description.')
        parser.add_argument('--tags', '-tg', nargs='+', required=False, help='Tags to apply to the project.')
        parser.add_argument('--export-folder', '-ex', required=False, help='Project preferred export folder.')

        parser.add_argument('--add', '-a', action='store_true', help='Add a project (requires at least a project name).')
        parser.add_argument('--delete', '-de', action='store_true', required=False,
                            help='Delete a project (requires at least a project name).')
        parser.add_argument('--force', '-f', action='store_true', help='Skip confirmation on project deletion')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))

        service = ProjectService(client, args.verbose)

        if args.add:
            service.add_project(args.name, args.database, args.title, args.description, args.tags, args.export_folder)
        elif args.delete:
            if not args.name:
                raise Exception('A project name is required.')
            # confirm
            if args.force:
                service.delete_project(args.name)
            else:
                confirmed = input('Delete the project "' + args.name + '"? [y/N]: ')
                if confirmed == 'y':
                    service.delete_project(args.name)
                else:
                    print('Aborted.')
                    sys.exit(0)
        elif not args.name:
            res = service.get_projects()
            core.Formatter.print_json(res, args.json)
        else:
            res = service.get_project(args.name, fail_safe=False)
            core.Formatter.print_json(res, args.json)

    def get_projects(self) -> list:
        """
        Get the projects.
        """
        request = self._make_request()
        response = request.get().resource(core.UriBuilder(['projects']).build()).send()
        return response.from_json()

    def get_project(self, name: str, fail_safe: bool = True) -> dict:
        """
        Get the project by its name.

        :param name: The project name
        """
        if not name:
            raise ValueError('The project name is required.')
        request = self._make_request(fail_safe)
        response = request.get().resource(core.UriBuilder(['project', name]).build()).send()
        return response.from_json() if response.code == 200 else None

    def delete_project(self, name: str):
        """
        Delete the project by its name.

        :param name: The project name
        """
        if not name:
            raise ValueError('The project name is required.')
        request = self._make_request()
        request.delete().resource(core.UriBuilder(['project', name]).build()).send()

    def add_project(self, name: str, database: str = None, title: str = None, description: str = None, tags: list = None, export_folder: str = None):
        """
        Add a project.

        :param name: The project name
        :param database: The project database name. If not provided only views can be added. See get_databases() for the list of databases available for storage.
        :param title: The project title
        :param description: The project description
        :param tags: The list of project tags
        :param export_folder: The project's preferred export folder
        """
        if not name:
            raise ValueError('The project name is required.')

        # create project
        project = {'name': name}
        if database:
            project['database'] = database
        if title:
            project['title'] = title
        else:
            project['title'] = name
        if description:
            project['description'] = description
        if tags:
            project['tags'] = tags
        if export_folder:
            project['exportFolder'] = export_folder

        request = self._make_request()
        request.accept_json().content_type_json()
        request.post().resource(core.UriBuilder(['projects']).build()).content(json.dumps(project)).send()

    def get_databases(self, usage: str = 'storage') -> list:
        """
        Get the databases available.

        :param usage: Database usage: 'storage' (default), 'import' or 'export'
        """
        request = self._make_request()
        request.accept_json()
        response = request.get().resource(core.UriBuilder(['system', 'databases']).query('usage', usage).build()).send()
        return response.from_json()

    def _make_request(self, fail_safe: bool = False):
        request = self.client.new_request()
        if not fail_safe:
            request.fail_on_error()
        if self.verbose:
            request.verbose()
        return request


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


class RestoreProjectCommand:
    """
    Restore a project: launch a restore task.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        parser.add_argument('--project', '-pr', required=True, help='Destination project name (must exist)')
        parser.add_argument('--archive', '-ar', required=True,
                            help='Archive directory or zip file path in the Opal file system')
        parser.add_argument('--arpassword', '-arp', required=False, help='Password to decrypt zip archive (optional)')
        parser.add_argument('--force', '-f', action='store_true',
                            help='Force overwriting existing items (table, view, resource, report). Files override is not checked')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Prepare the restore parameters and launch the restore task on the project
        """

        # Build and send request
        # restore options
        options = {'archive': args.archive}
        if args.force:
            options['override'] = args.force
        if args.arpassword:
            options['password'] = args.arpassword

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        uri = core.UriBuilder(['project', args.project, 'commands', '_restore']).build()
        request = client.new_request()
        request.fail_on_error().accept_json().content_type_json()
        if args.verbose:
            request.verbose()
        response = request.post().resource(uri).content(json.dumps(options)).send()

        # get job status
        location = None
        if 'Location' in response.headers:
            location = response.headers['Location']
        elif 'location' in response.headers:
            location = response.headers['location']
        job_resource = re.sub(r'http.*\/ws', r'', location)
        request = client.new_request()
        request.fail_on_error().accept_json()
        if args.verbose:
            request.verbose()
        response = request.get().resource(job_resource).send()
        # format response
        res = response.content.decode('utf-8')
        if args.json:
            res = response.pretty_json()

        # output to stdout
        print(res)
