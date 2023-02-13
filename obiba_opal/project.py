"""
Opal project management.
"""

import json
import obiba_opal.core as core
import sys


def add_arguments(parser):
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


def do_command(args):
    """
    Execute command
    """
    # Build and send request
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error()

    if args.verbose:
        request.verbose()

    if args.add:
        if not args.name:
            raise Exception('A project name is required.')
        # create project
        project = {'name': args.name}
        if args.database:
            project['database'] = args.database
        if args.title:
            project['title'] = args.title
        else:
            project['title'] = args.name
        if args.description:
            project['description'] = args.description
        if args.tags:
            project['tags'] = args.tags
        if args.export_folder:
            project['exportFolder'] = args.export_folder
        request.fail_on_error().accept_json().content_type_json()
        request.post().resource(core.UriBuilder(['projects']).build()).content(json.dumps(project)).send()
    elif args.delete:
        if not args.name:
            raise Exception('A project name is required.')
        # confirm
        if args.force:
            request.delete().resource(core.UriBuilder(['project', args.name]).build()).send()
        else:
            confirmed = input('Delete the project "' + args.name + '"? [y/N]: ')
            if confirmed == 'y':
                request.delete().resource(core.UriBuilder(['project', args.name]).build()).send()
            else:
                print('Aborted.')
                sys.exit(0)
    elif not args.name:
        response = request.get().resource(core.UriBuilder(['projects']).build()).send()
        # format response
        res = response.content
        if args.json:
            res = response.pretty_json()
        # output to stdout
        print(res)
    else:
        response = request.get().resource(core.UriBuilder(['project', args.name]).build()).send()
        # format response
        res = response.content
        if args.json:
            res = response.pretty_json()
        # output to stdout
        print(res)
