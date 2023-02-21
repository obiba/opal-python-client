"""
Backup views of a project: download view's JSON representation and save it in a file, one for each view.
"""

import obiba_opal.core as core
import os


def add_arguments(parser):
    """
    Add command specific options
    """
    parser.add_argument('--project', '-pr', required=True, help='Source project name')
    parser.add_argument('--views', '-vw', nargs='+', required=False,
                        help='List of view names to be backed up (default is all)')
    parser.add_argument('--output', '-out', required=False, help='Output directory name (default is current directory)')
    parser.add_argument('--force', '-f', action='store_true',
                        help='Skip confirmation when overwriting the backup file.')


def retrieve_datasource_views(client: core.OpalClient, project: str, verbose: bool = False):
    request = client.new_request()
    request.fail_on_error()
    if verbose:
        request.verbose()
    response = request.get().resource(
        core.UriBuilder(['datasource', project, 'tables']).build()).send().from_json()

    views = []
    for table in response:
        if 'viewLink' in table:
            views.append(str(table['name']))

    return views


def backup_view(client: core.OpalClient, project: str, view, outdir, force: bool, verbose: bool = False):
    outfile = view + '.json'
    print('Backup of', view, 'in', outfile, '...')

    outpath = os.path.join(outdir, outfile)

    request = client.new_request()
    request.fail_on_error()
    if verbose:
        request.verbose()
    response = request.get().resource(
        core.UriBuilder(['datasource', project, 'view', view]).build()).send()

    dowrite = True
    if os.path.exists(outpath) and not force:
        dowrite = False
        confirmed = input('Overwrite the file "' + outpath + '"? [y/N]: ')
        if confirmed == 'y':
            dowrite = True

    if dowrite:
        out = open(outpath, 'w+')
        out.write(response.content)
        out.close()


def backup_views(client: core.OpalClient, project: str, views: list, output: str, force: bool, verbose: bool = False):
    """
    Retrieve table DTOs of the project, look for the views, download the views in JSON into a file in provided or current directory
    
    :param client: Opal connection object
    :param project: The project name
    :param views: List of view names to be backed up (default is all)
    :param output: Output directory name (default is current directory)
    :param force: Skip confirmation when overwriting the backup file
    :param verbose: Verbose requests
    """

    views_ = views
    obsviews = retrieve_datasource_views(client, project, verbose)
    if not views_:
        views_ = obsviews
    else:
        safeviews = []
        for view in views_:
            if view in obsviews:
                safeviews.append(view)
        views_ = safeviews
    if not views_:
        print('No views to backup in project', project)
    else:
        # prepare output directory
        outdir = output
        if not outdir:
            outdir = os.getcwd()
        else:
            outdir = os.path.normpath(outdir)
        print('Output directory is', outdir)
        if not os.path.exists(outdir):
            print('Creating output directory ...')
            os.makedirs(outdir)

        # backup each view
        for view in views:
            backup_view(client, project, view, outdir, force, verbose)


def do_command(args):
    """
    Retrieve table DTOs of the project, look for the views, download the views in JSON into a file in provided or current directory
    """

    # Build and send request
    client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
    try:
        backup_views(client, args.project, args.views, args.output, args.force, args.verbose)
    finally:
        client.close()