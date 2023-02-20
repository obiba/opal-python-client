"""
Restore views of a project: upload view's JSON representation and make it a view.
"""

import obiba_opal.core as core
import os
import zipfile


def add_arguments(parser):
    """
    Add data command specific options
    """
    parser.add_argument('--project', '-pr', required=True, help='Destination project name')
    parser.add_argument('--views', '-vw', nargs='+', required=False,
                        help='List of view names to be restored (default is all the JSON files that are found in the backup directory/zip archive)')
    parser.add_argument('--input', '-in', required=False,
                        help='Input directory name or input zip file containing JSON views (default is current directory)')
    parser.add_argument('--force', '-f', action='store_true',
                        help='Skip confirmation when overwriting an existing view.')


def retrieve_datasource_views(args):
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error()
    if args.verbose:
        request.verbose()
    response = request.get().resource(
        core.UriBuilder(['datasource', args.project, 'tables']).build()).send().from_json()

    views = []
    for table in response:
        if 'viewLink' in table:
            views.append(str(table['name']))

    return views


def restore_view(args, obsviews, infile):
    view = os.path.basename(infile[:-5])  # supposed to be a .json file path

    dowrite = True
    if view in obsviews and not args.force:
        dowrite = False
        confirmed = input('Overwrite the view "' + view + '"? [y/N]: ')
        if confirmed == 'y':
            dowrite = True

    if dowrite:
        print('Restore of', view, 'from', infile, '...')

        request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
        request.fail_on_error()
        with open(infile, 'r') as inf:
            request.content(inf.read())
        request.content_type_json()

        if view in obsviews:
            request.put().resource(
                core.UriBuilder(['datasource', args.project, 'view', view]).query('comment',
                                                                                       'restore-view').build()).send()
        else:
            request.post().resource(
                core.UriBuilder(['datasource', args.project, 'views']).query('comment',
                                                                                  'restore-view').build()).send()


def restore_zipped_view(args, obsviews, infile, zippedinput):
    view = infile[:-5]  # supposed to be a .json file name

    dowrite = True
    if view in obsviews and not args.force:
        dowrite = False
        confirmed = input('Overwrite the view "' + view + '"? [y/N]: ')
        if confirmed == 'y':
            dowrite = True

    if dowrite:
        print('Restore of', view, 'from', infile, '...')

        request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
        request.fail_on_error()
        request.content(zippedinput.read(infile))
        request.content_type_json()

        if view in obsviews:
            request.put().resource(
                core.UriBuilder(['datasource', args.project, 'view', view]).query('comment',
                                                                                       'restore-view').build()).send()
        else:
            request.post().resource(
                core.UriBuilder(['datasource', args.project, 'views']).query('comment',
                                                                                  'restore-view').build()).send()


def list_json_files(dirref, basenames):
    matches = []
    for root, dirnames, filenames in os.walk(dirref):
        for filename in filenames:
            if filename.endswith('.json'):
                if not basenames or filename[:-5] in basenames:
                    matches.append(os.path.join(root, filename))
    return matches


def do_command(args):
    """
    Retrieve table DTOs of the project, look for the views, download the views in JSON into a file in provided or current directory
    """

    # Build and send request
    views = args.views
    obsviews = retrieve_datasource_views(args)

    # list input directory content
    indir = args.input
    if not indir:
        indir = os.getcwd()
    else:
        indir = os.path.normpath(indir)
    print('Input directory is', indir)

    if indir.endswith('.zip'):
        with zipfile.ZipFile(indir, 'r') as inzip:
            for viewfile in [filename for filename in inzip.namelist() if
                                filename.endswith('.json') and (not views or filename[:-5] in views)]:
                restore_zipped_view(args, obsviews, viewfile, inzip)
    else:
        for viewfile in list_json_files(indir, views):
            restore_view(args, obsviews, viewfile)
