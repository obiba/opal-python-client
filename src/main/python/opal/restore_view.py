"""
Restore views of a project: upload view's JSON representation and make it a view.
"""

import sys
import opal.core
import opal.io
import os


def add_arguments(parser):
    """
    Add data command specific options
    """
    parser.add_argument('--project', '-pr', required=True, help='Destination project name')
    parser.add_argument('--views', '-vw', nargs='+', required=False,
                        help='List of view names to be restored (default is all the XML files that are found in the backup directory)')
    parser.add_argument('--input', '-in', required=False, help='Input directory name (default is current directory)')
    parser.add_argument('--force', '-f', action='store_true', help='Skip confirmation when overwriting an existing view.')


def retrieve_datasource_views(args):
    request = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error()
    if args.verbose:
        request.verbose()
    response = request.get().resource(
        opal.core.UriBuilder(['datasource', args.project, 'tables']).build()).send().as_json()

    views = []
    for table in response:
        if 'viewLink' in table:
            views.append(str(table[u'name']))

    return views

def restore_view(args, obsviews, infile):
    view = os.path.basename(infile[:-5]) # supposed to be a .json file path

    dowrite = True
    if view in obsviews and not args.force:
        dowrite = False
        print 'Overwrite the view "' + view + '"? [y/N]: ',
        confirmed = sys.stdin.readline().rstrip().strip()
        if confirmed == 'y':
            dowrite = True

    if dowrite:
        print 'Restore of', view, 'from', infile, '...'

        request = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args)).new_request()
        request.fail_on_error()
        with open(infile, 'r') as inf:
            request.content(inf.read())
        request.content_type_json()

        if view in obsviews:
            request.put().resource(
                opal.core.UriBuilder(['datasource', args.project, 'view', view]).query('comment', 'restore-view').build()).send()
        else:
            request.post().resource(
                opal.core.UriBuilder(['datasource', args.project, 'views']).query('comment', 'restore-view').build()).send()

def list_json_files(dir, basenames):
    matches = []
    for root, dirnames, filenames in os.walk(dir):
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
    try:
        views = args.views
        obsviews = retrieve_datasource_views(args)

        # list input directory content
        indir = args.input
        if not indir:
            indir = os.getcwd()
        else:
            indir = os.path.normpath(indir)
        print 'Input directory is', indir

        for viewfile in list_json_files(indir, views):
            restore_view(args, obsviews, viewfile)


    except Exception, e:
        print e
        sys.exit(2)
    except pycurl.error, error:
        errno, errstr = error
        print >> sys.stderr, 'An error occurred: ', errstr
        sys.exit(2)