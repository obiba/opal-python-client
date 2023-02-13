"""
Opal Export analysis plugin.
"""

import obiba_opal.core as core


def do_ws(args):
    """
    Build the web service resource path
    """
    if args.table is None:
        ws = "/project/%s/analyses/_export" % args.project
    elif args.analysis_id is None:
        ws = "/project/%s/table/%s/analyses/_export" % (args.project, args.table)
    else:
        ws = "/project/%s/table/%s/analysis/%s/_export" % (args.project, args.table, args.analysis_id)

    return "%s?all=true" % ws if args.all_results else ws


def add_arguments(parser):
    """
    Add export analysis command specific options
    """
    parser.add_argument('--project', '-pr', required=True,
                        help='Project name for which analysis data will be exported.')
    parser.add_argument('--table', '-t', required=False, help='Table name for which analysis data will be exported.')
    parser.add_argument('--all-results', '-ar', action='store_true',
                        help='Export all results (default exports last result).')
    parser.add_argument('--analysis-id', '-ai', required=False,
                        help='A table Analysis ID for which results will be exported.')


def do_command(args):
    """
    Execute export analysis command
    """
    # Build and send request
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error().accept("application/zip")
    response = request.get().resource(do_ws(args)).send()
    print(response.content)