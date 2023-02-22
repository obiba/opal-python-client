"""
Opal Export analysis plugin.
"""

import obiba_opal.core as core
import os
import sys

class ExportAnalysisService:
    """
    Export project tables analyses in a zip file.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
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

    @classmethod
    def do_command(self, args):
        """
        Execute export analysis command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            fd = sys.stdout.fileno()
            if args.table is None:    
                ExportAnalysisService(client, args.verbose).export_project_analyses(args.project, fd, args.all_results)
            elif args.analysis_id is None:
                ExportAnalysisService(client, args.verbose).export_table_analyses(args.project, args.table, fd, args.all_results)
            else:
                ExportAnalysisService(client, args.verbose).export_table_analysis(args.project, args.table, args.analysis_id, fd, args.all_results)
        finally:
            client.close()

    def export_project_analyses(self, project: str, fd, all_results: bool = True):
        """
        Export project's analyses for all tables in a zip file.

        :param project: The project name
        :param table: The table name
        :param fd: Destination file descriptor (see os.fdopen())
        """
        request = self.client.new_request()
        request.fail_on_error().accept("application/zip")
        fp = os.fdopen(fd, 'wb')
        request.get().resource(self._make_ws(project, all_results=all_results)).send(fp)
        fp.flush()

    def export_table_analyses(self, project: str, table: str, fd, all_results: bool = True):
        """
        Export project's analyses for a specific table in a zip file.

        :param project: The project name
        :param table: The table name
        :param fd: Destination file descriptor (see os.fdopen())
        """
        request = self.client.new_request()
        request.fail_on_error().accept("application/zip")
        fp = os.fdopen(fd, 'wb')
        request.get().resource(self._make_ws(project, table, all_results=all_results)).send()
        fp.flush()

    def export_table_analysis(self, project: str, table: str, analysis_id: str, fd, all_results: bool = True):
        """
        Export project's analysis for a specific table and analyis in a zip file.

        :param project: The project name
        :param table: The table name
        :param analysis_id: The analysis identifier
        :param fd: Destination file descriptor (see os.fdopen())
        """
        request = self.client.new_request()
        request.fail_on_error().accept('application/zip')
        fp = os.fdopen(fd, 'wb')
        request.get().resource(self._make_ws(project, table, analysis_id, all_results)).send()
        fp.flush()

    def _make_ws(self, project: str, table: str = None, analysis_id: str = None, all_results: bool = True):
        """
        Build the web service resource path
        """
        if table is None:
            ws = '/project/%s/analyses/_export' % project
        elif analysis_id is None:
            ws = '/project/%s/table/%s/analyses/_export' % (project, table)
        else:
            ws = '/project/%s/table/%s/analysis/%s/_export' % (project, table, analysis_id)

        return '%s?all=true' % ws if all_results else ws
