"""
Table/view copy and management.
"""

import obiba_opal.core as core
import obiba_opal.io as io
from obiba_opal.dictionary import DictionaryService
import os
import zipfile


class CopyTableCommand:
    """
    Execute a copy table command.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--project', '-pr', required=True, help='Source project name')
        parser.add_argument('--tables', '-t', nargs='+', required=False,
                            help='List of table names to be copied (default is all)')
        parser.add_argument('--destination', '-d', required=True, help='Destination project name')
        parser.add_argument('--name', '-na', required=False,
                            help='New table name (required if source and destination are the same, ignored if more than one table is to be copied)')
        parser.add_argument('--incremental', '-i', action='store_true', help='Incremental copy')
        parser.add_argument('--nulls', '-nu', action='store_true', help='Copy the null values')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute copy data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = CopyTableCommand(client, args.verbose).copy_tables(args.project, args.tables, args.destination, args.name, args.incremental, args.nulls)
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def copy_tables(self, project: str, tables: list, destination: str, name: str, incremental: bool, nulls: bool) -> dict:
        """
        Execute copy data command

        :param project: The project name
        :param tables: List of table names to be copied (default is all)
        :param destination: Destination project name
        :param name: New table name (required if source and destination are the same, ignored if more than one table is to be copied)
        :param incremental: Incremental copy
        :param nulls: Copy the null values
        """
        tables_ = tables
        if not tables:
            tables_ = self._retrieve_datasource_tables(project)
        copier = io.OpalCopier.build(client=self.client, datasource=project, tables=tables_,
                                    destination=destination, name=name,
                                    incremental=incremental, nulls=nulls,
                                    verbose=self.verbose)
        response = copier.submit()
        return response.from_json()
    
    def _retrieve_datasource_tables(self, project: str) -> list:
        request = self.client.new_request()
        if self.verbose:
            request.verbose()
        response = request.fail_on_error().get().resource(
            core.UriBuilder(['datasource', project, 'tables']).build()).send().from_json()

        tables = []
        for table in response:
            tables.append(str(table['name']))

        return tables

class DeleteTableService:
    """
    Delete some tables in a project.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        parser.add_argument('--project', '-pr', required=True, help='Project name to which the tables belong')
        parser.add_argument('--tables', '-t', nargs='+', required=False,
                            help='List of table names which will be deleted (default is all)')

    @classmethod
    def do_command(cls, args):
        """
        Execute delete command
        """
        # Build and send requests
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            DictionaryService(client, args.verbose).delete_tables(args.project, args.tables)
        finally:
            client.close()


class BackupViewService:
    """
    Backup views of a project: download view's JSON representation and save it in a file, one for each view, for later restore.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        parser.add_argument('--project', '-pr', required=True, help='Source project name')
        parser.add_argument('--views', '-vw', nargs='+', required=False,
                            help='List of view names to be backed up (default is all)')
        parser.add_argument('--output', '-out', required=False, help='Output directory name (default is current directory)')
        parser.add_argument('--force', '-f', action='store_true',
                            help='Skip confirmation when overwriting the backup file.')

    @classmethod
    def do_command(cls, args):
        """
        Retrieve table DTOs of the project, look for the views, download the views in JSON into a file in provided or current directory
        """

        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            BackupViewService(client, args.verbose).backup_views(args.project, args.views, args.output, args.force)
        finally:
            client.close()
    
    def backup_view(self, project: str, view, outdir, force: bool):
        outfile = view + '.json'
        print('Backup of', view, 'in', outfile, '...')

        outpath = os.path.join(outdir, outfile)

        request = self.client.new_request()
        request.fail_on_error()
        if self.verbose:
            request.verbose()
        response = request.get().resource(core.UriBuilder(['datasource', project, 'view', view]).build()).send()

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

    def backup_views(self, project: str, views: list, output: str, force: bool) -> list:
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
        obsviews = self._retrieve_datasource_views(project)
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
            if self.verbose:
                print('Output directory is', outdir)
            if not os.path.exists(outdir):
                if self.verbose:
                    print('Creating output directory ...')
                os.makedirs(outdir)

            # backup each view
            for view in views_:
                self.backup_view(project, view, outdir, force)
        return views_
    
    def _retrieve_datasource_views(self, project: str) -> list:
        request = self.client.new_request()
        request.fail_on_error()
        if self.verbose:
            request.verbose()
        response = request.get().resource(core.UriBuilder(['datasource', project, 'tables']).build()).send().from_json()

        views = []
        for table in response:
            if 'viewLink' in table:
                views.append(str(table['name']))

        return views


class RestoreViewService:
    """
    Restore views of a project: upload view's JSON representation and make it a view.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
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

    @classmethod
    def do_command(cls, args):
        """
        Retrieve table DTOs of the project, look for the views, download the views in JSON into a file in provided or current directory
        """

        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        service = RestoreViewService(client, args.verbose)
        service.restore_views(args.project, args.views, args.input, args.force)

    def restore_views(self, project: str, views: list, input: str = None, force: bool = False):
        obsviews = self._retrieve_datasource_views(project)

        # list input directory content
        indir = input
        if not indir:
            indir = os.getcwd()
        else:
            indir = os.path.normpath(indir)
        print('Input directory is', indir)

        if indir.endswith('.zip'):
            with zipfile.ZipFile(indir, 'r') as inzip:
                for viewfile in [filename for filename in inzip.namelist() if
                                    filename.endswith('.json') and (not views or filename[:-5] in views)]:
                    self._restore_zipped_view(project, obsviews, viewfile, inzip, force)
        else:
            for viewfile in self._list_json_files(indir, views):
                self._restore_view(project, obsviews, viewfile, force)

    def _retrieve_datasource_views(self, project: str):
        request = self._make_request()
        response = request.get().resource(core.UriBuilder(['datasource', project, 'tables']).build()).send().from_json()

        views = []
        for table in response:
            if 'viewLink' in table:
                views.append(str(table['name']))

        return views

    def _restore_view(self, project: str, obsviews: list, infile: str, force: bool = False):
        view = os.path.basename(infile[:-5])  # supposed to be a .json file path

        dowrite = True
        if view in obsviews and not force:
            dowrite = False
            confirmed = input('Overwrite the view "' + view + '"? [y/N]: ')
            if confirmed == 'y':
                dowrite = True

        if dowrite:
            print('Restore of', view, 'from', infile, '...')

            request =  self._make_request()
            with open(infile, 'r') as inf:
                request.content(inf.read())
            request.content_type_json()

            if view in obsviews:
                request.put().resource(
                    core.UriBuilder(['datasource', project, 'view', view]).query('comment', 'restore-view').build()).send()
            else:
                request.post().resource(
                    core.UriBuilder(['datasource', project, 'views']).query('comment', 'restore-view').build()).send()

    def _restore_zipped_view(self, project: str, obsviews: list, infile: str, zippedinput, force: bool = False):
        view = infile[:-5]  # supposed to be a .json file name

        dowrite = True
        if view in obsviews and not force:
            dowrite = False
            confirmed = input('Overwrite the view "' + view + '"? [y/N]: ')
            if confirmed == 'y':
                dowrite = True

        if dowrite:
            print('Restore of', view, 'from', infile, '...')

            request = self._make_request()
            request.content(zippedinput.read(infile))
            request.content_type_json()

            if view in obsviews:
                request.put().resource(
                    core.UriBuilder(['datasource', project, 'view', view]).query('comment',
                                                                                        'restore-view').build()).send()
            else:
                request.post().resource(
                    core.UriBuilder(['datasource', project, 'views']).query('comment',
                                                                                    'restore-view').build()).send()

    def _list_json_files(self, dirref: str, basenames):
        matches = []
        for root, dirnames, filenames in os.walk(dirref):
            for filename in filenames:
                if filename.endswith('.json'):
                    if not basenames or filename[:-5] in basenames:
                        matches.append(os.path.join(root, filename))
        return matches

    def _make_request(self, fail_safe: bool = False):
        request = self.client.new_request()
        if not fail_safe:
            request.fail_on_error()
        if self.verbose:
            request.verbose()
        return request