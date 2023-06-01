#
# Opal commands main entry point
#
import argparse
import sys
import getpass

from obiba_opal.core import Formatter, HTTPError
from obiba_opal.project import ProjectService, BackupProjectCommand, RestoreProjectCommand
from obiba_opal.table import CopyTableCommand, DeleteTableService, BackupViewService, RestoreViewService
from obiba_opal.dictionary import DictionaryService, ExportAnnotationsService, ImportAnnotationsService
from obiba_opal.data import DataService, EntityService
from obiba_opal.analysis import AnalysisCommand, ExportAnalysisService
from obiba_opal.file import FileService
from obiba_opal.exports import ExportPluginCommand, ExportCSVCommand, ExportXMLCommand, ExportRSASCommand, ExportRSPSSCommand, ExportRSTATACommand, ExportRDSCommand, ExportSQLCommand, ExportVCFCommand
from obiba_opal.subjects import UserService, GroupService
from obiba_opal.perm import ProjectPermService, DatasourcePermService, TablePermService, VariablePermService, ResourcePermService, ResourcesPermService, RPermService, DataSHIELDPermService, SystemPermService
from obiba_opal.imports import ImportPluginCommand, ImportCSVCommand, ImportIDMapService, ImportIDService, ImportLimeSurveyCommand, ImportOpalCommand, ImportRDSCommand, ImportRSASCommand, ImportRSPSSCommand, ImportRSTATACommand, ImportSQLCommand, ImportVCFCommand, ImportXMLCommand
from obiba_opal.system import PluginService, SystemService, TaxonomyService, TaskService, RESTService
from obiba_opal.sql import SQLService, SQLHistoryService
from obiba_opal.security import EncryptService, DecryptService

def prompt_password():
    return getpass.getpass(prompt='Enter password: ')

def add_opal_arguments(parser):
    """
    Add Opal access arguments
    """
    parser.add_argument('--opal', '-o', required=False, default='http://localhost:8080',
                        help='Opal server base url (default: http://localhost:8080)')
    parser.add_argument('--user', '-u', required=False, help='Credentials auth: user name (requires a password)')
    parser.add_argument('--password', '-p', required=False, nargs="?",
                        help='Credentials auth: user password (requires a user name)')
    parser.add_argument('--token', '-tk', required=False, help='Token auth: User access token')
    parser.add_argument('--ssl-cert', '-sc', required=False,
                        help='Two-way SSL auth: certificate/public key file (requires a private key)')
    parser.add_argument('--ssl-key', '-sk', required=False,
                        help='Two-way SSL auth: private key file (requires a certificate)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--no-ssl-verify', '-nv', action='store_true', help='Do not verify SSL certificates for HTTPS.')

def add_subcommand(subparsers, name, help, add_args_func, default_func):
    """
    Make a sub-parser, add default arguments to it, add sub-command arguments and set the sub-command callback function.
    """
    subparser = subparsers.add_parser(name, help=help)
    add_opal_arguments(subparser)
    add_args_func(subparser)
    subparser.set_defaults(func=default_func)

def run():
    """
    Command-line entry point.
    """
    # Parse arguments
    parser = argparse.ArgumentParser(description='Opal command line tool.')
    subparsers = parser.add_subparsers(title='sub-commands',
                                      help='Available sub-commands. Use --help option on the sub-command '
                                            'for more details.')

    # Add subcommands
    add_subcommand(subparsers, 'project', 'Fetch, create, delete a project.', ProjectService.add_arguments, ProjectService.do_command)
    add_subcommand(subparsers, 'dict', 'Query for data dictionary.', DictionaryService.add_arguments, DictionaryService.do_command)
    add_subcommand(subparsers, 'data', 'Query for data.', DataService.add_arguments, DataService.do_command)
    add_subcommand(subparsers, 'entity', 'Query for entities (Participant, etc.).', EntityService.add_arguments, EntityService.do_command)
    add_subcommand(subparsers, 'file', 'Manage Opal file system.', FileService.add_arguments, FileService.do_command)
    add_subcommand(subparsers, 'taxonomy', 'Manage taxonomies: list available taxonomies, download, import or delete a taxonomy.', TaxonomyService.add_arguments, TaxonomyService.do_command)
    add_subcommand(subparsers, 'backup-project',
                  'Backup project data: tables (data export), views, resources, report templates, files.',
                  BackupProjectCommand.add_arguments, BackupProjectCommand.do_command)
    add_subcommand(subparsers, 'restore-project',
                  'Restore project data: tables (data import), views, resources, report templates, files.',
                  RestoreProjectCommand.add_arguments, RestoreProjectCommand.do_command)
    add_subcommand(subparsers, 'backup-view', 'Backup views of a project.', BackupViewService.add_arguments, BackupViewService.do_command)
    add_subcommand(subparsers, 'restore-view', 'Restore views of a project.', RestoreViewService.add_arguments,
                  RestoreViewService.do_command)
    add_subcommand(subparsers, 'import-opal', 'Import data from a remote Opal server.', ImportOpalCommand.add_arguments,
                  ImportOpalCommand.do_command)
    add_subcommand(subparsers, 'import-csv', 'Import data from a CSV file.', ImportCSVCommand.add_arguments,
                  ImportCSVCommand.do_command)
    add_subcommand(subparsers, 'import-xml', 'Import data from a ZIP file.', ImportXMLCommand.add_arguments,
                  ImportXMLCommand.do_command)
    add_subcommand(subparsers, 'import-r-sas', 'Import data from a SAS or SAS Transport file (using R).',
                  ImportRSASCommand.add_arguments,
                  ImportRSASCommand.do_command)
    add_subcommand(subparsers, 'import-r-stata', 'Import data from a Stata file (using R).', ImportRSTATACommand.add_arguments,
                  ImportRSTATACommand.do_command)
    add_subcommand(subparsers, 'import-r-spss', 'Import data from a SPSS or compressed SPSS file (using R).',
                  ImportRSPSSCommand.add_arguments,
                  ImportRSPSSCommand.do_command)
    add_subcommand(subparsers, 'import-r-rds', 'Import data from a RDS file (single serialized R object, expected to be a tibble, using R).', ImportRDSCommand.add_arguments,
                  ImportRDSCommand.do_command)
    add_subcommand(subparsers, 'import-plugin', 'Import data from an Opal datasource plugin.', ImportPluginCommand.add_arguments,
                  ImportPluginCommand.do_command)
    add_subcommand(subparsers, 'import-limesurvey', 'Import data from a LimeSurvey database.', ImportLimeSurveyCommand.add_arguments,
                  ImportLimeSurveyCommand.do_command)
    add_subcommand(subparsers, 'import-sql', 'Import data from a SQL database.', ImportSQLCommand.add_arguments,
                  ImportSQLCommand.do_command)
    add_subcommand(subparsers, 'import-vcf', 'Import genotypes data from some VCF/BCF files.', ImportVCFCommand.add_arguments,
                  ImportVCFCommand.do_command)
    add_subcommand(subparsers, 'import-ids', 'Import system identifiers.', ImportIDService.add_arguments,
                  ImportIDService.do_command)
    add_subcommand(subparsers, 'import-ids-map', 'Import identifiers mappings.', ImportIDMapService.add_arguments,
                  ImportIDMapService.do_command)
    add_subcommand(subparsers, 'import-annot',
                  'Apply data dictionary annotations specified in a file in CSV/TSV format (see export-annot).',
                  ImportAnnotationsService.add_arguments, ImportAnnotationsService.do_command)
    add_subcommand(subparsers, 'export-xml', 'Export data to a zip of Opal XML files.', ExportXMLCommand.add_arguments,
                  ExportXMLCommand.do_command)
    add_subcommand(subparsers, 'export-csv', 'Export data to a folder of CSV files.', ExportCSVCommand.add_arguments,
                  ExportCSVCommand.do_command)
    add_subcommand(subparsers, 'export-r-sas', 'Export data to a SAS or SAS Transport file (using R).', ExportRSASCommand.add_arguments,
                  ExportRSASCommand.do_command)
    add_subcommand(subparsers, 'export-r-stata', 'Export data to a Stata file (using R).', ExportRSTATACommand.add_arguments,
                  ExportRSTATACommand.do_command)
    add_subcommand(subparsers, 'export-r-spss', 'Export data to a SPSS or compressed SPSS file (using R).',
                  ExportRSPSSCommand.add_arguments,
                  ExportRSPSSCommand.do_command)
    add_subcommand(subparsers, 'export-r-rds', 'Export data to a RDS file (single serialized R object, using R).', ExportRDSCommand.add_arguments,
                  ExportRDSCommand.do_command)
    add_subcommand(subparsers, 'export-sql', 'Export data to a SQL database.', ExportSQLCommand.add_arguments,
                  ExportSQLCommand.do_command)
    add_subcommand(subparsers, 'export-plugin', 'Export data to a Opal datasource plugin.', ExportPluginCommand.add_arguments,
                  ExportPluginCommand.do_command)
    add_subcommand(subparsers, 'export-vcf', 'Export genotypes data to VCF/BCF files.', ExportVCFCommand.add_arguments,
                  ExportVCFCommand.do_command)
    add_subcommand(subparsers, 'export-annot', 'Extract data dictionary annotations in CSV/TSV format.',
                  ExportAnnotationsService.add_arguments, ExportAnnotationsService.do_command)
    add_subcommand(subparsers, 'copy-table', 'Copy a table into another table.', CopyTableCommand.add_arguments,
                  CopyTableCommand.do_command)
    add_subcommand(subparsers, 'delete-table', 'Delete some tables.', DeleteTableService.add_arguments, DeleteTableService.do_command)
    add_subcommand(subparsers, 'user', 'Manage users.', UserService.add_arguments, UserService.do_command)
    add_subcommand(subparsers, 'group', 'Manage groups.', GroupService.add_arguments, GroupService.do_command)
    add_subcommand(subparsers, 'perm-project', 'Get or apply permission on a project.', ProjectPermService.add_arguments,
                  ProjectPermService.do_command)
    add_subcommand(subparsers, 'perm-datasource', 'Get or apply permission on a datasource.', DatasourcePermService.add_arguments,
                  DatasourcePermService.do_command)
    add_subcommand(subparsers, 'perm-table', 'Get or apply permission on a set of tables.', TablePermService.add_arguments,
                  TablePermService.do_command)
    add_subcommand(subparsers, 'perm-variable', 'Get or apply permission on a set of variables.', VariablePermService.add_arguments,
                  VariablePermService.do_command)
    add_subcommand(subparsers, 'perm-resources', 'Get or apply permission on resources as a whole.', ResourcesPermService.add_arguments,
                  ResourcesPermService.do_command)
    add_subcommand(subparsers, 'perm-resource', 'Get or apply permission on a set of resources.', ResourcePermService.add_arguments,
                  ResourcePermService.do_command)
    add_subcommand(subparsers, 'perm-r', 'Get or apply R permission.', RPermService.add_arguments, RPermService.do_command)
    add_subcommand(subparsers, 'perm-datashield', 'Get or apply DataSHIELD permission.', DataSHIELDPermService.add_arguments,
                  DataSHIELDPermService.do_command)
    add_subcommand(subparsers, 'perm-system', 'Get or apply system permission.', SystemPermService.add_arguments, SystemPermService.do_command)
    add_subcommand(subparsers, 'plugin', 'Manage system plugins.', PluginService.add_arguments,
                  PluginService.do_command)
    add_subcommand(subparsers, 'encrypt', "Encrypt string using Opal's secret key.", EncryptService.add_arguments,
                  EncryptService.do_command)
    add_subcommand(subparsers, 'decrypt', "Decrypt string using Opal's secret key.", DecryptService.add_arguments,
                  DecryptService.do_command)
    add_subcommand(subparsers, 'task', 'Manage a task.', TaskService.add_arguments, TaskService.do_command)
    add_subcommand(subparsers, 'system', 'Query for system status and configuration.', SystemService.add_arguments,
                  SystemService.do_command)
    add_subcommand(subparsers, 'rest', 'Request directly the Opal REST API, for advanced users.', RESTService.add_arguments,
                  RESTService.do_command)
    add_subcommand(subparsers, 'analysis-plugin', 'Analyses a project variables using external R plugins.',
                  AnalysisCommand.add_arguments,
                  AnalysisCommand.do_command)
    add_subcommand(subparsers, 'export-analysis-plugin', 'Exports analysis data of a project or specific tables.',
                  ExportAnalysisService.add_arguments,
                  ExportAnalysisService.do_command)
    add_subcommand(subparsers, 'sql', 'Execute a SQL statement on project\'s tables.',
                  SQLService.add_arguments,
                  SQLService.do_command)
    add_subcommand(subparsers, 'sql-history', 'SQL execution history of current user or of other users (administrator only).',
                  SQLHistoryService.add_arguments,
                  SQLHistoryService.do_command)

    # Execute selected command
    args = parser.parse_args()


    if hasattr(args, 'func'):
        try:
          # Prompt for a missing password only when user/password is required
          if not (args.ssl_cert or args.ssl_key) and not args.token:
            if not args.password or len(args.password) == 0:
              args.password = prompt_password()
          args.func(args)
        except HTTPError as e:
            Formatter.print_json(e.error, args.json if hasattr(args, 'json') else False)
            sys.exit(2)
        except Exception as e:
            if hasattr(e, 'message'):
                print(e.message)
            else:
                print(e)
            sys.exit(2)
    else:
        print('Opal command line tool.')
        print('For more details: opal --help')
