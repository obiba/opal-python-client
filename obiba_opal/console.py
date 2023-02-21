#
# Opal commands main entry point
#
import argparse
import sys

import obiba_opal.rest as rest
import obiba_opal.dictionary as dictionary
import obiba_opal.data as data
import obiba_opal.file as file
import obiba_opal.entity as entity
import obiba_opal.import_opal as import_opal
import obiba_opal.import_csv as import_csv
import obiba_opal.import_xml as import_xml
import obiba_opal.import_rsas as import_rsas
import obiba_opal.import_rstata as import_rstata
import obiba_opal.import_rspss as import_rspss
import obiba_opal.import_rds as import_rds
import obiba_opal.import_plugin as import_plugin
import obiba_opal.import_limesurvey as import_limesurvey
import obiba_opal.import_sql as import_sql
import obiba_opal.import_vcf as import_vcf
import obiba_opal.import_ids as import_ids
import obiba_opal.import_idsmap as import_idsmap
import obiba_opal.import_annotations as import_annotations
import obiba_opal.export_xml as export_xml
import obiba_opal.export_csv as export_csv
import obiba_opal.export_plugin as export_plugin
import obiba_opal.export_rsas as export_rsas
import obiba_opal.export_rspss as export_rspss
import obiba_opal.export_rstata as export_rstata
import obiba_opal.export_rds as export_rds
import obiba_opal.export_sql as export_sql
import obiba_opal.export_vcf as export_vcf
import obiba_opal.export_annotations as export_annotations
import obiba_opal.copy_table as copy_table
import obiba_opal.delete_table as delete_table
import obiba_opal.task as task
import obiba_opal.user as user
import obiba_opal.group as group
import obiba_opal.perm_project as perm_project
import obiba_opal.perm_datasource as perm_datasource
import obiba_opal.perm_table as perm_table
import obiba_opal.perm_variable as perm_variable
import obiba_opal.perm_resource as perm_resource
import obiba_opal.perm_resources as perm_resources
import obiba_opal.perm_r as perm_r
import obiba_opal.perm_datashield as perm_datashield
import obiba_opal.perm_system as perm_system
import obiba_opal.project as project
import obiba_opal.plugin as plugin
import obiba_opal.security.encrypt as encrypt
import obiba_opal.security.decrypt as decrypt
import obiba_opal.system as system
import obiba_opal.analysis_plugin as analysis_plugin
import obiba_opal.export_analysis_plugin as export_analysis_plugin
import obiba_opal.backup_view as backup_view
import obiba_opal.restore_view as restore_view
import obiba_opal.backup_project as backup_project
import obiba_opal.restore_project as restore_project
import obiba_opal.taxonomy as taxonomy
import obiba_opal.sql as sql
import obiba_opal.sql_history as sql_history


def add_opal_arguments(parser):
    """
    Add Opal access arguments
    """
    parser.add_argument('--opal', '-o', required=False, default='http://localhost:8080',
                        help='Opal server base url (default: http://localhost:8080)')
    parser.add_argument('--user', '-u', required=False, help='Credentials auth: user name (requires a password)')
    parser.add_argument('--password', '-p', required=False,
                        help='Credentials auth: user password (requires a user name)')
    parser.add_argument('--token', '-tk', required=False, help='Token auth: User access token')
    parser.add_argument('--ssl-cert', '-sc', required=False,
                        help='Two-way SSL auth: certificate/public key file (requires a private key)')
    parser.add_argument('--ssl-key', '-sk', required=False,
                        help='Two-way SSL auth: private key file (requires a certificate)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')


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
    add_subcommand(subparsers, 'project', 'Fetch, create, delete a project.', project.add_arguments, project.do_command)
    add_subcommand(subparsers, 'dict', 'Query for data dictionary.', dictionary.add_arguments, dictionary.do_command)
    add_subcommand(subparsers, 'data', 'Query for data.', data.add_arguments, data.do_command)
    add_subcommand(subparsers, 'entity', 'Query for entities (Participant, etc.).', entity.add_arguments, entity.do_command)
    add_subcommand(subparsers, 'file', 'Manage Opal file system.', file.add_arguments, file.do_command)
    add_subcommand(subparsers, 'taxonomy', 'Manage taxonomies: list available taxonomies, download, import or delete a taxonomy.', taxonomy.add_arguments, taxonomy.do_command)
    add_subcommand(subparsers, 'backup-project',
                  'Backup project data: tables (data export), views, resources, report templates, files.',
                  backup_project.add_arguments, backup_project.do_command)
    add_subcommand(subparsers, 'restore-project',
                  'Restore project data: tables (data import), views, resources, report templates, files.',
                  restore_project.add_arguments, restore_project.do_command)
    add_subcommand(subparsers, 'backup-view', 'Backup views of a project.', backup_view.add_arguments, backup_view.do_command)
    add_subcommand(subparsers, 'restore-view', 'Restore views of a project.', restore_view.add_arguments,
                  restore_view.do_command)
    add_subcommand(subparsers, 'import-opal', 'Import data from a remote Opal server.', import_opal.add_arguments,
                  import_opal.do_command)
    add_subcommand(subparsers, 'import-csv', 'Import data from a CSV file.', import_csv.add_arguments,
                  import_csv.do_command)
    add_subcommand(subparsers, 'import-xml', 'Import data from a ZIP file.', import_xml.add_arguments,
                  import_xml.do_command)
    add_subcommand(subparsers, 'import-r-sas', 'Import data from a SAS or SAS Transport file (using R).',
                  import_rsas.add_arguments,
                  import_rsas.do_command)
    add_subcommand(subparsers, 'import-r-stata', 'Import data from a Stata file (using R).', import_rstata.add_arguments,
                  import_rstata.do_command)
    add_subcommand(subparsers, 'import-r-spss', 'Import data from a SPSS or compressed SPSS file (using R).',
                  import_rspss.add_arguments,
                  import_rspss.do_command)
    add_subcommand(subparsers, 'import-r-rds', 'Import data from a RDS file (single serialized R object, expected to be a tibble, using R).', import_rds.add_arguments,
                  import_rds.do_command)
    add_subcommand(subparsers, 'import-plugin', 'Import data from an Opal datasource plugin.', import_plugin.add_arguments,
                  import_plugin.do_command)
    add_subcommand(subparsers, 'import-limesurvey', 'Import data from a LimeSurvey database.', import_limesurvey.add_arguments,
                  import_limesurvey.do_command)
    add_subcommand(subparsers, 'import-sql', 'Import data from a SQL database.', import_sql.add_arguments,
                  import_sql.do_command)
    add_subcommand(subparsers, 'import-vcf', 'Import genotypes data from some VCF/BCF files.', import_vcf.add_arguments,
                  import_vcf.do_command)
    add_subcommand(subparsers, 'import-ids', 'Import system identifiers.', import_ids.add_arguments,
                  import_ids.do_command)
    add_subcommand(subparsers, 'import-ids-map', 'Import identifiers mappings.', import_idsmap.add_arguments,
                  import_idsmap.do_command)
    add_subcommand(subparsers, 'import-annot',
                  'Apply data dictionary annotations specified in a file in CSV/TSV format (see export-annot).',
                  import_annotations.add_arguments, import_annotations.do_command)
    add_subcommand(subparsers, 'export-xml', 'Export data to a zip of Opal XML files.', export_xml.add_arguments,
                  export_xml.do_command)
    add_subcommand(subparsers, 'export-csv', 'Export data to a folder of CSV files.', export_csv.add_arguments,
                  export_csv.do_command)
    add_subcommand(subparsers, 'export-r-sas', 'Export data to a SAS or SAS Transport file (using R).', export_rsas.add_arguments,
                  export_rsas.do_command)
    add_subcommand(subparsers, 'export-r-stata', 'Export data to a Stata file (using R).', export_rstata.add_arguments,
                  export_rstata.do_command)
    add_subcommand(subparsers, 'export-r-spss', 'Export data to a SPSS or compressed SPSS file (using R).',
                  export_rspss.add_arguments,
                  export_rspss.do_command)
    add_subcommand(subparsers, 'export-r-rds', 'Export data to a RDS file (single serialized R object, using R).', export_rds.add_arguments,
                  export_rds.do_command)
    add_subcommand(subparsers, 'export-sql', 'Export data to a SQL database.', export_sql.add_arguments,
                  export_sql.do_command)
    add_subcommand(subparsers, 'export-plugin', 'Export data to a Opal datasource plugin.', export_plugin.add_arguments,
                  export_plugin.do_command)
    add_subcommand(subparsers, 'export-vcf', 'Export genotypes data to VCF/BCF files.', export_vcf.add_arguments,
                  export_vcf.do_command)
    add_subcommand(subparsers, 'export-annot', 'Extract data dictionary annotations in CSV/TSV format.',
                  export_annotations.add_arguments, export_annotations.do_command)
    add_subcommand(subparsers, 'copy-table', 'Copy a table into another table.', copy_table.add_arguments,
                  copy_table.do_command)
    add_subcommand(subparsers, 'delete-table', 'Delete some tables.', delete_table.add_arguments, delete_table.do_command)
    add_subcommand(subparsers, 'user', 'Manage users.', user.add_arguments, user.do_command)
    add_subcommand(subparsers, 'group', 'Manage groups.', group.add_arguments, group.do_command)
    add_subcommand(subparsers, 'perm-project', 'Apply permission on a project.', perm_project.add_arguments,
                  perm_project.do_command)
    add_subcommand(subparsers, 'perm-datasource', 'Apply permission on a datasource.', perm_datasource.add_arguments,
                  perm_datasource.do_command)
    add_subcommand(subparsers, 'perm-table', 'Apply permission on a set of tables.', perm_table.add_arguments,
                  perm_table.do_command)
    add_subcommand(subparsers, 'perm-variable', 'Apply permission on a set of variables.', perm_variable.add_arguments,
                  perm_variable.do_command)
    add_subcommand(subparsers, 'perm-resources', 'Apply permission on resources as a whole.', perm_resources.add_arguments,
                  perm_resources.do_command)
    add_subcommand(subparsers, 'perm-resource', 'Apply permission on a set of resources.', perm_resource.add_arguments,
                  perm_resource.do_command)
    add_subcommand(subparsers, 'perm-r', 'Apply R permission.', perm_r.add_arguments, perm_r.do_command)
    add_subcommand(subparsers, 'perm-datashield', 'Apply DataSHIELD permission.', perm_datashield.add_arguments,
                  perm_datashield.do_command)
    add_subcommand(subparsers, 'perm-system', 'Apply system permission.', perm_system.add_arguments, perm_system.do_command)
    add_subcommand(subparsers, 'plugin', 'Manage system plugins.', plugin.add_arguments,
                  plugin.do_command)
    add_subcommand(subparsers, 'encrypt', "Encrypt string using Opal's secret key.", encrypt.add_arguments,
                  encrypt.do_command)
    add_subcommand(subparsers, 'decrypt', "Decrypt string using Opal's secret key.", decrypt.add_arguments,
                  decrypt.do_command)
    add_subcommand(subparsers, 'task', 'Manage a task.', task.add_arguments, task.do_command)
    add_subcommand(subparsers, 'system', 'Query for system status and configuration.', system.add_arguments,
                  system.do_command)
    add_subcommand(subparsers, 'rest', 'Request directly the Opal REST API, for advanced users.', rest.add_arguments,
                  rest.do_command)
    add_subcommand(subparsers, 'analysis-plugin', 'Analyses a project variables using external R plugins.',
                  analysis_plugin.add_arguments,
                  analysis_plugin.do_command)
    add_subcommand(subparsers, 'export-analysis-plugin', 'Exports analysis data of a project or specific tables.',
                  export_analysis_plugin.add_arguments,
                  export_analysis_plugin.do_command)
    add_subcommand(subparsers, 'sql', 'Execute a SQL statement on project\'s tables.',
                  sql.add_arguments,
                  sql.do_command)
    add_subcommand(subparsers, 'sql-history', 'SQL execution history of current user or of other users (administrator only).',
                  sql_history.add_arguments,
                  sql_history.do_command)

    # Execute selected command
    args = parser.parse_args()
    if hasattr(args, 'func'):
      try:
        args.func(args)
      except Exception as e:
          print(e)
          sys.exit(2)
    else:
      print('Opal command line tool.')
      print('For more details: opal --help')
