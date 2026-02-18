#
# Opal commands native Typer implementation
#
from __future__ import annotations

from types import SimpleNamespace

import sys
import typer

from obiba_opal.project import (
    ProjectService,
    BackupProjectCommand,
    RestoreProjectCommand,
)
from obiba_opal.table import (
    CopyTableCommand,
    DeleteTableService,
    BackupViewService,
    RestoreViewService,
)
from obiba_opal.dictionary import (
    DictionaryService,
    ExportAnnotationsService,
    ImportAnnotationsService,
)
from obiba_opal.data import DataService, EntityService
from obiba_opal.analysis import AnalysisCommand, ExportAnalysisService
from obiba_opal.file import FileService
from obiba_opal.exports import (
    ExportPluginCommand,
    ExportCSVCommand,
    ExportXMLCommand,
    ExportRSASCommand,
    ExportRSPSSCommand,
    ExportRSTATACommand,
    ExportRDSCommand,
    ExportSQLCommand,
    ExportVCFCommand,
)
from obiba_opal.subjects import UserService, GroupService
from obiba_opal.perm import (
    ProjectPermService,
    DatasourcePermService,
    TablePermService,
    VariablePermService,
    ResourcePermService,
    ResourcesPermService,
    RPermService,
    DataSHIELDPermService,
    SystemPermService,
)
from obiba_opal.imports import (
    ImportPluginCommand,
    ImportCSVCommand,
    ImportIDMapService,
    ImportIDService,
    ImportLimeSurveyCommand,
    ImportOpalCommand,
    ImportRDSCommand,
    ImportRSASCommand,
    ImportRSPSSCommand,
    ImportRSTATACommand,
    ImportSQLCommand,
    ImportVCFCommand,
    ImportXMLCommand,
)
from obiba_opal.system import (
    PluginService,
    SystemService,
    TaxonomyService,
    TaskService,
    RESTService,
)
from obiba_opal.sql import SQLService, SQLHistoryService
from obiba_opal.security import EncryptService, DecryptService


def _make_args_with_globals(
    ctx: typer.Context,
    opal: str | None,
    user: str | None,
    password: str | None,
    token: str | None,
    ssl_cert: str | None,
    ssl_key: str | None,
    verbose: bool,
    no_ssl_verify: bool,
    **kwargs,
) -> SimpleNamespace:
    """Create args namespace with global options from either callback or direct args."""
    ctx_obj = ctx.obj if ctx.obj else {}

    args_dict = {
        "opal": opal if opal is not None else ctx_obj.get("opal", "http://localhost:8080"),
        "user": user if user is not None else ctx_obj.get("user"),
        "password": password if password is not None else ctx_obj.get("password"),
        "token": token if token is not None else ctx_obj.get("token"),
        "ssl_cert": ssl_cert if ssl_cert is not None else ctx_obj.get("ssl_cert"),
        "ssl_key": ssl_key if ssl_key is not None else ctx_obj.get("ssl_key"),
        "verbose": verbose if verbose else ctx_obj.get("verbose", False),
        "no_ssl_verify": no_ssl_verify if no_ssl_verify else ctx_obj.get("no_ssl_verify", False),
    }
    args_dict.update(kwargs)
    return SimpleNamespace(**args_dict)


# =============================================================================
# Project Commands
# =============================================================================


def project_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    name: str | None = typer.Option(
        None, "--name", "-n", help="Project name. Not specifying the project name, will get the list of the projects."
    ),
    database: str | None = typer.Option(
        None, "--database", "-db", help="Project database name. If not provided only views can be added."
    ),
    title: str | None = typer.Option(None, "--title", "-t", help="Project title."),
    description: str | None = typer.Option(None, "--description", "-dc", help="Project description."),
    tags: list[str] | None = typer.Option(None, "--tags", "-tg", help="Tags to apply to the project."),
    export_folder: str | None = typer.Option(None, "--export-folder", "-ex", help="Project preferred export folder."),
    add: bool = typer.Option(False, "--add", "-a", help="Add a project (requires at least a project name)."),
    delete: bool = typer.Option(False, "--delete", "-de", help="Delete a project (requires at least a project name)."),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation on project deletion"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Fetch, create, delete a project."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        name=name,
        database=database,
        title=title,
        description=description,
        tags=tags,
        export_folder=export_folder,
        add=add,
        delete=delete,
        force=force,
        json=json_output,
    )
    ProjectService.do_command(args)


def backup_project_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Source project name"),
    archive: str | None = typer.Option(
        None,
        "--archive",
        "-ar",
        help="Archive file path in Opal file system. Default is <project>.zip in the project folder.",
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
    views_as_tables: bool = typer.Option(
        False,
        "--views-as-tables",
        "-vt",
        help="Treat views as tables, i.e. export data instead of keeping derivation scripts (default is false)",
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Backup project data: tables (data export), views, resources, report templates, files."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        archive=archive,
        force=force,
        views_as_tables=views_as_tables,
        json=json_output,
    )
    BackupProjectCommand.do_command(args)


def restore_project_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Source project name"),
    archive: str | None = typer.Option(
        None,
        "--archive",
        "-ar",
        help="Archive file path in Opal file system. Default is <project>.zip in the project folder.",
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation"),
    arpassword: str | None = typer.Option(
        None, "--arpassword", "-arp", help="Password to decrypt zip archive (optional)"
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Restore project data: tables (data import), views, resources, report templates, files."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        archive=archive,
        force=force,
        arpassword=arpassword,
        json=json_output,
    )
    RestoreProjectCommand.do_command(args)


# =============================================================================
# Dictionary Commands
# =============================================================================


def dict_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    name: str = typer.Argument(
        ...,
        help="Fully qualified name of a datasource/project or a table or a variable, "
        "for instance: opal-data or opal-data.questionnaire or "
        "opal-data.questionnaire:Q1. Wild cards can also be used, "
        'for instance: "*", "opal-data.*", etc.',
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
    excel: str | None = typer.Option(
        None, "--excel", "-xls", help="Full path of the target data dictionary Excel file."
    ),
):
    """Query for data dictionary."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        name=name,
        json=json_output,
        excel=excel,
    )
    DictionaryService.do_command(args)


def export_annot_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    name: str = typer.Argument(
        ...,
        help="Fully qualified name of a datasource/project or a table or a variable, for instance: "
        "opal-data or opal-data.questionnaire or opal-data.questionnaire:Q1. "
        'Wild cards can also be used, for instance: "opal-data.*", etc.',
    ),
    output: typer.FileTextWrite | None = typer.Option(
        None, "--output", "-out", help="CSV/TSV file to output (default is stdout)"
    ),
    locale: str | None = typer.Option(None, "--locale", "-l", help="Exported locale (default is none)"),
    separator: str | None = typer.Option(
        None, "--separator", "-s", help="Separator char for CSV/TSV format (default is the tabulation character)"
    ),
    taxonomies: list[str] | None = typer.Option(
        None,
        "--taxonomies",
        "-tx",
        help="The list of taxonomy names of interest (default is any that are found in the variable attributes)",
    ),
):
    """Extract data dictionary annotations in CSV/TSV format."""

    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        name=name,
        output=output if output else sys.stdout,
        locale=locale,
        separator=separator,
        taxonomies=taxonomies,
    )
    ExportAnnotationsService.do_command(args)


def import_annot_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    input_file: typer.FileText | None = typer.Option(
        None,
        "--input",
        "-in",
        help='CSV/TSV input file, typically the output of the "export-annot" command (default is stdin)',
    ),
    locale: str | None = typer.Option(None, "--locale", "-l", help="Destination annotation locale (default is none)"),
    separator: str | None = typer.Option(
        None, "--separator", "-s", help="Separator char for CSV/TSV format (default is the tabulation character)"
    ),
    destination: str | None = typer.Option(
        None,
        "--destination",
        "-d",
        help="Destination datasource name (default is the one(s) specified in the input file)",
    ),
    tables: list[str] | None = typer.Option(
        None,
        "--tables",
        "-t",
        help="The list of tables which variables are to be annotated (defaults to all that are found in the input file)",
    ),
    taxonomies: list[str] | None = typer.Option(
        None,
        "--taxonomies",
        "-tx",
        help="The list of taxonomy names of interest (default is any that is found in the input file)",
    ),
):
    """Apply data dictionary annotations specified in a file in CSV/TSV format (see export-annot)."""

    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        input=input_file if input_file else sys.stdin,
        locale=locale,
        separator=separator,
        destination=destination,
        tables=tables,
        taxonomies=taxonomies,
    )
    ImportAnnotationsService.do_command(args)


# =============================================================================
# Data Commands
# =============================================================================


def data_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    name: str = typer.Argument(
        ...,
        help="Fully qualified name of a table or a variable, "
        "for instance: opal-data.questionnaire or opal-data.questionnaire:Q1.",
    ),
    entity_id: str | None = typer.Option(
        None, "--id", "-i", help="Entity identifier. If missing the list of entities is returned."
    ),
    raw: bool = typer.Option(
        False, "--raw", "-r", help="Get raw value, output to stdout, useful for downloading a binary value"
    ),
    pos: str | None = typer.Option(
        None, "--pos", "-po", help="Position of the value to query in case of a repeatable variable (starting at 0)."
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Query for data."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        name=name,
        id=entity_id,
        raw=raw,
        pos=pos,
        json=json_output,
    )
    DataService.do_command(args)


def entity_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    id: str = typer.Argument(..., help="Identifier of the entity"),
    type: str = typer.Option("Participant", "--type", "-ty", help="Entity type"),
    tables: bool = typer.Option(False, "--tables", "-ta", help="Get the list of tables in which the entity exists"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Query for entities (Participant, etc.)."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        type=type,
        id=id,
        tables=tables,
        json=json_output,
    )
    EntityService.do_command(args)


# =============================================================================
# File Commands
# =============================================================================


def file_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    path: str = typer.Argument(..., help="File path in Opal file system."),
    download: bool = typer.Option(False, "--download", "-dl", help="Download file, or folder (as a zip file)."),
    download_password: str | None = typer.Option(
        None, "--download-password", "-dlp", help="Password to encrypt the file content."
    ),
    upload: str | None = typer.Option(
        None, "--upload", "-up", help="Upload a local file to a folder in Opal file system."
    ),
    delete: bool = typer.Option(False, "--delete", "-dt", help="Delete a file on Opal file system."),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation."),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Manage Opal file system."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        path=path,
        download=download,
        download_password=download_password,
        upload=upload,
        delete=delete,
        force=force,
        json=json_output,
    )
    FileService.do_command(args)


# =============================================================================
# Table Commands
# =============================================================================


def copy_table_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Source project name"),
    destination: str = typer.Option(..., "--destination", "-d", help="Destination project name"),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-t", help="List of table names to be copied (default is all)"
    ),
    name: str | None = typer.Option(
        None,
        "--name",
        "-na",
        help="New table name (required if source and destination are the same, "
        "ignored if more than one table is to be copied)",
    ),
    incremental: bool = typer.Option(False, "--incremental", "-i", help="Incremental copy"),
    nulls: bool = typer.Option(False, "--nulls", "-nu", help="Copy the null values"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Copy a table into another table."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        tables=tables,
        destination=destination,
        name=name,
        incremental=incremental,
        nulls=nulls,
        json=json_output,
    )
    CopyTableCommand.do_command(args)


def delete_table_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Project name"),
    tables: list[str] | None = typer.Option(None, "--tables", "-t", help="List of table names to be deleted"),
):
    """Delete some tables."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        tables=tables,
    )
    DeleteTableService.do_command(args)


def backup_view_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Source project name"),
    views: list[str] | None = typer.Option(
        None, "--views", "-vw", help="List of view names to be backed up (default is all)"
    ),
    output: str | None = typer.Option(
        None, "--output", "-out", help="Output directory name (default is current directory)"
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation when overwriting the backup file."),
):
    """Backup views of a project."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        views=views,
        output=output,
        force=force,
    )
    BackupViewService.do_command(args)


def restore_view_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Destination project name"),
    views: list[str] | None = typer.Option(
        None, "--views", "-vw", help="List of view names to be restored (default is all)"
    ),
    input: str | None = typer.Option(
        None, "--input", "-in", help="Input directory name (default is current directory)"
    ),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation when overwriting an existing view."),
):
    """Restore views of a project."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        views=views,
        input=input,
        force=force,
    )
    RestoreViewService.do_command(args)


# =============================================================================
# Import Commands
# =============================================================================


def import_plugin_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    name: str = typer.Option(..., "--name", "-n", help="Opal datasource plugin name"),
    config: str | None = typer.Option(
        None,
        "--config",
        "-c",
        help="A JSON file containing the import configuration. If not provided, the plugin will apply default values (or will fail).",
    ),
    destination: str = typer.Option(..., "--destination", "-d", help="Destination datasource name"),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-t", help="The list of tables to be imported (defaults to all)"
    ),
    incremental: bool = typer.Option(
        False, "--incremental", "-i", help="Incremental import (new and updated value sets)"
    ),
    limit: int | None = typer.Option(None, "--limit", "-li", help="Import limit (maximum number of value sets)"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    policy: str | None = typer.Option(
        None,
        "--policy",
        "-po",
        help='ID mapping policy: "required" (each identifiers must be mapped prior importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a system identifier for each unknown identifier)',
    ),
    merge: bool = typer.Option(
        False,
        "--merge",
        "-mg",
        help="Merge imported data dictionary with the destination one (default is false, i.e. data dictionary is overridden).",
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Import data from an Opal datasource plugin."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        name=name,
        config=config,
        destination=destination,
        tables=tables,
        incremental=incremental,
        limit=limit,
        identifiers=identifiers,
        policy=policy,
        merge=merge,
        json=json_output,
    )
    ImportPluginCommand.do_command(args)


def import_csv_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    path: str = typer.Option(
        ...,
        "--path",
        "-pa",
        help="Path to the CSV file in the Opal file system. Depending on the type of import (tables or variables), the path can be a folder or a single file.",
    ),
    destination: str = typer.Option(..., "--destination", "-d", help="Destination datasource name"),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-t", help="The list of tables to be imported (defaults to all)"
    ),
    incremental: bool = typer.Option(
        False, "--incremental", "-i", help="Incremental import (new and updated value sets)"
    ),
    limit: int | None = typer.Option(None, "--limit", "-li", help="Import limit (maximum number of value sets)"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    policy: str | None = typer.Option(
        None,
        "--policy",
        "-po",
        help='ID mapping policy: "required" (each identifiers must be mapped prior importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a system identifier for each unknown identifier)',
    ),
    merge: bool = typer.Option(
        False,
        "--merge",
        "-mg",
        help="Merge imported data dictionary with the destination one (default is false, i.e. data dictionary is overridden).",
    ),
    character_set: str | None = typer.Option(None, "--characterSet", "-c", help="Character set."),
    separator: str | None = typer.Option(None, "--separator", "-s", help="Field separator."),
    quote: str | None = typer.Option(None, "--quote", "-q", help="Quotation mark character."),
    first_row: int | None = typer.Option(None, "--firstRow", "-f", help="From row."),
    value_type: str | None = typer.Option(
        "text",
        "--valueType",
        "-vt",
        help="Default value type (text, integer, decimal, boolean etc.).",
    ),
    entity_type: str = typer.Option("Participant", "--type", "-ty", help="Entity type (e.g. Participant)"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Import data from a CSV file."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        path=path,
        destination=destination,
        tables=tables,
        incremental=incremental,
        limit=limit,
        identifiers=identifiers,
        policy=policy,
        merge=merge,
        characterSet=character_set,
        separator=separator,
        quote=quote,
        firstRow=first_row,
        valueType=value_type,
        type=entity_type,
        json=json_output,
    )
    ImportCSVCommand.do_command(args)


def import_xml_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    path: str = typer.Option(..., "--path", "-pa", help="Path to the XML ZIP file in the Opal file system"),
    destination: str = typer.Option(..., "--destination", "-d", help="Destination datasource name"),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-t", help="The list of tables to be imported (defaults to all)"
    ),
    incremental: bool = typer.Option(
        False, "--incremental", "-i", help="Incremental import (new and updated value sets)"
    ),
    limit: int | None = typer.Option(None, "--limit", "-li", help="Import limit (maximum number of value sets)"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    policy: str | None = typer.Option(
        None,
        "--policy",
        "-po",
        help='ID mapping policy: "required" (each identifiers must be mapped prior importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a system identifier for each unknown identifier)',
    ),
    merge: bool = typer.Option(
        False,
        "--merge",
        "-mg",
        help="Merge imported data dictionary with the destination one (default is false, i.e. data dictionary is overridden).",
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Import data from a ZIP file."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        path=path,
        destination=destination,
        tables=tables,
        incremental=incremental,
        limit=limit,
        identifiers=identifiers,
        policy=policy,
        merge=merge,
        json=json_output,
    )
    ImportXMLCommand.do_command(args)


def import_r_sas_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    path: str = typer.Option(..., "--path", "-pa", help="Path to the SAS file in the Opal file system"),
    destination: str = typer.Option(..., "--destination", "-d", help="Destination datasource name"),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-t", help="The list of tables to be imported (defaults to all)"
    ),
    incremental: bool = typer.Option(
        False, "--incremental", "-i", help="Incremental import (new and updated value sets)"
    ),
    limit: int | None = typer.Option(None, "--limit", "-li", help="Import limit (maximum number of value sets)"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    policy: str | None = typer.Option(
        None,
        "--policy",
        "-po",
        help='ID mapping policy: "required" (each identifiers must be mapped prior importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a system identifier for each unknown identifier)',
    ),
    merge: bool = typer.Option(
        False,
        "--merge",
        "-mg",
        help="Merge imported data dictionary with the destination one (default is false, i.e. data dictionary is overridden).",
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
    locale: str | None = typer.Option(None, "--locale", "-l", help="Preferred locale (e.g. 'en')"),
    type: str = typer.Option("Participant", "--type", "-ty", help="Entity type (e.g. 'Participant')"),
    idVariable: str | None = typer.Option(
        None,
        "--idVariable",
        "-iv",
        help="The name of the column representing the entity identifier. If not specified, the first column will be used.",
    ),
):
    """Import data from a SAS or SAS Transport file (using R)."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        path=path,
        destination=destination,
        tables=tables,
        incremental=incremental,
        limit=limit,
        identifiers=identifiers,
        policy=policy,
        merge=merge,
        json=json_output,
        locale=locale,
        type=type,
        idVariable=idVariable,
    )
    ImportRSASCommand.do_command(args)


def import_r_stata_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    path: str = typer.Option(..., "--path", "-pa", help="Path to the Stata file in the Opal file system"),
    destination: str = typer.Option(..., "--destination", "-d", help="Destination datasource name"),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-t", help="The list of tables to be imported (defaults to all)"
    ),
    incremental: bool = typer.Option(
        False, "--incremental", "-i", help="Incremental import (new and updated value sets)"
    ),
    limit: int | None = typer.Option(None, "--limit", "-li", help="Import limit (maximum number of value sets)"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    policy: str | None = typer.Option(
        None,
        "--policy",
        "-po",
        help='ID mapping policy: "required" (each identifiers must be mapped prior importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a system identifier for each unknown identifier)',
    ),
    merge: bool = typer.Option(
        False,
        "--merge",
        "-mg",
        help="Merge imported data dictionary with the destination one (default is false, i.e. data dictionary is overridden).",
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
    locale: str | None = typer.Option(None, "--locale", "-l", help="Preferred locale (e.g. 'en')"),
    type: str = typer.Option("Participant", "--type", "-ty", help="Entity type (e.g. 'Participant')"),
    idVariable: str | None = typer.Option(
        None,
        "--idVariable",
        "-iv",
        help="The name of the column representing the entity identifier. If not specified, the first column will be used.",
    ),
):
    """Import data from a Stata file (using R)."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        path=path,
        destination=destination,
        tables=tables,
        incremental=incremental,
        limit=limit,
        identifiers=identifiers,
        policy=policy,
        merge=merge,
        json=json_output,
        locale=locale,
        type=type,
        idVariable=idVariable,
    )
    ImportRSTATACommand.do_command(args)


def import_r_spss_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    path: str = typer.Option(..., "--path", "-pa", help="Path to the SPSS file in the Opal file system"),
    destination: str = typer.Option(..., "--destination", "-d", help="Destination datasource name"),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-t", help="The list of tables to be imported (defaults to all)"
    ),
    incremental: bool = typer.Option(
        False, "--incremental", "-i", help="Incremental import (new and updated value sets)"
    ),
    limit: int | None = typer.Option(None, "--limit", "-li", help="Import limit (maximum number of value sets)"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    policy: str | None = typer.Option(
        None,
        "--policy",
        "-po",
        help='ID mapping policy: "required" (each identifiers must be mapped prior importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a system identifier for each unknown identifier)',
    ),
    merge: bool = typer.Option(
        False,
        "--merge",
        "-mg",
        help="Merge imported data dictionary with the destination one (default is false, i.e. data dictionary is overridden).",
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
    locale: str | None = typer.Option(None, "--locale", "-l", help="Preferred locale (e.g. 'en')"),
    type: str = typer.Option("Participant", "--type", "-ty", help="Entity type (e.g. 'Participant')"),
    idVariable: str | None = typer.Option(
        None,
        "--idVariable",
        "-iv",
        help="The name of the column representing the entity identifier. If not specified, the first column will be used.",
    ),
):
    """Import data from a SPSS or compressed SPSS file (using R)."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        path=path,
        destination=destination,
        tables=tables,
        incremental=incremental,
        limit=limit,
        identifiers=identifiers,
        policy=policy,
        merge=merge,
        json=json_output,
        locale=locale,
        type=type,
        idVariable=idVariable,
    )
    ImportRSPSSCommand.do_command(args)


def import_r_rds_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    path: str = typer.Option(..., "--path", "-pa", help="Path to the RDS file in the Opal file system"),
    destination: str = typer.Option(..., "--destination", "-d", help="Destination datasource name"),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-t", help="The list of tables to be imported (defaults to all)"
    ),
    incremental: bool = typer.Option(
        False, "--incremental", "-i", help="Incremental import (new and updated value sets)"
    ),
    limit: int | None = typer.Option(None, "--limit", "-li", help="Import limit (maximum number of value sets)"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    policy: str | None = typer.Option(
        None,
        "--policy",
        "-po",
        help='ID mapping policy: "required" (each identifiers must be mapped prior importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a system identifier for each unknown identifier)',
    ),
    merge: bool = typer.Option(
        False,
        "--merge",
        "-mg",
        help="Merge imported data dictionary with the destination one (default is false, i.e. data dictionary is overridden).",
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
    type: str = typer.Option("Participant", "--type", "-ty", help="Entity type (e.g. 'Participant')"),
    idVariable: str | None = typer.Option(
        None,
        "--idVariable",
        "-iv",
        help="The name of the column representing the entity identifier. If not specified, the first column will be used.",
    ),
):
    """Import data from a RDS file (single serialized R object, expected to be a tibble, using R)."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        path=path,
        destination=destination,
        tables=tables,
        incremental=incremental,
        limit=limit,
        identifiers=identifiers,
        policy=policy,
        merge=merge,
        json=json_output,
        type=type,
        idVariable=idVariable,
    )
    ImportRDSCommand.do_command(args)


def import_opal_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    remote: str = typer.Option(..., "--ropal", "-ro", help="Remote Opal server URL"),
    remote_user: str = typer.Option(..., "--ruser", "-ru", help="Remote Opal user name"),
    remote_password: str | None = typer.Option(None, "--rpassword", "-rp", help="Remote Opal user password"),
    remote_token: str | None = typer.Option(None, "--rtoken", "-rt", help="Remote Opal user access token"),
    remote_datasource: str | None = typer.Option(
        None, "--rdatasource", "-rd", help="Remote datasource name (default is the destination datasource name)"
    ),
    destination: str = typer.Option(..., "--destination", "-d", help="Destination datasource name"),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-t", help="The list of tables to be imported (defaults to all)"
    ),
    incremental: bool = typer.Option(
        False, "--incremental", "-i", help="Incremental import (new and updated value sets)"
    ),
    limit: int | None = typer.Option(None, "--limit", "-li", help="Import limit (maximum number of value sets)"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    policy: str | None = typer.Option(
        None,
        "--policy",
        "-po",
        help='ID mapping policy: "required" (each identifiers must be mapped prior importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a system identifier for each unknown identifier)',
    ),
    merge: bool = typer.Option(
        False,
        "--merge",
        "-mg",
        help="Merge imported data dictionary with the destination one (default is false, i.e. data dictionary is overridden).",
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Import data from a remote Opal server."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        ropal=remote,
        rdatasource=remote_datasource,
        ruser=remote_user,
        rpassword=remote_password,
        rtoken=remote_token,
        destination=destination,
        tables=tables,
        incremental=incremental,
        limit=limit,
        identifiers=identifiers,
        policy=policy,
        merge=merge,
        json=json_output,
    )
    ImportOpalCommand.do_command(args)


def import_limesurvey_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    url: str = typer.Option(..., "--url", "-ur", help="LimeSurvey SQL database JDBC url"),
    uname: str = typer.Option(..., "--uname", "-un", help="LimeSurvey SQL database user name"),
    pword: str | None = typer.Option(None, "--pword", "-pw", help="LimeSurvey SQL database user password"),
    prefix: str | None = typer.Option(None, "--prefix", "-pr", help="Table prefix"),
    properties: str | None = typer.Option(
        None, "--properties", "-pp", help="A JSON file containing LimeSurvey properties"
    ),
    destination: str = typer.Option(..., "--destination", "-d", help="Destination datasource name"),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-t", help="The list of tables to be imported (defaults to all)"
    ),
    incremental: bool = typer.Option(
        False, "--incremental", "-i", help="Incremental import (new and updated value sets)"
    ),
    limit: int | None = typer.Option(None, "--limit", "-li", help="Import limit (maximum number of value sets)"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    policy: str | None = typer.Option(
        None,
        "--policy",
        "-po",
        help='ID mapping policy: "required" (each identifiers must be mapped prior importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a system identifier for each unknown identifier)',
    ),
    merge: bool = typer.Option(
        False,
        "--merge",
        "-mg",
        help="Merge imported data dictionary with the destination one (default is false, i.e. data dictionary is overridden).",
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Import data from a LimeSurvey database."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        url=url,
        uname=uname,
        pword=pword,
        prefix=prefix,
        properties=properties,
        destination=destination,
        tables=tables,
        incremental=incremental,
        limit=limit,
        identifiers=identifiers,
        policy=policy,
        merge=merge,
        json=json_output,
    )
    ImportLimeSurveyCommand.do_command(args)


def import_sql_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    database: str = typer.Option(..., "--database", "-db", help="SQL database JDBC url"),
    destination: str = typer.Option(..., "--destination", "-d", help="Destination datasource name"),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-t", help="The list of tables to be imported (defaults to all)"
    ),
    incremental: bool = typer.Option(
        False, "--incremental", "-i", help="Incremental import (new and updated value sets)"
    ),
    limit: int | None = typer.Option(None, "--limit", "-li", help="Import limit (maximum number of value sets)"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    policy: str | None = typer.Option(
        None,
        "--policy",
        "-po",
        help='ID mapping policy: "required" (each identifiers must be mapped prior importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a system identifier for each unknown identifier)',
    ),
    merge: bool = typer.Option(
        False,
        "--merge",
        "-mg",
        help="Merge imported data dictionary with the destination one (default is false, i.e. data dictionary is overridden).",
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Import data from a SQL database."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        database=database,
        destination=destination,
        tables=tables,
        incremental=incremental,
        limit=limit,
        identifiers=identifiers,
        policy=policy,
        merge=merge,
        json=json_output,
    )
    ImportSQLCommand.do_command(args)


def import_vcf_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(
        ..., "--project", "-pr", help="Project name into which genotypes data will be imported"
    ),
    vcf: list[str] = typer.Option(..., "--vcf", help="List of VCF/BCF file paths (in Opal file system)"),
):
    """Import genotypes data from some VCF/BCF files."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        vcf=vcf,
    )
    ImportVCFCommand.do_command(args)


def import_ids_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    type: str = typer.Option("Participant", "--type", "-ty", help="Entity type (e.g. Participant)"),
):
    """Import system identifiers."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        type=type,
    )
    ImportIDService.do_command(args)


def import_ids_map_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    type: str = typer.Option("Participant", "--type", "-ty", help="Entity type (e.g. Participant)"),
    mapping: str = typer.Option(..., "--mapping", "-m", help="Mapping name"),
    separator: str | None = typer.Option(",", "--separator", "-s", help="Field separator in the mapping file"),
):
    """Import identifiers mappings."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        type=type,
        map=mapping,
        separator=separator,
    )
    ImportIDMapService.do_command(args)


# =============================================================================
# Export Commands
# =============================================================================


def export_plugin_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    datasource: str = typer.Option(..., "--datasource", "-d", help="Project name"),
    tables: list[str] = typer.Option(..., "--tables", "-t", help="The list of tables to be exported"),
    name: str = typer.Option(..., "--name", "-n", help="Opal datasource plugin name"),
    config: str = typer.Option(..., "--config", "-c", help="A JSON file containing the export configuration"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Export data to a Opal datasource plugin."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        datasource=datasource,
        tables=tables,
        name=name,
        config=config,
        identifiers=identifiers,
        json=json_output,
    )
    ExportPluginCommand.do_command(args)


def export_csv_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    datasource: str = typer.Option(..., "--datasource", "-d", help="Project name"),
    tables: list[str] = typer.Option(..., "--tables", "-t", help="The list of tables to be exported"),
    output: str = typer.Option(..., "--output", "-out", help="Output folder path in Opal file system"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    id_name: str | None = typer.Option(None, "--id-name", "-in", help='Name of the ID column name. Default is "_id".'),
    no_multilines: bool = typer.Option(
        False, "--no-multilines", "-nl", help="Do not write value sequences as multiple lines"
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Export data to a folder of CSV files."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        datasource=datasource,
        tables=tables,
        output=output,
        identifiers=identifiers,
        id_name=id_name,
        no_multilines=no_multilines,
        json=json_output,
    )
    ExportCSVCommand.do_command(args)


def export_xml_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    datasource: str = typer.Option(..., "--datasource", "-d", help="Project name"),
    tables: list[str] = typer.Option(..., "--tables", "-t", help="The list of tables to be exported"),
    output: str = typer.Option(..., "--output", "-out", help="Output ZIP file path in Opal file system"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Export data to a zip of Opal XML files."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        datasource=datasource,
        tables=tables,
        output=output,
        identifiers=identifiers,
        json=json_output,
    )
    ExportXMLCommand.do_command(args)


def export_r_sas_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    datasource: str = typer.Option(..., "--datasource", "-d", help="Project name"),
    tables: list[str] = typer.Option(..., "--tables", "-t", help="The list of tables to be exported"),
    output: str = typer.Option(..., "--output", "-out", help="Output file path in Opal file system"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    id_name: str | None = typer.Option(None, "--id-name", "-in", help='Name of the ID column name. Default is "_id".'),
    no_multilines: bool = typer.Option(
        False, "--no-multilines", "-nl", help="Do not write value sequences as multiple lines"
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Export data to a SAS or SAS Transport file (using R)."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        datasource=datasource,
        tables=tables,
        output=output,
        identifiers=identifiers,
        id_name=id_name,
        no_multilines=no_multilines,
        json=json_output,
    )
    ExportRSASCommand.do_command(args)


def export_r_stata_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    datasource: str = typer.Option(..., "--datasource", "-d", help="Project name"),
    tables: list[str] = typer.Option(..., "--tables", "-t", help="The list of tables to be exported"),
    output: str = typer.Option(..., "--output", "-out", help="Output file path in Opal file system"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    id_name: str | None = typer.Option(None, "--id-name", "-in", help='Name of the ID column name. Default is "_id".'),
    no_multilines: bool = typer.Option(
        False, "--no-multilines", "-nl", help="Do not write value sequences as multiple lines"
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Export data to a Stata file (using R)."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        datasource=datasource,
        tables=tables,
        output=output,
        identifiers=identifiers,
        id_name=id_name,
        no_multilines=no_multilines,
        json=json_output,
    )
    ExportRSTATACommand.do_command(args)


def export_r_spss_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    datasource: str = typer.Option(..., "--datasource", "-d", help="Project name"),
    tables: list[str] = typer.Option(..., "--tables", "-t", help="The list of tables to be exported"),
    output: str = typer.Option(..., "--output", "-out", help="Output file path in Opal file system"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    id_name: str | None = typer.Option(None, "--id-name", "-in", help='Name of the ID column name. Default is "_id".'),
    no_multilines: bool = typer.Option(
        False, "--no-multilines", "-nl", help="Do not write value sequences as multiple lines"
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Export data to a SPSS or compressed SPSS file (using R)."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        datasource=datasource,
        tables=tables,
        output=output,
        identifiers=identifiers,
        id_name=id_name,
        no_multilines=no_multilines,
        json=json_output,
    )
    ExportRSPSSCommand.do_command(args)


def export_r_rds_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    datasource: str = typer.Option(..., "--datasource", "-d", help="Project name"),
    tables: list[str] = typer.Option(..., "--tables", "-t", help="The list of tables to be exported"),
    output: str = typer.Option(..., "--output", "-out", help="Output file path in Opal file system"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    id_name: str | None = typer.Option(None, "--id-name", "-in", help='Name of the ID column name. Default is "_id".'),
    no_multilines: bool = typer.Option(
        False, "--no-multilines", "-nl", help="Do not write value sequences as multiple lines"
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Export data to a RDS file (single serialized R object, using R)."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        datasource=datasource,
        tables=tables,
        output=output,
        identifiers=identifiers,
        id_name=id_name,
        no_multilines=no_multilines,
        json=json_output,
    )
    ExportRDSCommand.do_command(args)


def export_sql_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    datasource: str = typer.Option(..., "--datasource", "-d", help="Project name"),
    tables: list[str] = typer.Option(..., "--tables", "-t", help="The list of tables to be exported"),
    database: str = typer.Option(..., "--database", "-db", help="SQL database JDBC url"),
    identifiers: str | None = typer.Option(None, "--identifiers", "-id", help="Name of the ID mapping"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Export data to a SQL database."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        datasource=datasource,
        tables=tables,
        database=database,
        identifiers=identifiers,
        json=json_output,
    )
    ExportSQLCommand.do_command(args)


def export_vcf_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Project name"),
    vcf: list[str] = typer.Option(..., "--vcf", help="List of VCF/BCF file names"),
    destination: str = typer.Option(..., "--destination", "-d", help="Destination folder path in Opal file system"),
    filter_table: str | None = typer.Option(
        None, "--filter-table", "-f", help="Participant table name to be used to filter the samples by participant"
    ),
    no_case_controls: bool = typer.Option(
        False, "--no-case-controls", "-nocc", help="Do not include case-control data"
    ),
):
    """Export genotypes data to VCF/BCF files."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        vcf=vcf,
        destination=destination,
        filter_table=filter_table,
        no_case_controls=no_case_controls,
    )
    ExportVCFCommand.do_command(args)


def export_analysis_plugin_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Project name for which analysis data will be exported."),
    table: str | None = typer.Option(
        None, "--table", "-t", help="Table name for which analysis data will be exported."
    ),
    all_results: bool = typer.Option(
        False, "--all-results", "-ar", help="Export all results (default exports last result)."
    ),
    analysis_id: str | None = typer.Option(
        None, "--analysis-id", "-ai", help="A table Analysis ID for which results will be exported."
    ),
):
    """Exports analysis data of a project or specific tables."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        table=table,
        all_results=all_results,
        analysis_id=analysis_id,
    )
    ExportAnalysisService.do_command(args)


# =============================================================================
# User/Group Commands
# =============================================================================


def user_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    name: str | None = typer.Option(None, "--name", "-n", help="User name."),
    upassword: str | None = typer.Option(
        None,
        "--upassword",
        "-upa",
        help="User password of at least 8 characters, must contain at least one digit, one upper case alphabet, one lower case alphabet, one special character (which includes @#$%^&+=!) and no white space.",
    ),
    ucertificate: str | None = typer.Option(None, "--ucertificate", "-uc", help="User certificate (public key) file"),
    disabled: bool = typer.Option(
        False, "--disabled", "-di", help="Disable user account (if omitted the user is enabled by default)."
    ),
    groups: list[str] | None = typer.Option(None, "--groups", "-g", help="User groups"),
    fetch: bool = typer.Option(False, "--fetch", "-fe", help="Fetch one or multiple user(s)."),
    add: bool = typer.Option(False, "--add", "-a", help="Add a user."),
    update: bool = typer.Option(False, "--update", "-ud", help="Update a user."),
    delete: bool = typer.Option(False, "--delete", "-de", help="Delete a user."),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Manage users."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        name=name,
        upassword=upassword,
        ucertificate=ucertificate,
        disabled=disabled,
        groups=groups,
        fetch=fetch,
        add=add,
        update=update,
        delete=delete,
        json=json_output,
    )
    UserService.do_command(args)


def group_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    name: str | None = typer.Option(None, "--name", "-n", help="Group name."),
    add: bool = typer.Option(False, "--add", "-a", help="Add a group."),
    delete: bool = typer.Option(False, "--delete", "-de", help="Delete a group."),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Manage groups."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        name=name,
        add=add,
        delete=delete,
        json=json_output,
    )
    GroupService.do_command(args)


# =============================================================================
# Permission Commands
# =============================================================================


def perm_project_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Project name"),
    fetch: bool = typer.Option(False, "--fetch", "-f", help="Fetch permissions"),
    add: bool = typer.Option(False, "--add", "-a", help="Add a permission"),
    delete: bool = typer.Option(False, "--delete", "-d", help="Delete a permission"),
    permission: str | None = typer.Option(None, "--permission", "-pe", help="Permission to apply: administrate"),
    subject: str | None = typer.Option(
        None,
        "--subject",
        "-s",
        help="Subject name to which the permission will be granted/removed (required on add/delete)",
    ),
    type: str = typer.Option(..., "--type", "-ty", help="Subject type: user or group"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Get or apply permission on a project."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        fetch=fetch,
        add=add,
        delete=delete,
        permission=permission,
        subject=subject,
        type=type,
        json=json_output,
    )
    ProjectPermService.do_command(args)


def perm_datasource_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Project name"),
    fetch: bool = typer.Option(False, "--fetch", "-f", help="Fetch permissions"),
    add: bool = typer.Option(False, "--add", "-a", help="Add a permission"),
    delete: bool = typer.Option(False, "--delete", "-de", help="Delete a permission"),
    permission: str | None = typer.Option(
        None, "--permission", "-pe", help="Permission to apply: view-value, add-table, administrate"
    ),
    subject: str | None = typer.Option(
        None,
        "--subject",
        "-s",
        help="Subject name to which the permission will be granted/removed (required on add/delete)",
    ),
    type: str = typer.Option(..., "--type", "-ty", help="Subject type: user or group"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Get or apply permission on a datasource."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        fetch=fetch,
        add=add,
        delete=delete,
        permission=permission,
        subject=subject,
        type=type,
        json=json_output,
    )
    DatasourcePermService.do_command(args)


def perm_table_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Project name"),
    tables: list[str] | None = typer.Option(
        None, "--tables", "-t", help="List of table names on which the permission is to be get/set (default is all)"
    ),
    fetch: bool = typer.Option(False, "--fetch", "-f", help="Fetch permissions"),
    add: bool = typer.Option(False, "--add", "-a", help="Add a permission"),
    delete: bool = typer.Option(False, "--delete", "-d", help="Delete a permission"),
    permission: str | None = typer.Option(
        None, "--permission", "-pe", help="Permission to apply: view, view-value, edit, edit-values, administrate"
    ),
    subject: str | None = typer.Option(
        None,
        "--subject",
        "-s",
        help="Subject name to which the permission will be granted/removed (required on add/delete)",
    ),
    type: str = typer.Option(..., "--type", "-ty", help="Subject type: user or group"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Get or apply permission on a set of tables."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        tables=tables,
        fetch=fetch,
        add=add,
        delete=delete,
        permission=permission,
        subject=subject,
        type=type,
        json=json_output,
    )
    TablePermService.do_command(args)


def perm_variable_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Project name"),
    table: str = typer.Option(..., "--table", "-t", help="Table name to which the variables belong"),
    variables: list[str] | None = typer.Option(
        None,
        "--variables",
        "-va",
        help="List of variable names on which the permission is to be get/set (default is all)",
    ),
    fetch: bool = typer.Option(False, "--fetch", "-f", help="Fetch permissions"),
    add: bool = typer.Option(False, "--add", "-a", help="Add a permission"),
    delete: bool = typer.Option(False, "--delete", "-d", help="Delete a permission"),
    permission: str | None = typer.Option(None, "--permission", "-pe", help="Permission to apply: view"),
    subject: str | None = typer.Option(
        None,
        "--subject",
        "-s",
        help="Subject name to which the permission will be granted/removed (required on add/delete)",
    ),
    type: str = typer.Option(..., "--type", "-ty", help="Subject type: user or group"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Get or apply permission on a set of variables."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        table=table,
        variables=variables,
        fetch=fetch,
        add=add,
        delete=delete,
        permission=permission,
        subject=subject,
        type=type,
        json=json_output,
    )
    VariablePermService.do_command(args)


def perm_resources_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Project name"),
    fetch: bool = typer.Option(False, "--fetch", "-f", help="Fetch permissions"),
    add: bool = typer.Option(False, "--add", "-a", help="Add a permission"),
    delete: bool = typer.Option(False, "--delete", "-d", help="Delete a permission"),
    permission: str | None = typer.Option(None, "--permission", "-pe", help="Permission to apply: view, administrate"),
    subject: str | None = typer.Option(
        None,
        "--subject",
        "-s",
        help="Subject name to which the permission will be granted/removed (required on add/delete)",
    ),
    type: str = typer.Option(..., "--type", "-ty", help="Subject type: user or group"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Get or apply permission on resources as a whole."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        fetch=fetch,
        add=add,
        delete=delete,
        permission=permission,
        subject=subject,
        type=type,
        json=json_output,
    )
    ResourcesPermService.do_command(args)


def perm_resource_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Project name"),
    resources: list[str] | None = typer.Option(
        None,
        "--resources",
        "-r",
        help="List of resource names on which the permission is to be get/set (default is all)",
    ),
    fetch: bool = typer.Option(False, "--fetch", "-f", help="Fetch permissions"),
    add: bool = typer.Option(False, "--add", "-a", help="Add a permission"),
    delete: bool = typer.Option(False, "--delete", "-d", help="Delete a permission"),
    permission: str | None = typer.Option(None, "--permission", "-pe", help="Permission to apply: view, administrate"),
    subject: str | None = typer.Option(
        None,
        "--subject",
        "-s",
        help="Subject name to which the permission will be granted/removed (required on add/delete)",
    ),
    type: str = typer.Option(..., "--type", "-ty", help="Subject type: user or group"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Get or apply permission on a set of resources."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        resources=resources,
        fetch=fetch,
        add=add,
        delete=delete,
        permission=permission,
        subject=subject,
        type=type,
        json=json_output,
    )
    ResourcePermService.do_command(args)


def perm_r_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    fetch: bool = typer.Option(False, "--fetch", "-f", help="Fetch permissions"),
    add: bool = typer.Option(False, "--add", "-a", help="Add a permission"),
    delete: bool = typer.Option(False, "--delete", "-d", help="Delete a permission"),
    permission: str | None = typer.Option(None, "--permission", "-pe", help="Permission to apply: use"),
    subject: str | None = typer.Option(
        None,
        "--subject",
        "-s",
        help="Subject name to which the permission will be granted/removed (required on add/delete)",
    ),
    type: str = typer.Option(..., "--type", "-ty", help="Subject type: user or group"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Get or apply R permission."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        fetch=fetch,
        add=add,
        delete=delete,
        permission=permission,
        subject=subject,
        type=type,
        json=json_output,
    )
    RPermService.do_command(args)


def perm_datashield_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    fetch: bool = typer.Option(False, "--fetch", "-f", help="Fetch permissions"),
    add: bool = typer.Option(False, "--add", "-a", help="Add a permission"),
    delete: bool = typer.Option(False, "--delete", "-d", help="Delete a permission"),
    permission: str | None = typer.Option(None, "--permission", "-pe", help="Permission to apply: use, administrate"),
    subject: str | None = typer.Option(
        None,
        "--subject",
        "-s",
        help="Subject name to which the permission will be granted/removed (required on add/delete)",
    ),
    type: str = typer.Option(..., "--type", "-ty", help="Subject type: user or group"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Get or apply DataSHIELD permission."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        fetch=fetch,
        add=add,
        delete=delete,
        permission=permission,
        subject=subject,
        type=type,
        json=json_output,
    )
    DataSHIELDPermService.do_command(args)


def perm_system_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    fetch: bool = typer.Option(False, "--fetch", "-f", help="Fetch permissions"),
    add: bool = typer.Option(False, "--add", "-a", help="Add a permission"),
    delete: bool = typer.Option(False, "--delete", "-d", help="Delete a permission"),
    permission: str | None = typer.Option(
        None, "--permission", "-pe", help="Permission to apply: add-project, administrate"
    ),
    subject: str | None = typer.Option(
        None,
        "--subject",
        "-s",
        help="Subject name to which the permission will be granted/removed (required on add/delete)",
    ),
    type: str = typer.Option(..., "--type", "-ty", help="Subject type: user or group"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Get or apply system permission."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        fetch=fetch,
        add=add,
        delete=delete,
        permission=permission,
        subject=subject,
        type=type,
        json=json_output,
    )
    SystemPermService.do_command(args)


# =============================================================================
# System Commands
# =============================================================================


def system_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
    version: bool = typer.Option(False, "--version", help="Opal version number"),
    env: bool = typer.Option(False, "--env", help="Opal java execution environment (JVM related statistic properties)"),
    status: bool = typer.Option(False, "--status", help="Opal application status (JVM related dynamic properties)"),
    conf: bool = typer.Option(False, "--conf", help="Opal application configuration"),
):
    """Query for system status and configuration."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        json=json_output,
        version=version,
        env=env,
        status=status,
        conf=conf,
    )
    SystemService.do_command(args)


def plugin_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    list_plugins: bool = typer.Option(False, "--list", "-ls", help="List the installed plugins."),
    updates: bool = typer.Option(False, "--updates", "-lu", help="List the installed plugins that can be updated."),
    available: bool = typer.Option(False, "--available", "-la", help="List the new plugins that could be installed."),
    install: str | None = typer.Option(
        None,
        "--install",
        "-i",
        help="Install a plugin by providing its name or name:version or a path to a plugin archive file (in Opal file system). If no version is specified, the latest version is installed. Requires system restart to be effective.",
    ),
    remove: str | None = typer.Option(
        None, "--remove", "-rm", help="Remove a plugin by providing its name. Requires system restart to be effective."
    ),
    reinstate: str | None = typer.Option(
        None, "--reinstate", "-ri", help="Reinstate a plugin that was previously removed by providing its name."
    ),
    fetch: str | None = typer.Option(None, "--fetch", "-f", help="Get the named plugin description."),
    configure: str | None = typer.Option(
        None,
        "--configure",
        "-c",
        help="Configure the plugin site properties. Usually requires to restart the associated service to be effective.",
    ),
    status: str | None = typer.Option(
        None, "--status", "-su", help="Get the status of the service associated to the named plugin."
    ),
    start: str | None = typer.Option(None, "--start", "-sa", help="Start the service associated to the named plugin."),
    stop: str | None = typer.Option(None, "--stop", "-so", help="Stop the service associated to the named plugin."),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Manage system plugins."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        list=list_plugins,
        updates=updates,
        available=available,
        install=install,
        remove=remove,
        reinstate=reinstate,
        fetch=fetch,
        configure=configure,
        status=status,
        start=start,
        stop=stop,
        json=json_output,
    )
    PluginService.do_command(args)


def task_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    id: str | None = typer.Option(
        None,
        "--id",
        "-i",
        help="The task ID. If not provided, it will be read from the standard input (from the JSON representation of the task or a plain value).",
    ),
    status: bool = typer.Option(False, "--status", "-st", help="Get the status of the task"),
    cancel: bool = typer.Option(False, "--cancel", "-c", help="Cancel the task"),
    delete: bool = typer.Option(False, "--delete", "-d", help="Delete the task"),
    show: bool = typer.Option(False, "--show", "-sh", help="Show JSON representation of the task"),
    wait: bool = typer.Option(False, "--wait", "-w", help="Wait for the task to complete (successfully or not)"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Manage a task."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        id=id,
        status=status,
        cancel=cancel,
        delete=delete,
        show=show,
        wait=wait,
        json=json_output,
    )
    TaskService.do_command(args)


def taxonomy_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    import_file: str | None = typer.Option(
        None, "--import-file", "-if", help="Import a taxonomy from the provided Opal file path (YAML format)."
    ),
    download: str | None = typer.Option(
        None, "--download", "-dl", help="Download a taxonomy from the provided name, in YAML format"
    ),
    delete: str | None = typer.Option(None, "--delete", "-dt", help="Delete a taxonomy by name."),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation."),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Manage taxonomies: list available taxonomies, download, import or delete a taxonomy."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        import_file=import_file,
        download=download,
        delete=delete,
        force=force,
        json=json_output,
    )
    TaxonomyService.do_command(args)


def rest_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    ws: str = typer.Argument(..., help="Web service path, e.g. /system/subject-profile/_current"),
    method: str = typer.Option("GET", "--method", "-m", help="HTTP method: GET, POST, PUT, DELETE, OPTIONS"),
    content_type: str | None = typer.Option(None, "--content-type", "-ct", help="Content type of the request body"),
    accept: str | None = typer.Option(None, "--accept", "-a", help="Acceptable response content type"),
    content: str | None = typer.Option(None, "--content", "-c", help="Request body content"),
    headers: str | None = typer.Option(
        None, "--headers", "-hs", help='Custom headers in the form of: { "Key1": "Value1", "Key2": "Value2" }'
    ),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Request directly the Opal REST API, for advanced users."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        ws=ws,
        method=method,
        content_type=content_type,
        accept=accept,
        content=content,
        headers=headers,
        json=json_output,
    )
    RESTService.do_command(args)


# =============================================================================
# Security Commands
# =============================================================================


def encrypt_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    plain: str = typer.Argument(..., help="Plain text to encrypt"),
):
    """Encrypt string using Opal's secret key."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        plain=plain,
    )
    EncryptService.do_command(args)


def decrypt_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    encrypted: str = typer.Argument(..., help="Encrypted text to decrypt"),
):
    """Decrypt string using Opal's secret key."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        encrypted=encrypted,
    )
    DecryptService.do_command(args)


# =============================================================================
# SQL Commands
# =============================================================================


def sql_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    query: str = typer.Option(..., "--query", "-q", help="SQL query"),
    project: str | None = typer.Option(
        None,
        "--project",
        "-pr",
        help="Source project name, that will be used to resolve the table names in the FROM statement. If not provided, the fully qualified table names must be specified in the query (escaped by backquotes: `<project>.<table>`).",
    ),
    format: str | None = typer.Option(
        "csv", "--format", "-f", help='The format of the output, can be "json" or "csv".'
    ),
    id_name: str | None = typer.Option("_id", "--id-name", "-in", help="Name of the ID column name."),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Execute a SQL statement on project's tables."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        query=query,
        format=format,
        id_name=id_name,
        json=json_output,
    )
    SQLService.do_command(args)


def sql_history_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str | None = typer.Option(
        None,
        "--project",
        "-pr",
        help="Project name used as the SQL execution context to filter. If not specified, history from any context is returned. If '*' is specified, history of SQL execution without context is returned.",
    ),
    offset: str | None = typer.Option(
        None,
        "--offset",
        "-os",
        help="Number of history items to skip. Default is 0 (note that the items are ordered by most recent first).",
    ),
    limit: str | None = typer.Option(
        None, "--limit", "-lm", help="Maximum number of history items to return. Default is 100."
    ),
    subject: str | None = typer.Option(None, "--subject", "-sb", help="Subject name to filter."),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """SQL execution history of current user or of other users (administrator only)."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        offset=offset,
        limit=limit,
        subject=subject,
        json=json_output,
    )
    SQLHistoryService.do_command(args)


# =============================================================================
# Analysis Commands
# =============================================================================


def analysis_plugin_command(
    ctx: typer.Context,
    opal: str = typer.Option("http://localhost:8080", "--opal", "-o", help="Opal server base url"),
    user: str | None = typer.Option(
        None, "--user", "-u", help="Credentials auth: user name (password will be requested if not provided)"
    ),
    password: str | None = typer.Option(
        None, "--password", "-p", help="Credentials auth: user password (requires a user name)"
    ),
    token: str | None = typer.Option(None, "--token", "-tk", help="Token auth: User access token"),
    ssl_cert: str | None = typer.Option(
        None, "--ssl-cert", "-sc", help="Two-way SSL auth: certificate/public key file (requires a private key)"
    ),
    ssl_key: str | None = typer.Option(
        None, "--ssl-key", "-sk", help="Two-way SSL auth: private key file (requires a certificate)"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False, "--no-ssl-verify", "-nv", help="Do not verify SSL certificates for HTTPS."
    ),
    project: str = typer.Option(..., "--project", "-pr", help="Project name"),
    config: str = typer.Option(..., "--config", "-c", help="A local JSON file containing the analysis configuration"),
    json_output: bool = typer.Option(False, "--json", "-j", help="Pretty JSON formatting of the response"),
):
    """Analyses a project variables using external R plugins."""
    args = _make_args_with_globals(
        ctx,
        opal=opal,
        user=user,
        password=password,
        token=token,
        ssl_cert=ssl_cert,
        ssl_key=ssl_key,
        verbose=verbose,
        no_ssl_verify=no_ssl_verify,
        project=project,
        config=config,
        json=json_output,
    )
    AnalysisCommand.do_command(args)
