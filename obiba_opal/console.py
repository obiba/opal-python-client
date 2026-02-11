#
# Opal commands main entry point (Typer-based)
#
from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import TypeVar

import typer

from obiba_opal import commands as cmd
from obiba_opal.core import Formatter, HTTPError

F = TypeVar("F", bound=Callable[..., None])

app = typer.Typer(
    help="Opal command line tool.",
    add_completion=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)


def handle_exceptions(func: F) -> F:
    """Decorator to handle HTTP errors gracefully."""

    @wraps(func)
    def wrapper(*args, **kwargs) -> None:
        try:
            return func(*args, **kwargs)
        except HTTPError as e:
            Formatter.print_json(e.error, False)
            raise typer.Exit(code=2) from None

    return wrapper  # type: ignore[return-value]


# =============================================================================
# Register all commands
# =============================================================================

# Core commands
app.command(name="project", help="Fetch, create, delete a project.")(handle_exceptions(cmd.project_command))
app.command(name="dict", help="Query for data dictionary.")(handle_exceptions(cmd.dict_command))
app.command(name="data", help="Query for data.")(handle_exceptions(cmd.data_command))
app.command(name="entity", help="Query for entities (Participant, etc.).")(handle_exceptions(cmd.entity_command))
app.command(name="file", help="Manage Opal file system.")(handle_exceptions(cmd.file_command))

# Backup/Restore commands
app.command(
    name="backup-project",
    help="Backup project data: tables (data export), views, resources, report templates, files.",
)(handle_exceptions(cmd.backup_project_command))
app.command(
    name="restore-project",
    help="Restore project data: tables (data import), views, resources, report templates, files.",
)(handle_exceptions(cmd.restore_project_command))
app.command(name="backup-view", help="Backup views of a project.")(handle_exceptions(cmd.backup_view_command))
app.command(name="restore-view", help="Restore views of a project.")(handle_exceptions(cmd.restore_view_command))

# Table commands
app.command(name="copy-table", help="Copy a table into another table.")(handle_exceptions(cmd.copy_table_command))
app.command(name="delete-table", help="Delete some tables.")(handle_exceptions(cmd.delete_table_command))

# Dictionary annotation commands
app.command(
    name="export-annot",
    help="Extract data dictionary annotations in CSV/TSV format.",
)(handle_exceptions(cmd.export_annot_command))
app.command(
    name="import-annot",
    help="Apply data dictionary annotations specified in a file in CSV/TSV format (see export-annot).",
)(handle_exceptions(cmd.import_annot_command))

# Import commands
app.command(name="import-opal", help="Import data from a remote Opal server.")(
    handle_exceptions(cmd.import_opal_command)
)
app.command(name="import-csv", help="Import data from a CSV file.")(handle_exceptions(cmd.import_csv_command))
app.command(name="import-xml", help="Import data from a ZIP file.")(handle_exceptions(cmd.import_xml_command))
app.command(
    name="import-r-sas",
    help="Import data from a SAS or SAS Transport file (using R).",
)(handle_exceptions(cmd.import_r_sas_command))
app.command(name="import-r-stata", help="Import data from a Stata file (using R).")(
    handle_exceptions(cmd.import_r_stata_command)
)
app.command(
    name="import-r-spss",
    help="Import data from a SPSS or compressed SPSS file (using R).",
)(handle_exceptions(cmd.import_r_spss_command))
app.command(
    name="import-r-rds",
    help="Import data from a RDS file (single serialized R object, expected to be a tibble, using R).",
)(handle_exceptions(cmd.import_r_rds_command))
app.command(name="import-plugin", help="Import data from an Opal datasource plugin.")(
    handle_exceptions(cmd.import_plugin_command)
)
app.command(
    name="import-limesurvey",
    help="Import data from a LimeSurvey database.",
)(handle_exceptions(cmd.import_limesurvey_command))
app.command(name="import-sql", help="Import data from a SQL database.")(handle_exceptions(cmd.import_sql_command))
app.command(name="import-vcf", help="Import genotypes data from some VCF/BCF files.")(
    handle_exceptions(cmd.import_vcf_command)
)
app.command(name="import-ids", help="Import system identifiers.")(handle_exceptions(cmd.import_ids_command))
app.command(name="import-ids-map", help="Import identifiers mappings.")(handle_exceptions(cmd.import_ids_map_command))

# Export commands
app.command(name="export-xml", help="Export data to a zip of Opal XML files.")(
    handle_exceptions(cmd.export_xml_command)
)
app.command(name="export-csv", help="Export data to a folder of CSV files.")(handle_exceptions(cmd.export_csv_command))
app.command(
    name="export-r-sas",
    help="Export data to a SAS or SAS Transport file (using R).",
)(handle_exceptions(cmd.export_r_sas_command))
app.command(name="export-r-stata", help="Export data to a Stata file (using R).")(
    handle_exceptions(cmd.export_r_stata_command)
)
app.command(
    name="export-r-spss",
    help="Export data to a SPSS or compressed SPSS file (using R).",
)(handle_exceptions(cmd.export_r_spss_command))
app.command(
    name="export-r-rds",
    help="Export data to a RDS file (single serialized R object, using R).",
)(handle_exceptions(cmd.export_r_rds_command))
app.command(name="export-sql", help="Export data to a SQL database.")(handle_exceptions(cmd.export_sql_command))
app.command(name="export-plugin", help="Export data to a Opal datasource plugin.")(
    handle_exceptions(cmd.export_plugin_command)
)
app.command(name="export-vcf", help="Export genotypes data to VCF/BCF files.")(
    handle_exceptions(cmd.export_vcf_command)
)

# Analysis export
app.command(
    name="export-analysis-plugin",
    help="Exports analysis data of a project or specific tables.",
)(handle_exceptions(cmd.export_analysis_plugin_command))

# User/Group commands
app.command(name="user", help="Manage users.")(handle_exceptions(cmd.user_command))
app.command(name="group", help="Manage groups.")(handle_exceptions(cmd.group_command))

# Permission commands
app.command(name="perm-project", help="Get or apply permission on a project.")(
    handle_exceptions(cmd.perm_project_command)
)
app.command(name="perm-datasource", help="Get or apply permission on a datasource.")(
    handle_exceptions(cmd.perm_datasource_command)
)
app.command(name="perm-table", help="Get or apply permission on a set of tables.")(
    handle_exceptions(cmd.perm_table_command)
)
app.command(name="perm-variable", help="Get or apply permission on a set of variables.")(
    handle_exceptions(cmd.perm_variable_command)
)
app.command(
    name="perm-resources",
    help="Get or apply permission on resources as a whole.",
)(handle_exceptions(cmd.perm_resources_command))
app.command(name="perm-resource", help="Get or apply permission on a set of resources.")(
    handle_exceptions(cmd.perm_resource_command)
)
app.command(name="perm-r", help="Get or apply R permission.")(handle_exceptions(cmd.perm_r_command))
app.command(name="perm-datashield", help="Get or apply DataSHIELD permission.")(
    handle_exceptions(cmd.perm_datashield_command)
)
app.command(name="perm-system", help="Get or apply system permission.")(handle_exceptions(cmd.perm_system_command))

# System commands
app.command(name="plugin", help="Manage system plugins.")(handle_exceptions(cmd.plugin_command))
app.command(name="system", help="Query for system status and configuration.")(handle_exceptions(cmd.system_command))
app.command(name="task", help="Manage a task.")(handle_exceptions(cmd.task_command))
app.command(name="rest", help="Request directly the Opal REST API, for advanced users.")(
    handle_exceptions(cmd.rest_command)
)
app.command(
    name="taxonomy",
    help="Manage taxonomies: list available taxonomies, download, import or delete a taxonomy.",
)(handle_exceptions(cmd.taxonomy_command))

# Security commands
app.command(name="encrypt", help="Encrypt string using Opal's secret key.")(handle_exceptions(cmd.encrypt_command))
app.command(name="decrypt", help="Decrypt string using Opal's secret key.")(handle_exceptions(cmd.decrypt_command))

# SQL commands
app.command(name="sql", help="Execute a SQL statement on project's tables.")(handle_exceptions(cmd.sql_command))
app.command(
    name="sql-history",
    help="SQL execution history of current user or of other users (administrator only).",
)(handle_exceptions(cmd.sql_history_command))

# Analysis commands
app.command(
    name="analysis-plugin",
    help="Analyses a project variables using external R plugins.",
)(handle_exceptions(cmd.analysis_plugin_command))


# =============================================================================
# Global options callback
# =============================================================================


def prompt_password() -> str:
    return typer.prompt("Enter password", hide_input=True)


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    opal: str = typer.Option(
        "http://localhost:8080",
        "--opal",
        "-o",
        help="Opal server base url (default: http://localhost:8080)",
    ),
    user: str | None = typer.Option(
        None,
        "--user",
        "-u",
        help="Credentials auth: user name (requires a password)",
    ),
    password: str | None = typer.Option(
        None,
        "--password",
        "-p",
        help="Credentials auth: user password (requires a user name)",
    ),
    token: str | None = typer.Option(
        None,
        "--token",
        "-tk",
        help="Token auth: User access token",
    ),
    ssl_cert: str | None = typer.Option(
        None,
        "--ssl-cert",
        "-sc",
        help="Two-way SSL auth: certificate/public key file (requires a private key)",
    ),
    ssl_key: str | None = typer.Option(
        None,
        "--ssl-key",
        "-sk",
        help="Two-way SSL auth: private key file (requires a certificate)",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
    no_ssl_verify: bool = typer.Option(
        False,
        "--no-ssl-verify",
        "-nv",
        help="Do not verify SSL certificates for HTTPS.",
    ),
) -> None:
    ctx.ensure_object(dict)

    # Prompt for password if needed
    if not (ssl_cert or ssl_key) and not token and not password and user:
        password = prompt_password()

    ctx.obj.update({
        "opal": opal,
        "user": user,
        "password": password,
        "token": token,
        "ssl_cert": ssl_cert,
        "ssl_key": ssl_key,
        "verbose": verbose,
        "no_ssl_verify": no_ssl_verify,
    })
    if ctx.invoked_subcommand is None:
        typer.echo("Opal command line tool.")
        typer.echo("For more details: opal --help")
        raise typer.Exit()


def run() -> None:
    app()
