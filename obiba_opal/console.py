#
# Opal commands main entry point (Typer-based)
#
from __future__ import annotations

import typer

from obiba_opal import commands as cmd

app = typer.Typer(
    help="Opal command line tool.",
    add_completion=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)


# =============================================================================
# Register all commands
# =============================================================================

# Core commands
app.command(name="project", help="Fetch, create, delete a project.")(cmd.project_command)
app.command(name="dict", help="Query for data dictionary.")(cmd.dict_command)
app.command(name="data", help="Query for data.")(cmd.data_command)
app.command(name="entity", help="Query for entities (Participant, etc.).")(cmd.entity_command)
app.command(name="file", help="Manage Opal file system.")(cmd.file_command)

# Backup/Restore commands
app.command(
    name="backup_project",
    help="Backup project data: tables (data export), views, resources, report templates, files.",
)(cmd.backup_project_command)
app.command(
    name="restore_project",
    help="Restore project data: tables (data import), views, resources, report templates, files.",
)(cmd.restore_project_command)
app.command(name="backup_view", help="Backup views of a project.")(cmd.backup_view_command)
app.command(name="restore_view", help="Restore views of a project.")(cmd.restore_view_command)

# Table commands
app.command(name="copy_table", help="Copy a table into another table.")(cmd.copy_table_command)
app.command(name="delete_table", help="Delete some tables.")(cmd.delete_table_command)

# Dictionary annotation commands
app.command(
    name="export_annot",
    help="Extract data dictionary annotations in CSV/TSV format.",
)(cmd.export_annot_command)
app.command(
    name="import_annot",
    help="Apply data dictionary annotations specified in a file in CSV/TSV format (see export-annot).",
)(cmd.import_annot_command)

# Import commands
app.command(name="import_opal", help="Import data from a remote Opal server.")(cmd.import_opal_command)
app.command(name="import_csv", help="Import data from a CSV file.")(cmd.import_csv_command)
app.command(name="import_xml", help="Import data from a ZIP file.")(cmd.import_xml_command)
app.command(
    name="import_r_sas",
    help="Import data from a SAS or SAS Transport file (using R).",
)(cmd.import_r_sas_command)
app.command(name="import_r_stata", help="Import data from a Stata file (using R).")(cmd.import_r_stata_command)
app.command(
    name="import_r_spss",
    help="Import data from a SPSS or compressed SPSS file (using R).",
)(cmd.import_r_spss_command)
app.command(
    name="import_r_rds",
    help="Import data from a RDS file (single serialized R object, expected to be a tibble, using R).",
)(cmd.import_r_rds_command)
app.command(name="import_plugin", help="Import data from an Opal datasource plugin.")(cmd.import_plugin_command)
app.command(
    name="import_limesurvey",
    help="Import data from a LimeSurvey database.",
)(cmd.import_limesurvey_command)
app.command(name="import_sql", help="Import data from a SQL database.")(cmd.import_sql_command)
app.command(name="import_vcf", help="Import genotypes data from some VCF/BCF files.")(cmd.import_vcf_command)
app.command(name="import_ids", help="Import system identifiers.")(cmd.import_ids_command)
app.command(name="import_ids_map", help="Import identifiers mappings.")(cmd.import_ids_map_command)

# Export commands
app.command(name="export_xml", help="Export data to a zip of Opal XML files.")(cmd.export_xml_command)
app.command(name="export_csv", help="Export data to a folder of CSV files.")(cmd.export_csv_command)
app.command(
    name="export_r_sas",
    help="Export data to a SAS or SAS Transport file (using R).",
)(cmd.export_r_sas_command)
app.command(name="export_r_stata", help="Export data to a Stata file (using R).")(cmd.export_r_stata_command)
app.command(
    name="export_r_spss",
    help="Export data to a SPSS or compressed SPSS file (using R).",
)(cmd.export_r_spss_command)
app.command(
    name="export_r_rds",
    help="Export data to a RDS file (single serialized R object, using R).",
)(cmd.export_r_rds_command)
app.command(name="export_sql", help="Export data to a SQL database.")(cmd.export_sql_command)
app.command(name="export_plugin", help="Export data to a Opal datasource plugin.")(cmd.export_plugin_command)
app.command(name="export_vcf", help="Export genotypes data to VCF/BCF files.")(cmd.export_vcf_command)

# Analysis export
app.command(
    name="export_analysis_plugin",
    help="Exports analysis data of a project or specific tables.",
)(cmd.export_analysis_plugin_command)

# User/Group commands
app.command(name="user", help="Manage users.")(cmd.user_command)
app.command(name="group", help="Manage groups.")(cmd.group_command)

# Permission commands
app.command(name="perm_project", help="Get or apply permission on a project.")(cmd.perm_project_command)
app.command(name="perm_datasource", help="Get or apply permission on a datasource.")(cmd.perm_datasource_command)
app.command(name="perm_table", help="Get or apply permission on a set of tables.")(cmd.perm_table_command)
app.command(name="perm_variable", help="Get or apply permission on a set of variables.")(cmd.perm_variable_command)
app.command(
    name="perm_resources",
    help="Get or apply permission on resources as a whole.",
)(cmd.perm_resources_command)
app.command(name="perm_resource", help="Get or apply permission on a set of resources.")(cmd.perm_resource_command)
app.command(name="perm_r", help="Get or apply R permission.")(cmd.perm_r_command)
app.command(name="perm_datashield", help="Get or apply DataSHIELD permission.")(cmd.perm_datashield_command)
app.command(name="perm_system", help="Get or apply system permission.")(cmd.perm_system_command)

# System commands
app.command(name="plugin", help="Manage system plugins.")(cmd.plugin_command)
app.command(name="system", help="Query for system status and configuration.")(cmd.system_command)
app.command(name="task", help="Manage a task.")(cmd.task_command)
app.command(name="rest", help="Request directly the Opal REST API, for advanced users.")(cmd.rest_command)
app.command(
    name="taxonomy",
    help="Manage taxonomies: list available taxonomies, download, import or delete a taxonomy.",
)(cmd.taxonomy_command)

# Security commands
app.command(name="encrypt", help="Encrypt string using Opal's secret key.")(cmd.encrypt_command)
app.command(name="decrypt", help="Decrypt string using Opal's secret key.")(cmd.decrypt_command)

# SQL commands
app.command(name="sql", help="Execute a SQL statement on project's tables.")(cmd.sql_command)
app.command(
    name="sql_history",
    help="SQL execution history of current user or of other users (administrator only).",
)(cmd.sql_history_command)

# Analysis commands
app.command(
    name="analysis_plugin",
    help="Analyses a project variables using external R plugins.",
)(cmd.analysis_plugin_command)


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
