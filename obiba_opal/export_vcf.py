"""
Export some VCF/BCF files.
"""

import json
import obiba_opal.core as core


class ExportVCFCommand:

    @classmethod
    def add_arguments(self, parser):
        """
        Add command specific options
        """
        parser.add_argument('--project', '-pr', required=True,
                            help='Project name from which genotypes data will be exported')
        parser.add_argument('--vcf', '-vcf', nargs='+', required=True, help='List of VCF/BCF file names')
        parser.add_argument('--destination', '-d', required=True, help='Destination folder (in Opal file system)')
        parser.add_argument('--filter-table', '-f', required=False,
                            help='Participant table name to be used to filter the samples by participant ID (only relevant if there is a sample-participant mapping defined)')
        parser.add_argument('--no-case-controls', '-nocc', action='store_true',
                            help='Do not include case control samples (only relevant if there is a sample-participant mapping defined)')

    @classmethod
    def do_command(self, args):
        """
        Execute delete command
        """
        # Build and send requests
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            request = client.new_request()
            request.fail_on_error().accept_json().content_type_json()
            if args.verbose:
                request.verbose()

            options = {
                'project': args.project,
                'names': args.vcf,
                'destination': args.destination,
                'caseControl': not args.no_case_controls
            }
            if args.filter_table:
                options['table'] = args.filter_table

            # send request
            uri = core.UriBuilder(['project', args.project, 'commands', '_export_vcf']).build()
            request.resource(uri).post().content(json.dumps(options)).send()
        finally:
            client.close()