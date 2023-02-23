"""
Opal data importer
"""

import json
import obiba_opal.core as core
import re


def add_import_arguments(parser):
    """
    Add Default Import arguments
    """
    parser.add_argument('--destination', '-d', required=True, help='Destination datasource name')
    parser.add_argument('--tables', '-t', nargs='+', required=False,
                        help='The list of tables to be imported (defaults to all)')
    parser.add_argument('--incremental', '-i', action='store_true',
                        help='Incremental import (new and updated value sets)')
    parser.add_argument('--limit', '-li', required=False, type=int, help='Import limit (maximum number of value sets)')
    parser.add_argument('--identifiers', '-id', required=False, help='Name of the ID mapping')
    parser.add_argument('--policy', '-po', required=False,
                        help='ID mapping policy: "required" (each identifiers must be mapped prior importation, default), "ignore" (ignore unknown identifiers), "generate" (generate a system identifier for each unknown identifier)')
    parser.add_argument('--merge', '-mg', action='store_true',
                        help='Merge imported data dictionary with the destination one (default is false, i.e. data dictionary is overridden).')
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')


class OpalImporter:
    """
    OpalImporter takes care of submitting an import job.
    """

    class ExtensionFactoryInterface:
        def add(self, factory):
            raise Exception("ExtensionFactoryInterface.add() method must be implemented by a concrete class.")

    @classmethod
    def build(cls, client: core.OpalClient, destination: str, tables: list = None, incremental: bool = None, limit: int = None, identifiers: str =None, policy: str = None,
              merge: bool = None, verbose: bool = False):
        setattr(cls, 'client', client)
        setattr(cls, 'destination', destination)
        setattr(cls, 'tables', tables)
        setattr(cls, 'incremental', incremental)
        setattr(cls, 'limit', limit)
        setattr(cls, 'identifiers', identifiers)
        setattr(cls, 'policy', policy)
        setattr(cls, 'merge', merge)
        setattr(cls, 'verbose', verbose)
        return cls()

    def submit(self, extension_factory) -> core.OpalResponse:
        """
        Build a specific transient datasource, using extension_factory, and submit import job.
        """
        transient = self.__create_transient_datasource(extension_factory)

        # submit data import job
        request = self.client.new_request()
        request.fail_on_error().accept_json().content_type_json()

        if self.verbose:
            request.verbose()

        # import options
        options = {'destination': self.destination}
        # tables must be the ones of the transient
        tables2import = transient['table']
        if self.tables:
            def f(t): return any(t in s for s in transient['table'])

            tables2import = list(filter(f, self.tables))

        def table_fullname(t):
            return transient['name'] + '.' + t

        options['tables'] = list(map(table_fullname, tables2import))

        if self.identifiers:
            options['idConfig'] = {'name': self.identifiers}
            if self.policy:
                if self.policy == 'ignore':
                    options['idConfig']['allowIdentifierGeneration'] = False
                    options['idConfig']['ignoreUnknownIdentifier'] = True
                elif self.policy == 'generate':
                    options['idConfig']['allowIdentifierGeneration'] = True
                    options['idConfig']['ignoreUnknownIdentifier'] = False
                else:
                    options['idConfig']['allowIdentifierGeneration'] = False
                    options['idConfig']['ignoreUnknownIdentifier'] = False
            else:
                options['idConfig']['allowIdentifierGeneration'] = False
                options['idConfig']['ignoreUnknownIdentifier'] = False

        if self.verbose:
            print("** Import options:")
            print(options)
            print("**")

        uri = core.UriBuilder(['project', self.destination, 'commands', '_import']).build()
        response = request.post().resource(uri).content(json.dumps(options)).send()

        # get job status
        location = None
        if 'Location' in response.headers:
            location = response.headers['Location']
        elif 'location' in response.headers:
            location = response.headers['location']
        job_resource = re.sub(r'http.*\/ws', r'', location)
        request = self.client.new_request()
        request.fail_on_error().accept_json()
        return request.get().resource(job_resource).send()

    def __create_transient_datasource(self, extension_factory, ):
        """
        Create a transient datasource
        """
        request = self.client.new_request()
        request.fail_on_error().accept_json().content_type_json()

        if self.verbose:
            request.verbose()

        # build transient datasource factory
        factory = {}
        if self.incremental:
            factory['incrementalConfig'] = {
                'incremental': True,
                'incrementalDestinationName': self.destination
            }
        if self.limit:
            factory['batchConfig'] = {'limit': self.limit}
        if self.identifiers:
            factory['idConfig'] = {'name': self.identifiers}
            if self.policy:
                if self.policy == 'ignore':
                    factory['idConfig']['allowIdentifierGeneration'] = False
                    factory['idConfig']['ignoreUnknownIdentifier'] = True
                elif self.policy == 'generate':
                    factory['idConfig']['allowIdentifierGeneration'] = True
                    factory['idConfig']['ignoreUnknownIdentifier'] = False
                else:
                    factory['idConfig']['allowIdentifierGeneration'] = False
                    factory['idConfig']['ignoreUnknownIdentifier'] = False
            else:
                factory['idConfig']['allowIdentifierGeneration'] = False
                factory['idConfig']['ignoreUnknownIdentifier'] = False

        extension_factory.add(factory)

        if self.verbose:
            print("** Datasource factory:")
            print(factory)
            print("**")

        # send request and parse response as a datasource
        mergeStr = 'false'
        if self.merge:
            mergeStr = 'true'
        uri = core.UriBuilder(['project', self.destination, 'transient-datasources']).query('merge',
                                                                                                 mergeStr).build()
        response = request.post().resource(uri).content(json.dumps(factory)).send()
        transient = json.loads(response.content)

        if self.verbose:
            print("** Transient datasource:")
            print(transient)
            print("**")
        return transient

    def compare_datasource(self, transient):
        # Compare datasources : /datasource/<transient_name>/compare/<ds_name>
        uri = core.UriBuilder(['datasource',
                                    transient['name'].encode('ascii', 'ignore'),
                                    'compare', self.destination]).build()
        request = self.client.new_request()
        request.fail_on_error().accept_json().content_type_json()
        if self.verbose:
            request.verbose()
        response = request.get().resource(uri).send()
        compare = json.loads(response.content)
        for i in compare['tableComparisons']:
            if i['conflicts']:
                all_conflicts = []
                for c in i['conflicts']:
                    all_conflicts.append(c['code'] + "(" + ', '.join(c['arguments']) + ")")

                raise Exception("Import conflicts: " + '; '.join(all_conflicts))


class OpalExporter:
    """
    OpalExporter takes care of submitting an export job.
    """

    @classmethod
    def build(cls, client: core.OpalClient, datasource: str, tables: list, output: str, incremental: bool = False, multilines: bool = True, identifiers: str = None,
              entityIdNames = None, verbose: bool = False):
        setattr(cls, 'client', client)
        setattr(cls, 'datasource', datasource)
        setattr(cls, 'tables', tables)
        setattr(cls, 'output', output)
        setattr(cls, 'incremental', incremental)
        setattr(cls, 'identifiers', identifiers)
        setattr(cls, 'multilines', multilines)
        setattr(cls, 'entityIdNames', entityIdNames)
        setattr(cls, 'verbose', verbose)
        return cls()

    def setClient(self, client):
        self.client = client
        return self

    def submit(self, format) -> core.OpalResponse:
        # export options
        options = {
            'format': format,
            'out': self.output,
            'nonIncremental': not self.incremental,
            'multilines': self.multilines,
            'noVariables': False
        }
        if self.entityIdNames:
            options['entityIdNames'] = self.entityIdNames

        if self.tables:
            tables2export = self.tables

            def table_fullname(t): return self.datasource + '.' + t

            options['tables'] = list(map(table_fullname, tables2export))
        if self.identifiers:
            options['idConfig'] = {
                'name': self.identifiers,
                'allowIdentifierGeneration': False,
                'ignoreUnknownIdentifier': False
            }
        if self.verbose:
            print("** Export options:")
            print(options)
            print("**")

        # submit data export job
        request = self.client.new_request()
        request.fail_on_error().accept_json().content_type_json()

        if self.verbose:
            request.verbose()

        uri = core.UriBuilder(['project', self.datasource, 'commands', '_export']).build()
        response = request.post().resource(uri).content(json.dumps(options)).send()

        # get job status
        location = None
        if 'Location' in response.headers:
            location = response.headers['Location']
        elif 'location' in response.headers:
            location = response.headers['location']
        job_resource = re.sub(r'http.*\/ws', r'', location)
        request = self.client.new_request()
        request.fail_on_error().accept_json()
        return request.get().resource(job_resource).send()


class OpalCopier:
    """
    OpalCopier takes care of submitting a copy job.
    """

    @classmethod
    def build(cls, client, datasource, tables, destination, name, incremental=False, nulls=False, verbose=None):
        setattr(cls, 'client', client)
        setattr(cls, 'datasource', datasource)
        setattr(cls, 'tables', tables)
        setattr(cls, 'destination', destination)
        setattr(cls, 'name', name)
        setattr(cls, 'incremental', incremental)
        setattr(cls, 'nulls', nulls)
        setattr(cls, 'verbose', verbose)
        return cls()

    def setClient(self, client):
        self.client = client
        return self

    def submit(self):
        # copy options
        options = {
            'destination': self.destination,
            'nonIncremental': not self.incremental,
            'noVariables': False,
            'noValues': False,
            'copyNullValues': self.nulls,
            'tables': []
        }
        if self.tables:
            tables2copy = self.tables

            def table_fullname(t): return self.datasource + '.' + t

            options['tables'] = list(map(table_fullname, tables2copy))
        # name option will be ignored if more than one table
        if self.name:
            options.destinationTableName = self.name

        if self.verbose:
            print("** Copy options:")
            print(options)
            print("**")

        # submit data copy job
        request = self.client.new_request()
        request.fail_on_error().accept_json().content_type_json()

        if self.verbose:
            request.verbose()

        uri = core.UriBuilder(['project', self.datasource, 'commands', '_copy']).build()
        response = request.post().resource(uri).content(json.dumps(options)).send()

        # get job status
        location = None
        if 'Location' in response.headers:
            location = response.headers['Location']
        elif 'location' in response.headers:
            location = response.headers['location']
        job_resource = re.sub(r'http.*\/ws', r'', location)
        request = self.client.new_request()
        request.fail_on_error().accept_json()
        return request.get().resource(job_resource).send()
