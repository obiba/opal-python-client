"""
Opal system data
"""

import obiba_opal.core as core
from typing import Union
import ast
import json
import sys
import time

class SystemService:
    """
    Get some system information.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add system command specific options
        """
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

        parser.add_argument('--version', action='store_true', required=False,
                            help='Opal version number')
        parser.add_argument('--env', action='store_true', required=False,
                            help='Opal java execution environment (JVM related statistic properties')
        parser.add_argument('--status', action='store_true', required=False,
                            help='Opal application status (JVM related dynamic properties)')
        parser.add_argument('--conf', action='store_true', required=False,
                            help='Opal application configuration')

    @classmethod
    def do_command(cls, args):
        """
        Execute SYSTEM command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            request = client.new_request()
            request.fail_on_error()

            request.accept_json()

            if args.verbose:
                request.verbose()

            # send request
            request.get().resource(cls.do_ws(args))
            response = request.send()

            # format response
            res = response.content.decode('utf-8')

            if args.json:
                res = response.pretty_json()

            # output to stdout
            print(res)
        finally:
            client.close()

    @classmethod
    def do_ws(cls, args):
        """
        Build the web service resource path
        """
        if args.version:
            args.json = False
            return "/system/version"
        if args.env:
            return "/system/env"
        if args.status:
            return "/system/status"
        if args.conf:
            return "/system/conf"
        return "/system/conf/general"


class PluginService:
    """
    Plugins management.
    """

    @classmethod
    def add_arguments(cls, parser):
        """
        Add plugin command specific options
        """

        parser.add_argument('--list', '-ls', action='store_true', help='List the installed plugins.')
        parser.add_argument('--updates', '-lu', action='store_true', help='List the installed plugins that can be updated.')
        parser.add_argument('--available', '-la', action='store_true', help='List the new plugins that could be installed.')
        parser.add_argument('--install', '-i', required=False,
                            help='Install a plugin by providing its name or name:version or a path to a plugin archive file (in Opal file system). If no version is specified, the latest version is installed. Requires system restart to be effective.')
        parser.add_argument('--remove', '-rm', required=False,
                            help='Remove a plugin by providing its name. Requires system restart to be effective.')
        parser.add_argument('--reinstate', '-ri', required=False,
                            help='Reinstate a plugin that was previously removed by providing its name.')
        parser.add_argument('--fetch', '-f', required=False, help='Get the named plugin description.')
        parser.add_argument('--configure', '-c', required=False,
                            help='Configure the plugin site properties. Usually requires to restart the associated service to be effective.')
        parser.add_argument('--status', '-su', required=False,
                            help='Get the status of the service associated to the named plugin.')
        parser.add_argument('--start', '-sa', required=False, help='Start the service associated to the named plugin.')
        parser.add_argument('--stop', '-so', required=False, help='Stop the service associated to the named plugin.')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute plugin command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            request = client.new_request()
            request.fail_on_error().accept_json()

            if args.verbose:
                request.verbose()

            if args.list:
                response = request.get().resource('/plugins').send()
            elif args.updates:
                response = request.get().resource('/plugins/_updates').send()
            elif args.available:
                response = request.get().resource('/plugins/_available').send()
            elif args.install:
                if args.install.startswith('/'):
                    response = request.post().resource('/plugins?file=' + args.install).send()
                else:
                    nameVersion = args.install.split(':')
                    if len(nameVersion) == 1:
                        response = request.post().resource('/plugins?name=' + nameVersion[0]).send()
                    else:
                        response = request.post().resource(
                            '/plugins?name=' + nameVersion[0] + '&version=' + nameVersion[1]).send()
            elif args.fetch:
                response = request.get().resource('/plugin/' + args.fetch).send()
            elif args.configure:
                request.content_type_text_plain()
                print('Enter plugin site properties (one property per line, Ctrl-D to end input):')
                request.content(sys.stdin.read())
                response = request.put().resource('/plugin/' + args.configure + '/cfg').send()
            elif args.remove:
                response = request.delete().resource('/plugin/' + args.remove).send()
            elif args.reinstate:
                response = request.put().resource('/plugin/' + args.reinstate).send()
            elif args.status:
                response = request.get().resource('/plugin/' + args.status + '/service').send()
            elif args.start:
                response = request.put().resource('/plugin/' + args.start + '/service').send()
            elif args.stop:
                response = request.delete().resource('/plugin/' + args.stop + '/service').send()

            # format response
            res = response.content.decode('utf-8')
            if args.json:
                res = response.pretty_json()

            # output to stdout
            print(res)
        finally:
            client.close()


class TaxonomyService:
    """
    Taxonomies management.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
      self.client = client
      self.verbose = verbose

    def __make_request(self):
        request = self.client.new_request()
        request.fail_on_error()
        request.accept_json()

        if self.verbose:
            request.verbose()

        return request

    def download(self, name: str):
        resource = self.OpalTaxonomyResource(name)
        request = self.__make_request().get().resource(resource.get_download_ws()).accept('text/plain')

        return request.send()

    def importFile(self, file: str, override: bool = False):
        uri = core.UriBuilder(['system', 'conf', 'taxonomies', 'import', '_file']) \
          .query('file', file) \
          .query('override', str(override).lower()) \
          .build()
        return self.__make_request() \
          .post() \
          .resource(uri) \
          .send()

    def delete(self, name: str):
        return self.__make_request().resource(self.OpalTaxonomyResource(name).get_ws()).delete().send()

    def confirmAndDelete(self, name: str, rejectHandler):
        confirmed = input('Delete the taxonomy {}? [y/N]: '.format(name))
        if confirmed == 'y':
            return self.delete(name)

        return rejectHandler()

    def summaries(self):
        return self.__make_request().get().resource('/system/conf/taxonomies/summaries').send()

    @classmethod
    def add_arguments(cls, parser):
        """
        Add file command specific options
        """
        parser.add_argument('--download', '-dl', required=False, help='Download a taxonomy by name (YAML format).')
        parser.add_argument('--import-file', '-if', required=False, help='Import a taxonomy from the provided Opal file path (YAML format).')
        parser.add_argument('--delete', '-dt', required=False, help='Delete a taxonomy by name.')
        parser.add_argument('--force', '-f', action='store_true', help='Skip confirmation.')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute taxonomy command
        """

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = TaxonomyService(client, args.verbose)
            # send request
            if args.download:
                response = service.download(args.download)
            elif args.import_file:
                response = service.importFile(args.import_file)
            elif args.delete:
                taxo = cls.OpalTaxonomyResource(args.delete)
                # confirm
                if args.force:
                    response = service.delete(args.delete)
                else:
                    def rejectHandler():
                        print('Aborted.')
                        sys.exit(0)

                    response = service.confirmAndDelete(args.delete, rejectHandler)
            else:
                response = service.summaries()

            # format response
            if args.json and not args.download and not args.delete and not args.import_file:
                print(response.pretty_json())
            else:
              # output to stdout as string
              print(response)

        finally:
            client.close()

    class OpalTaxonomyResource:
        """
        Taxonomy reference
        """

        def __init__(self, name):
            self.name = name

        def get_ws(self):
            return '/system/conf/taxonomy/' + self.name

        def get_download_ws(self):
            return '/system/conf/taxonomy/' + self.name + '/_download'


class TaskService:
    """
    Tasks management.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add task command specific options
        """
        parser.add_argument('--id', required=False,
                            help='The task ID. If not provided, it will be read from the standard input (from the JSON representation of the task or a plain value).')
        parser.add_argument('--show', '-sh', action='store_true', help='Show JSON representation of the task')
        parser.add_argument('--status', '-st', action='store_true', help='Get the status of the task')
        parser.add_argument('--wait', '-w', action='store_true', help='Wait for the task to complete (successfully or not)')
        parser.add_argument('--cancel', '-c', action='store_true', help='Cancel the task')
        parser.add_argument('--delete', '-d', action='store_true', help='Delete the task')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute task command
        """
        # Build and send request
        # Extract task identifier from stdin: can be the ID or the task in JSON
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        service = TaskService(client, args.verbose)
        try:
            if not args.id:
                id = sys.stdin.read().strip('\n')
                if id.startswith('{'):
                    id = str(json.loads(id)['id'])
                args.id = id

            if args.show or not (args.show or args.wait or args.status or args.cancel or args.delete):
                res = service.get_task(args.id)
                core.Formatter.print_json(res, args.json)
            if args.wait:
                status = service.wait_task(args.id)
                print('\r\033[K' + status)
            if args.status:
                print(service.get_task(args.id)['status'])
            if args.cancel:
                service.cancel_task(args.id)
            if args.delete:
                service.delete_task(args.id)
        finally:
            client.close()

    def get_task(self, id: Union[str, int]):
        request = self._make_request()
        request.get().resource('/shell/command/%s' % id)
        response = request.send()
        return response.from_json()

    def delete_task(self, id: Union[str, int]):
        request = self._make_request()
        request.delete().resource('/shell/command/%s' % id).send()

    def cancel_task(self, id: Union[str, int]):
        request = self._make_request().content_type_text_plain()
        request.content('CANCELED')
        request.put().resource('/shell/command/%s/status' % id).send()

    def wait_task(self, id: Union[str, int]):
        task = self.get_task(id)
        while task['status'] not in ['SUCCEEDED', 'CANCELED', 'FAILED']:
            if 'progress' in task:
                progress = task['progress']
                if 'message' in progress:
                    sys.stdout.write('\r\033[K' + str(progress['percent']) + '% ' + progress['message'])
                else:
                    sys.stdout.write('\r\033[K' + str(progress['percent']) + '%')
            else:
                sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(1)
            task = self.get_task(id)
        return task['status']

    def _make_request(self):
        request = self.client.new_request()
        request.fail_on_error()
        request.accept_json()
        if self.verbose:
            request.verbose()
        return request


class RESTService:
    """
    Perform raw web services requests.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    def make_request(self, method: str, accept: str = None, headers = None):
        request = self.client.new_request()
        request.method(method)
        request.fail_on_error()

        if accept:
            request.accept(accept)
        else:
            request.accept_json()

        if headers:
            headers = ast.literal_eval(headers)
            for key in list(headers.keys()):
                request.header(key, headers[key])

        if self.verbose:
            request.verbose()
        return request

    def make_request_with_content_type(self, method: str, contentType: str, accept: str = None, headers = None, content: str = None):
        request = self.make_request(method, accept, headers)

        if contentType:
          request.content_type(contentType)

          if content is not None:
              request.content(content)
          else:
            print('Enter content:')
            request.content(sys.stdin.read())

        return request

    def send_request(self, url: str, request: core.OpalRequest):
        return request.resource(url).send()

    @classmethod
    def add_arguments(cls, parser):
        """
        Add REST command specific options
        """
        parser.add_argument('ws', help='Web service path, for instance: /datasource/xxx/table/yyy/variable/vvv')
        parser.add_argument('--method', '-m', required=False,
                            help='HTTP method (default is GET, others are POST, PUT, DELETE, OPTIONS)')
        parser.add_argument('--accept', '-a', required=False, help='Accept header (default is application/json)')
        parser.add_argument('--content-type', '-ct', required=False,
                            help='Content-Type header (default is application/json)')
        parser.add_argument('--headers', '-hs', required=False,
                            help='Custom headers in the form of: { "Key2": "Value2", "Key2": "Value2" }')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(cls, args):
        """
        Execute REST command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        service = RESTService(client, args.verbose)
        method = args.method if args.method else 'GET'

        try:
            if method in ['POST', 'PUT']:
              request = service.make_request_with_content_type(args.method, args.content_type, args.accept, args.headers)
            else:
              request = service.make_request(args.method, args.accept, args.headers)

            # format response
            response = service.send_request(args.ws, request)
            res = response.content.decode('utf-8')

            if args.json:
                res = response.pretty_json()
            elif args.method in ['OPTIONS']:
                res = response.headers['Allow']

            # output to stdout
            print(res)
        finally:
            client.close()
