"""
Opal Entity.
"""

import obiba_opal.core as core


class EntityService:
    """
    Get information about entities.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
        """
        Add variable command specific options
        """
        parser.add_argument('id', help='Identifier of the entity.')
        parser.add_argument('--type', '-ty', required=False, help='Type of the entity. Default type is Participant.')
        parser.add_argument('--tables', '-ta', action='store_true',
                            help='Get the list of tables in which the entity with given identifier exists.')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(self, args):
        """
        Execute data command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            res = None
            if args.tables:
                res = EntityService(client, args.verbose).get_entity_tables(args.id, args.type)
            else:
                res = EntityService(client, args.verbose).get_entity(args.id, args.type)
        
            # format response
            core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_entity(self, id: str, type: str = None) -> dict:
        request = self.client.new_request()
        if self.verbose:
            request.verbose()
        # send request
        response = request.fail_on_error().accept_json().get().resource(self._make_ws(id, type, False)).send()
        return response.from_json()

    def get_entity_tables(self, id: str, type: str = None) -> list:
        request = self.client.new_request()
        if self.verbose:
            request.verbose()
        # send request
        response = request.fail_on_error().accept_json().get().resource(self._make_ws(id, type, True)).send()
        return response.from_json()

    def _make_ws(self, id: str, type: str = None, tables: bool = False):
        """
        Build the web service resource path
        """
        ws = '/entity/%s/type/' % id
        if type:
            ws = ws + type
        else:
            ws = ws + 'Participant'
        if tables:
            ws = ws + '/tables'
        return ws