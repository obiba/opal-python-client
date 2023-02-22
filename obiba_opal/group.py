"""
Opal groups management.
"""

import obiba_opal.core as core


class GroupService:
    """
    Groups management service.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
        """
        Add data command specific options
        """
        parser.add_argument('--name', '-n', required=False,
                            help='Group name.')
        parser.add_argument('--fetch', '-fe', action='store_true', required=False,
                            help='Fetch one or multiple group(s).')
        parser.add_argument('--delete', '-de', action='store_true', required=False,
                            help='Delete a group.')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    @classmethod
    def do_command(self, args):
        """
        Execute group command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        service = GroupService(client, args.verbose)
        try:
            if args.delete:
                service.delete_group(args.name)
            else:
                res = None
                if args.name:
                    res = service.get_group(args.name)
                else:
                    res = service.get_groups()
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_groups(self) -> list:
        """
        Get the groups.
        """
        request = self.client.new_request()
        request.fail_on_error()

        if self.verbose:
            request.verbose()
        response = request.get().resource(self._make_ws()).send()
        return response.from_json()

    def get_group(self, name: str) -> dict:
        """
        Get a specific group.
        
        :param name: The name of the group
        """
        if not name:
            raise ValueError('The name of the group to fetch is required')
        request = self.client.new_request()
        request.fail_on_error()

        if self.verbose:
            request.verbose()
        response = request.get().resource(self._make_ws(name)).send()
        return response.from_json()
    
    def delete_group(self, name: str):
        """
        Delete a specific group (does NOT delete the users of the group).
        
        :param name: The name of the group
        """
        if not name:
            raise ValueError('The name of the group to delete is required')
        request = self.client.new_request()
        request.fail_on_error()

        if self.verbose:
            request.verbose()
        request.delete().resource(self._make_ws(name)).send()

    def _make_ws(self, name: str = None):
        """
        Build the web service resource path
        """
        if name:
            ws = '/system/group/%s' % name
        else:
            ws = '/system/groups'

        return ws

