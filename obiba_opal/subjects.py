"""
Opal user and groups management.
"""

import obiba_opal.core as core
import json


class UserService:
    """
    Users management service.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
        """
        Add data command specific options
        """
        parser.add_argument("--name", "-n", required=False, help="User name.")
        parser.add_argument(
            "--upassword",
            "-upa",
            required=False,
            help="User password of at least 8 characters, must contain at least one digit, one upper case alphabet, one lower case alphabet, one special character (which includes @#$%^&+=!) and no white space.",
        )
        parser.add_argument(
            "--ucertificate",
            "-uc",
            required=False,
            help="User certificate (public key) file",
        )
        parser.add_argument(
            "--disabled",
            "-di",
            action="store_true",
            required=False,
            help="Disable user account (if omitted the user is enabled by default).",
        )
        parser.add_argument(
            "--groups", "-g", nargs="+", required=False, help="User groups"
        )

        parser.add_argument(
            "--fetch",
            "-fe",
            action="store_true",
            required=False,
            help="Fetch one or multiple user(s).",
        )
        parser.add_argument("--add", "-a", action="store_true", help="Add a user.")
        parser.add_argument(
            "--update",
            "-ud",
            action="store_true",
            required=False,
            help="Update a user.",
        )
        parser.add_argument(
            "--delete",
            "-de",
            action="store_true",
            required=False,
            help="Delete a user.",
        )
        parser.add_argument(
            "--json",
            "-j",
            action="store_true",
            help="Pretty JSON formatting of the response",
        )

    @classmethod
    def do_command(self, args):
        """
        Execute group command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = UserService(client, args.verbose)
            if args.add:
                service.add_user(
                    args.name,
                    args.upassword,
                    args.ucertificate,
                    args.groups,
                    args.disabled,
                )
            elif args.update:
                service.update_user(
                    args.name,
                    args.upassword,
                    args.ucertificate,
                    args.groups,
                    args.disabled,
                )
            elif args.delete:
                service.delete_user(args.name)
            else:
                res = None
                if args.name:
                    res = service.get_user(args.name, False)
                else:
                    res = service.get_users()
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_users(self) -> dict:
        """
        Get the users.
        """
        request = self.client.new_request()
        request.fail_on_error()
        if self.verbose:
            request.verbose()
        response = request.get().resource(self._make_ws()).send()
        return response.from_json()

    def get_user(self, name: str, fail_safe: bool = True) -> dict:
        """
        Get a user.

        :param name: The user name
        :param fail_safe: When user is not found, return None (True is default) else raise error
        """
        if not name:
            raise ValueError("The name of the user to fetch is required")
        request = self.client.new_request()
        if self.verbose:
            request.verbose()
        if not fail_safe:
            request.fail_on_error()
        response = request.get().resource(self._make_ws(name)).send()
        return response.from_json() if response.code == 200 else None

    def delete_user(self, name: str):
        """
        Delete a user.

        :param name: The user name
        """
        if not name:
            raise ValueError("The name of the user to delete is required")
        request = self.client.new_request()
        request.fail_on_error()
        if self.verbose:
            request.verbose()
        request.delete().resource(self._make_ws(name)).send()

    def update_user(
        self,
        name: str,
        upassword: str = None,
        ucertificate: str = None,
        groups: list = None,
        disabled: bool = False,
    ):
        """
        Update a user.

        :param name: The user name
        :param upassword: The user password of at least 8 characters, must contain at least one digit, one upper case alphabet, one lower case alphabet, one special character (which includes @#$%^&+=!) and no white space
        :param ucertificate: The user certificate file.
        :param groups: The list of groups
        :param disabled: Not enabled
        """
        if not name:
            raise ValueError("The name of the user to update is required")

        userInfo = self.get_user(name)
        user = {"name": name}

        request = self.client.new_request()
        request.fail_on_error()
        if self.verbose:
            request.verbose()

        if upassword:
            if userInfo["authenticationType"] == "CERTIFICATE":
                raise ValueError(
                    f"{user['name']} requires a certificate (public key) file"
                )
            if len(upassword) < 8:
                raise ValueError("Password must contain at least 8 characters.")
            user["authenticationType"] = "PASSWORD"
            user["password"] = upassword
        elif ucertificate:
            if userInfo["authenticationType"] == "PASSWORD":
                raise ValueError(f"{user['name']} requires a password")

            user["authenticationType"] = "CERTIFICATE"
            with open(ucertificate, "rb") as cert:
                user["certificate"] = cert.read()
        else:
            user["authenticationType"] = userInfo["authenticationType"]

        user["enabled"] = not disabled
        if groups:
            user["groups"] = groups

        request.fail_on_error().accept_json().content_type_json()
        request.put().resource(self._make_ws(name)).content(json.dumps(user)).send()

    def add_user(
        self,
        name: str,
        upassword: str = None,
        ucertificate: str = None,
        groups: list = None,
        disabled: bool = False,
    ):
        """
        Add a user.

        :param name: The user name
        :param upassword: The user password of at least 8 characters, must contain at least one digit, one upper case alphabet, one lower case alphabet, one special character (which includes @#$%^&+=!) and no white space
        :param ucertificate: The user certificate file.
        :param groups: The list of groups
        :param disabled: Not enabled
        """
        if not name:
            raise ValueError("The name of the user to add is required")
        if not upassword and not ucertificate:
            raise ValueError("The user password or a certificate file is required.")

        request = self.client.new_request()
        request.fail_on_error()
        if self.verbose:
            request.verbose()

        # create user
        user = {"name": name}
        if upassword:
            if len(upassword) < 8:
                raise Exception("Password must contain at least 8 characters.")
            user["authenticationType"] = "PASSWORD"
            user["password"] = upassword
        else:
            user["authenticationType"] = "CERTIFICATE"
            with open(ucertificate, "rb") as cert:
                user["certificate"] = cert.read()

        if disabled:
            user["enabled"] = False
        if groups:
            user["groups"] = groups

        request.fail_on_error().accept_json().content_type_json()
        request.post().resource(self._make_ws()).content(json.dumps(user)).send()

    def _make_ws(self, name: str = None):
        """
        Build the web service resource path
        """
        if not name:
            ws = "/system/subject-credentials"
        else:
            ws = f"/system/subject-credential/{name}"
        return ws


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
        parser.add_argument("--name", "-n", required=False, help="Group name.")
        parser.add_argument(
            "--fetch",
            "-fe",
            action="store_true",
            required=False,
            help="Fetch one or multiple group(s).",
        )
        parser.add_argument(
            "--delete",
            "-de",
            action="store_true",
            required=False,
            help="Delete a group.",
        )
        parser.add_argument(
            "--json",
            "-j",
            action="store_true",
            help="Pretty JSON formatting of the response",
        )

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
            raise ValueError("The name of the group to fetch is required")
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
            raise ValueError("The name of the group to delete is required")
        request = self.client.new_request()
        request.fail_on_error()

        if self.verbose:
            request.verbose()
        request.delete().resource(self._make_ws(name)).send()

    def _make_ws(self, name: str = None):
        """
        Build the web service resource path
        """
        ws = f"/system/group/{name}" if name else "/system/groups"

        return ws
