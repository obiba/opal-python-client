"""
Opal permissions
"""

import obiba_opal.core as core

class PermService:
    """
    Base class for permissions management.
    """

    SUBJECT_TYPES = ('USER', 'GROUP')

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def _add_permission_arguments(self, parser, permissions: list):
        """
        Add permission arguments
        """
        parser.add_argument('--fetch', '-f', action='store_true', required=False, help='Fetch permissions')
        parser.add_argument('--add', '-a', action='store_true', required=False, help='Add a permission')
        parser.add_argument('--delete', '-d', action='store_true', required=False, help='Delete a permission')
        parser.add_argument('--permission', '-pe', help="Permission to apply: %s" % ', '.join(permissions))
        parser.add_argument('--subject', '-s', required=False, help='Subject name to which the permission will be granted/removed (required on add/delete)')
        parser.add_argument('--type', '-ty', required=True, help='Subject type: user or group')
        parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')

    def _map_permission(self, permission: str, permissions: dict):
        """
        Map permission argument to permission query parameter
        """
        if permission.lower() not in list(permissions.keys()):
            return None

        return permissions[permission.lower()]

    @classmethod
    def _validate_args(self, args, permissions):
        """
        Validate action, permission and subject type
        """
        if args.add:
            if not args.subject:
                raise ValueError("The subject name is required")
            if not args.permission:
                raise ValueError("A permission name is required: %s" % ', '.join(list(permissions.keys())))
            if self._map_permission(args.permission, permissions) is None:
                raise ValueError("Valid permissions are: %s" % ', '.join(list(permissions.keys())))

        if args.delete:
            if not args.subject:
                raise ValueError("The subject name is required")
        
        if not args.type or args.type.upper() not in self.SUBJECT_TYPES:
            raise ValueError("Valid subject types are: %s" % ', '.join(self.SUBJECT_TYPES).lower())

    def _make_add_ws(self, path: list, subject: str, type: str, permission: str, permissions: dict):
        return core.UriBuilder(path) \
            .query('type', type.upper()) \
            .query('permission', self._map_permission(permission, permissions)) \
            .query('principal', subject) \
            .build()
    
    def _make_delete_ws(self, path: list, subject: str, type: str = 'user'):
        return core.UriBuilder(path) \
            .query('type', type.upper()) \
            .query('principal', subject) \
            .build()
    
    def _make_get_ws(self, path: list, type: str = 'user'):
        return core.UriBuilder(path) \
            .query('type', type.upper()) \
            .build()
    
    def _make_request(self, fail_safe: bool = False):
        request = self.client.new_request()
        if not fail_safe:
            request.fail_on_error()
        if self.verbose:
            request.verbose()
        return request


class ProjectPermService(PermService):
    """
    Project permissions management.
    """

    PERMISSIONS = {
        'administrate': 'PROJECT_ALL'
    }

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        super().__init__(client, verbose)

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        cls._add_permission_arguments(parser, list(cls.PERMISSIONS.keys()))
        parser.add_argument('--project', '-pr', required=True, help='Project name')

    @classmethod
    def do_command(cls, args):
        """
        Execute permission command
        """
        # Build and send requests
        cls._validate_args(args, cls.PERMISSIONS)

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = ProjectPermService(client, args.verbose)
            # send request
            if args.delete:
                service.delete_perm(args.project, args.subject, args.type)
            elif args.add:
                service.add_perm(args.project, args.subject, args.type, args.permission)
            else:
                res = service.get_perms(args.project, args.type)
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()
    
    def get_perms(self, project: str, type: str) -> list:
        """
        Get the project permissions.

        :param project: The project name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        response = request.get().resource(
                        self._make_get_ws(['project', project, 'permissions', 'project'], type)).send()
        return response.from_json()
    
    def delete_perm(self, project: str, subject: str, type: str):
        """
        Delete project level permissions.

        :param project: The project name
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        request.delete().resource(
                self._make_delete_ws(['project', project, 'permissions', 'project'], subject, type)).send()

    def add_perm(self, project: str, subject: str, type: str, permission: str):
        """
        Add project level permissions.

        :param project: The project name
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        :param permission: The permission
        """
        request = self._make_request()
        request.post().resource(
                self._make_add_ws(['project', project, 'permissions', 'project'], subject, type, permission, self.PERMISSIONS)).send()
    

class DatasourcePermService(PermService):
    """
    Project's datasource permissions management.
    """

    PERMISSIONS = {
        'view-value': 'DATASOURCE_VIEW',
        'add-table': 'TABLE_ADD',
        'administrate': 'DATASOURCE_ALL'
    }
    
    def __init__(self, client: core.OpalClient, verbose: bool = False):
        super().__init__(client, verbose)

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        cls._add_permission_arguments(parser, list(cls.PERMISSIONS.keys()))
        parser.add_argument('--project', '-pr', required=True, help='Project name to which the tables belong')

    @classmethod
    def do_command(cls, args):
        """
        Execute permission command
        """
        # Build and send requests
        cls._validate_args(args, cls.PERMISSIONS)

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = DatasourcePermService(client, args.verbose)
            # send request
            if args.delete:
                service.delete_perm(args.project, args.subject, args.type)
            elif args.add:
                service.add_perm(args.project, args.subject, args.type, args.permission)
            else:
                res = service.get_perms(args.project, args.type)
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_perms(self, project: str, type: str) -> list:
        """
        Get the project's datasource permissions.

        :param project: The project name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        response = request.get().resource(
                        self._make_get_ws(['project', project, 'permissions', 'datasource'], type)).send()
        return response.from_json()
    
    def delete_perm(self, project: str, subject: str, type: str):
        """
        Delete project's datasource level permissions.

        :param project: The project name
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        request.delete().resource(
                self._make_delete_ws(['project', project, 'permissions', 'datasource'], subject, type)).send()

    def add_perm(self, project: str, subject: str, type: str, permission: str):
        """
        Add project's datasource level permissions.

        :param project: The project name
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        :param permission: The permission
        """
        request = self._make_request()
        request.post().resource(
                self._make_add_ws(['project', project, 'permissions', 'datasource'], subject, type, permission, self.PERMISSIONS)).send()


class TablePermService(PermService):
    """
    Project tables permissions management.
    """

    PERMISSIONS = {
        'view': 'TABLE_READ',
        'view-value': 'TABLE_VALUES',
        'edit': 'TABLE_EDIT',
        'edit-values': 'TABLE_VALUES_EDIT',
        'administrate': 'TABLE_ALL'
    }

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        super().__init__(client, verbose)

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        cls._add_permission_arguments(parser, list(cls.PERMISSIONS.keys()))
        parser.add_argument('--project', '-pr', required=True, help='Project name to which the tables belong')
        parser.add_argument('--tables', '-t', nargs='+', required=False,
                            help='List of table names on which the permission is to be get/set (default is all)')

    @classmethod        
    def do_command(cls, args):
        """
        Execute permission command
        """
        # Build and send requests
        cls._validate_args(args, cls.PERMISSIONS)

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = TablePermService(client, args.verbose)

            # send request
            if args.delete:
                service.delete_perms(args.project, args.tables, args.subject, args.type)
            elif args.add:
                service.add_perms(args.project, args.tables, args.subject, args.type, args.permission)
            else:
                res = []
                for table in service._ensure_tables(args.project, args.tables):
                    res = res + service.get_perms(args.project, table, args.type)
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_perms(self, project: str, table: str, type: str) -> list:
        """
        Get the table permissions.
        
        :param project: The project name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        response = request.get().resource(
                        self._make_get_ws(['project', project, 'permissions', 'table', table], type)).send()
        return response.from_json()

    def delete_perms(self, project: str, tables: list, subject: str, type: str):
        """
        Delete project's tables level permissions.

        :param project: The project name
        :param tables: The table names (all if empty)
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        """
        tables_ = self._ensure_tables(project, tables)
        for table in tables_:
            self.delete_perm(project, table, subject, type)

    def delete_perm(self, project: str, table: str, subject: str, type: str):
        """
        Delete project's table level permissions.

        :param project: The project name
        :param table: The table name
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        request.delete().resource(
                self._make_delete_ws(['project', project, 'permissions', 'table', table], subject, type)).send()

    def add_perms(self, project: str, tables: list, subject: str, type: str):
        """
        Add project's tables level permissions.

        :param project: The project name
        :param tables: The table names (all if empty)
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        :param permission: The permission
        """
        tables_ = self._ensure_tables(project, tables)
        for table in tables_:
            self.add_perm(project, table, subject, type)

    def add_perm(self, project: str, table: str, subject: str, type: str, permission: str):
        """
        Add project's table level permissions.

        :param project: The project name
        :param table: The table name
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        :param permission: The permission
        """
        request = self._make_request()
        request.post().resource(
                self._make_add_ws(['project', project, 'permissions', 'table', table], subject, type, permission, self.PERMISSIONS)).send()
    
    def _ensure_tables(self, project: str, tables: list) -> list:
        """
        Get the table names of the project's datasource if none is specified.
        """
        if not tables:
            request = self._make_request()
            res = request.get().resource(core.UriBuilder(['datasource', project, 'tables']).build()).send().from_json()
            return [x['name'] for x in res]
        else:
            return tables


class VariablePermService(PermService):
    """
    Project table variables permissions management.
    """

    PERMISSIONS = {
        'view': 'VARIABLE_READ'
    }

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        super().__init__(client, verbose)

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        cls._add_permission_arguments(parser, list(cls.PERMISSIONS.keys()))
        parser.add_argument('--project', '-pr', required=True, help='Project name to which the tables belong')
        parser.add_argument('--table', '-t', required=True, help='Table name to which the variables belong')
        parser.add_argument('--variables', '-va', nargs='+', required=False,
                            help='List of variable names on which the permission is to be get/set (default is all)')

    @classmethod        
    def do_command(cls, args):
        """
        Execute permission command
        """
        # Build and send requests
        cls._validate_args(args, cls.PERMISSIONS)

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = VariablePermService(client, args.verbose)

            # send request
            if args.delete:
                service.delete_perms(args.project, args.table, args.variables, args.subject, args.type)
            elif args.add:
                service.add_perms(args.project, args.table, args.variables, args.subject, args.type, args.permission)
            else:
                res = []
                for variable in service._ensure_variables(args.project, args.table, args.variables):
                    res = res + service.get_perms(args.project, args.table, variable, args.type)
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_perms(self, project: str, table: str, variable: str, type: str) -> list:
        """
        Get the project's table variable permissions.

        :param project: The project name
        :param table: The table name
        :param variable: The variable name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        response = request.get().resource(
                        self._make_get_ws(['project', project, 'permissions', 'table', table, 'variable', variable], type)).send()
        return response.from_json()
    
    def delete_perms(self, project: str, table: str, variables: list, subject: str, type: str):
        """
        Delete project's table variables level permissions.

        :param project: The project name
        :param table: The table name
        :param variables: The variable names (all if empty)
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        """
        variables_ = self._ensure_variables(project, table, variables)
        for variable in variables_:
            self.delete_perm(project, table, variable, subject, type)

    def delete_perm(self, project: str, table: str, variable: str, subject: str, type: str):
        """
        Delete project's table variable level permissions.

        :param project: The project name
        :param table: The table name
        :param variable: The variable name
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        request.delete().resource(
                self._make_delete_ws(['project', project, 'permissions', 'table', table, 'variable', variable], subject, type)).send()

    def add_perms(self, project: str, table: str, variables: list, subject: str, type: str):
        """
        Add project's table variables level permissions.

        :param project: The project name
        :param table: The table name
        :param variables: The variable names (all if empty)
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        :param permission: The permission
        """
        variables_ = self._ensure_variables(project, table, variables)
        for variable in variables_:
            self.add_perm(project, table, variable, subject, type)

    def add_perm(self, project: str, table: str, variable: str, subject: str, type: str, permission: str):
        """
        Add project's table variable level permissions.

        :param project: The project name
        :param table: The table name
        :param variable: The variable name
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        :param permission: The permission
        """
        request = self._make_request()
        request.post().resource(
                self._make_add_ws(['project', project, 'permissions', 'table', table, 'variable', variable], subject, type, permission, self.PERMISSIONS)).send()
    
    def _ensure_variables(self, project: str, table: str, variables: list) -> list:
        """
        Get the table's variable names of the project's datasource if none is specified.
        """
        if not variables:
            request = self._make_request()
            res = request.get().resource(core.UriBuilder(['datasource', project, 'table', table, 'variables']).build()).send().from_json()
            return [x['name'] for x in res]
        else:
            return variables


class ResourcePermService(PermService):
    """
    Project resource permissions management.
    """

    PERMISSIONS = {
        'view': 'RESOURCE_VIEW',
        'administrate': 'RESOURCE_ALL'
    }

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        super().__init__(client, verbose)

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        cls._add_permission_arguments(parser, list(cls.PERMISSIONS.keys()))
        parser.add_argument('--project', '-pr', required=True, help='Project name to which the resources belong')
        parser.add_argument('--resources', '-r', nargs='+', required=False,
                            help='List of resource names on which the permission is to be get/set (default is all)')
    
    @classmethod        
    def do_command(cls, args):
        """
        Execute permission command
        """
        # Build and send requests
        cls._validate_args(args, cls.PERMISSIONS)

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = ResourcePermService(client, args.verbose)

            # send request
            if args.delete:
                service.delete_perms(args.project, args.resources, args.subject, args.type)
            elif args.add:
                service.add_perms(args.project, args.resources, args.subject, args.type, args.permission)
            else:
                res = []
                for resource in service._ensure_resources(args.project, args.resources):
                    res = res + service.get_perms(args.project, resource, args.type)
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_perms(self, project: str, resource: str, type: str) -> list:
        """
        Get the project's resource permissions.

        :param project: The project name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        response = request.get().resource(
                        self._make_get_ws(['project', project, 'permissions', 'resource', resource], type)).send()
        return response.from_json()
    
    def delete_perms(self, project: str, resources: list, subject: str, type: str):
        """
        Delete project's resources level permissions.

        :param project: The project name
        :param resources: The resource names (all if empty)
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        """
        resources_ = self._ensure_resources(project, resources)
        for resource in resources_:
            self.delete_perm(project, resource, subject, type)

    def delete_perm(self, project: str, resource: str, subject: str, type: str):
        """
        Delete project's resource level permissions.

        :param project: The project name
        :param resource: The resource name
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        request.delete().resource(
                self._make_delete_ws(['project', project, 'permissions', 'resource', resource], subject, type)).send()

    def add_perms(self, project: str, resources: list, subject: str, type: str):
        """
        Add project's resources level permissions.

        :param project: The project name
        :param resources: The resource names (all if empty)
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        :param permission: The permission
        """
        resources_ = self._ensure_resources(project, resources)
        for resource in resources_:
            self.add_perm(project, resource, subject, type)

    def add_perm(self, project: str, resource: str, subject: str, type: str, permission: str):
        """
        Add project's resource level permissions.

        :param project: The project name
        :param resource: The resource name
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        :param permission: The permission
        """
        request = self._make_request()
        request.post().resource(
                self._make_add_ws(['project', project, 'permissions', 'resource', resource], subject, type, permission, self.PERMISSIONS)).send()
    
    def _ensure_resources(self, project: str, resources: list) -> list:
        """
        Get the resource names of the project if none is specified.
        """
        if not resources:
            request = self._make_request()
            res = request.get().resource(core.UriBuilder(['project', project, 'resources']).build()).send().from_json()
            return [x['name'] for x in res]
        else:
            return resources


class ResourcesPermService(PermService):
    """
    Project resources permissions management.
    """

    PERMISSIONS = {
        'view': 'RESOURCES_VIEW',
        'administrate': 'RESOURCES_ALL'
    }

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        super().__init__(client, verbose)

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        cls._add_permission_arguments(parser, list(cls.PERMISSIONS.keys()))
        parser.add_argument('--project', '-pr', required=True, help='Project name to which the resources belong')

    @classmethod
    def do_command(cls, args):
        """
        Execute permission command
        """
        # Build and send requests
        cls._validate_args(args, cls.PERMISSIONS)

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = ResourcesPermService(client, args.verbose)
            # send request
            if args.delete:
                service.delete_perm(args.project, args.subject, args.type)
            elif args.add:
                service.add_perm(args.project, args.subject, args.type, args.permission)
            else:
                res = service.get_perms(args.project, args.type)
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_perms(self, project: str, type: str) -> list:
        """
        Get the project's resources permissions.

        :param project: The project name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        response = request.get().resource(
                        self._make_get_ws(['project', project, 'permissions', 'resources'], type)).send()
        return response.from_json()
    
    def delete_perm(self, project: str, subject: str, type: str):
        """
        Delete project resources level permissions.

        :param project: The project name
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        request.delete().resource(
                self._make_delete_ws(['project', project, 'permissions', 'resources'], subject, type)).send()

    def add_perm(self, project: str, subject: str, type: str, permission: str):
        """
        Add project resources level permissions.

        :param project: The project name
        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        :param permission: The permission
        """
        request = self._make_request()
        request.post().resource(
                self._make_add_ws(['project', project, 'permissions', 'resources'], subject, type, permission, self.PERMISSIONS)).send()


class RPermService(PermService):
    """
    R service permissions management.
    """

    PERMISSIONS = {
        'use': 'R_USE'
    }

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        super().__init__(client, verbose)

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        cls._add_permission_arguments(parser, list(cls.PERMISSIONS.keys()))

    @classmethod
    def do_command(cls, args):
        """
        Execute permission command
        """
        # Build and send requests
        cls._validate_args(args, cls.PERMISSIONS)

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = RPermService(client, args.verbose)
            # send request
            if args.delete:
                service.delete_perm(args.subject, args.type)
            elif args.add:
                service.add_perm(args.subject, args.type, args.permission)
            else:
                res = service.get_perms(args.type)
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_perms(self, type: str) -> list:
        """
        Get the R service permissions.

        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        response = request.get().resource(
                        self._make_get_ws(['system', 'permissions', 'r'], type)).send()
        return response.from_json()
    
    def delete_perm(self, subject: str, type: str):
        """
        Delete R level permissions.

        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        request.delete().resource(
                self._make_delete_ws(['system', 'permissions', 'r'], subject, type)).send()

    def add_perm(self, subject: str, type: str, permission: str):
        """
        Add R level permissions.

        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        :param permission: The permission
        """
        request = self._make_request()
        request.post().resource(
                self._make_add_ws(['system', 'permissions', 'r'], subject, type, permission, self.PERMISSIONS)).send()
    

class DataSHIELDPermService(PermService):
    """
    DataSHIELD service permissions management.
    """

    PERMISSIONS = {
        'use': 'DATASHIELD_USE',
        'administrate': 'DATASHIELD_ALL'
    }

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        super().__init__(client, verbose)

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        cls._add_permission_arguments(parser, list(cls.PERMISSIONS.keys()))

    @classmethod
    def do_command(cls, args):
        """
        Execute permission command
        """
        # Build and send requests
        cls._validate_args(args, cls.PERMISSIONS)

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = DataSHIELDPermService(client, args.verbose)
            # send request
            if args.delete:
                service.delete_perm(args.subject, args.type)
            elif args.add:
                service.add_perm(args.subject, args.type, args.permission)
            else:
                res = service.get_perms(args.type)
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_perms(self, type: str) -> list:
        """
        Get the DataSHIELD service permissions.

        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        response = request.get().resource(
                        self._make_get_ws(['system', 'permissions', 'datashield'], type)).send()
        return response.from_json()
    
    def delete_perm(self, subject: str, type: str):
        """
        Delete DataSHIELD level permissions.

        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        request.delete().resource(
                self._make_delete_ws(['system', 'permissions', 'datashield'], subject, type)).send()

    def add_perm(self, subject: str, type: str, permission: str):
        """
        Add DataSHIELD level permissions.

        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        :param permission: The permission
        """
        request = self._make_request()
        request.post().resource(
                self._make_add_ws(['system', 'permissions', 'datashield'], subject, type, permission, self.PERMISSIONS)).send()
    

class SystemPermService(PermService):
    """
    System administration permissions management.
    """

    PERMISSIONS = {
        'add-project': 'PROJECT_ADD',
        'administrate': 'SYSTEM_ALL'
    }

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        super().__init__(client, verbose)

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        cls._add_permission_arguments(parser, list(cls.PERMISSIONS.keys()))

    @classmethod
    def do_command(cls, args):
        """
        Execute permission command
        """
        # Build and send requests
        cls._validate_args(args, cls.PERMISSIONS)

        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = DataSHIELDPermService(client, args.verbose)
            # send request
            if args.delete:
                service.delete_perm(args.subject, args.type)
            elif args.add:
                service.add_perm(args.subject, args.type, args.permission)
            else:
                res = service.get_perms(args.type)
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()
    
    def get_perms(self, type: str) -> list:
        """
        Get the system administration permissions.

        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        response = request.get().resource(
                        self._make_get_ws(['system', 'permissions', 'administration'], type)).send()
        return response.from_json()
    

    def delete_perm(self, subject: str, type: str):
        """
        Delete system administration level permissions.

        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        """
        request = self._make_request()
        request.delete().resource(
                self._make_delete_ws(['system', 'permissions', 'administration'], subject, type)).send()

    def add_perm(self, subject: str, type: str, permission: str):
        """
        Add system administration level permissions.

        :param subject: The subject name
        :param type: The subject type ('user' or 'group')
        :param permission: The permission
        """
        request = self._make_request()
        request.post().resource(
                self._make_add_ws(['system', 'permissions', 'administration'], subject, type, permission, self.PERMISSIONS)).send()
