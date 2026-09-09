"""
Opal DataSHIELD management.
"""

import obiba_opal.core as core
import json


class DataSHIELDQuotaService:
    """
    DataSHIELD usage quotas management service.

    A quota is an allowance of R usage, in the DataSHIELD execution context, for a subject (the system default,
    a group or a user) and a usage metric. The usage consumed against it is summed over the rolling window
    of the quota period.
    """

    CONTEXT = "DataSHIELD"

    SUBJECT_TYPES = ("SYSTEM", "GROUP", "USER")
    METRICS = ("EXECUTION_TIME", "SESSION_TIME")
    PERIODS = ("DAILY", "WEEKLY")

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(cls, parser):
        """
        Add command specific options
        """
        parser.add_argument(
            "--id",
            "-id",
            required=False,
            help="Quota identifier. Not specifying the quota identifier, will get the list of the DataSHIELD quotas.",
        )
        parser.add_argument(
            "--type",
            "-ty",
            required=False,
            help=f"Subject type: {', '.join(cls.SUBJECT_TYPES).lower()}. Default is system.",
        )
        parser.add_argument(
            "--subject",
            "-s",
            required=False,
            help="Subject name: the user or the group name (required when subject type is user or group).",
        )
        parser.add_argument(
            "--metric",
            "-m",
            required=False,
            help=f"Usage metric: {', '.join(cls.METRICS).lower().replace('_', '-')}. Default is execution-time.",
        )
        parser.add_argument(
            "--period",
            "-pd",
            required=False,
            help=f"Rolling period the usage is summed over: {', '.join(cls.PERIODS).lower()}. Default is weekly.",
        )
        parser.add_argument(
            "--limit",
            "-lm",
            required=False,
            help="Usage allowance, in minutes (zero forbids DataSHIELD for this subject).",
        )
        parser.add_argument(
            "--disabled",
            "-di",
            action="store_true",
            required=False,
            help="Disable the quota (on add, if omitted the quota is enabled by default).",
        )
        parser.add_argument(
            "--enabled",
            "-en",
            action="store_true",
            required=False,
            help="Enable the quota (on update, when neither --enabled nor --disabled is specified, the quota is "
            "left as it is).",
        )
        parser.add_argument(
            "--fetch",
            "-fe",
            action="store_true",
            required=False,
            help="Fetch one or multiple quota(s). This is the default action.",
        )
        parser.add_argument("--add", "-a", action="store_true", help="Add a quota.")
        parser.add_argument(
            "--update",
            "-ud",
            action="store_true",
            required=False,
            help="Update a quota (requires a quota identifier).",
        )
        parser.add_argument(
            "--delete",
            "-de",
            action="store_true",
            required=False,
            help="Delete a quota (requires a quota identifier).",
        )
        parser.add_argument(
            "--usage",
            "-us",
            action="store_true",
            required=False,
            help="Get the quota usage of the user specified by --subject, or of the current user when omitted.",
        )
        parser.add_argument(
            "--json",
            "-j",
            action="store_true",
            help="Pretty JSON formatting of the response",
        )

    @classmethod
    def do_command(cls, args):
        """
        Execute quota command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = DataSHIELDQuotaService(client, args.verbose)
            limit_millis = cls._minutes_to_millis(args.limit)
            enabled = cls._parse_enabled(args)
            if args.add:
                res = service.add_quota(
                    limit_millis,
                    args.type,
                    args.subject,
                    args.metric,
                    args.period,
                    True if enabled is None else enabled,
                )
                core.Formatter.print_json(res, args.json)
            elif args.update:
                res = service.update_quota(
                    args.id,
                    limit_millis,
                    args.type,
                    args.subject,
                    args.metric,
                    args.period,
                    enabled,
                )
                core.Formatter.print_json(res, args.json)
            elif args.delete:
                service.delete_quota(args.id)
            elif args.usage:
                res = service.get_usage(args.subject) if args.subject else service.get_current_usage()
                core.Formatter.print_json(res, args.json)
            else:
                # fetching is the default action, --fetch being accepted to state it explicitly
                res = service.get_quota(args.id) if args.id else service.get_quotas()
                core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def get_quotas(self) -> list:
        """
        Get the DataSHIELD quotas.
        """
        request = self._make_request()
        response = request.get().resource(self._make_ws()).send()
        return response.from_json()

    def get_quota(self, id) -> dict:
        """
        Get a DataSHIELD quota.

        :param id: The quota identifier
        """
        if not id:
            raise ValueError("The identifier of the quota to fetch is required")
        request = self._make_request()
        response = request.get().resource(self._make_ws(id)).send()
        return response.from_json()

    def add_quota(
        self,
        limit_millis: int,
        subject_type: str = "SYSTEM",
        principal: str = None,
        metric: str = "EXECUTION_TIME",
        period: str = "WEEKLY",
        enabled: bool = True,
    ) -> dict:
        """
        Add a DataSHIELD quota. A subject that already has a quota for this metric has it replaced.

        :param limit_millis: The usage allowance, in milliseconds of whatever the metric names (zero forbids
            DataSHIELD for this subject)
        :param subject_type: The subject type: 'SYSTEM' (default), 'GROUP' or 'USER'
        :param principal: The user name or the group name (required when the subject type is 'GROUP' or 'USER')
        :param metric: The usage metric: 'EXECUTION_TIME' (default) or 'SESSION_TIME'
        :param period: The rolling period the usage is summed over: 'WEEKLY' (default) or 'DAILY'
        :param enabled: Whether the quota applies (True is default)
        """
        quota = self._make_quota(limit_millis, subject_type, principal, metric, period, enabled)
        request = self._make_request()
        request.accept_json().content_type_json()
        uri = core.UriBuilder(["service", "r", "quotas"]).build()
        response = request.post().resource(uri).content(json.dumps(quota)).send()
        return response.from_json()

    def update_quota(
        self,
        id,
        limit_millis: int = None,
        subject_type: str = None,
        principal: str = None,
        metric: str = None,
        period: str = None,
        enabled: bool = None,
    ) -> dict:
        """
        Update a DataSHIELD quota: the values that are not provided are left unchanged.

        :param id: The quota identifier
        :param limit_millis: The usage allowance, in milliseconds of whatever the metric names
        :param subject_type: The subject type: 'SYSTEM', 'GROUP' or 'USER'
        :param principal: The user name or the group name (required when the subject type is 'GROUP' or 'USER')
        :param metric: The usage metric: 'EXECUTION_TIME' or 'SESSION_TIME'
        :param period: The rolling period the usage is summed over: 'DAILY' or 'WEEKLY'
        :param enabled: Whether the quota applies
        """
        if not id:
            raise ValueError("The identifier of the quota to update is required")
        current = self.get_quota(id)
        quota = self._make_quota(
            current["limitMillis"] if limit_millis is None else limit_millis,
            current["subjectType"] if subject_type is None else subject_type,
            current["principal"] if principal is None else principal,
            current["metric"] if metric is None else metric,
            current["period"] if period is None else period,
            current["enabled"] if enabled is None else enabled,
        )
        quota["id"] = current["id"]
        request = self._make_request()
        request.accept_json().content_type_json()
        response = request.put().resource(self._make_ws(id)).content(json.dumps(quota)).send()
        return response.from_json()

    def delete_quota(self, id):
        """
        Delete a DataSHIELD quota. The subject becomes unlimited again, unless a broader quota still applies.

        :param id: The quota identifier
        """
        if not id:
            raise ValueError("The identifier of the quota to delete is required")
        request = self._make_request()
        request.delete().resource(self._make_ws(id)).send()

    def get_usage(self, user: str) -> list:
        """
        Get, for each usage metric, the DataSHIELD quota that applies to a user and what they have consumed
        against it. Reading the usage of another user requires administration permissions.

        :param user: The user name
        """
        if not user:
            raise ValueError("The name of the user to get the quota usage of is required")
        request = self._make_request()
        uri = core.UriBuilder(["service", "r", "quotas", "_usage"]).query("context", self.CONTEXT)
        response = request.get().resource(uri.query("user", user).build()).send()
        return response.from_json()

    def get_current_usage(self) -> list:
        """
        Get, for each usage metric, the DataSHIELD quota that applies to the current user and what they have
        consumed against it.
        """
        request = self._make_request()
        uri = core.UriBuilder(["service", "r", "quotas", "_current"]).query("context", self.CONTEXT).build()
        response = request.get().resource(uri).send()
        return response.from_json()

    def _make_quota(
        self,
        limit_millis: int,
        subject_type: str,
        principal: str,
        metric: str,
        period: str,
        enabled: bool,
    ) -> dict:
        """
        Make the quota representation, validating and normalizing the enumerated values
        """
        if limit_millis is None:
            raise ValueError("The quota limit is required")
        limit_millis = int(limit_millis)
        if limit_millis < 0:
            raise ValueError("The quota limit cannot be negative")
        subject_type = self._normalize(subject_type, "SYSTEM", self.SUBJECT_TYPES, "subject type")
        metric = self._normalize(metric, "EXECUTION_TIME", self.METRICS, "usage metric")
        period = self._normalize(period, "WEEKLY", self.PERIODS, "period")
        if subject_type == "SYSTEM":
            principal = ""
        elif not principal:
            raise ValueError("The subject name is required")
        return {
            "context": self.CONTEXT,
            "subjectType": subject_type,
            "principal": principal,
            "metric": metric,
            "period": period,
            "limitMillis": limit_millis,
            "enabled": enabled,
        }

    @classmethod
    def _normalize(cls, value: str, default: str, allowed: tuple, label: str) -> str:
        """
        Normalize an enumerated value, dashes being accepted in place of underscores
        """
        if not value:
            return default
        normalized = value.strip().upper().replace("-", "_")
        if normalized not in allowed:
            valid = ", ".join(allowed).lower().replace("_", "-")
            raise ValueError(f"Valid values for the {label} are: {valid}")
        return normalized

    @classmethod
    def _parse_enabled(cls, args) -> bool:
        """
        Get the enabled state to apply: None when it was not specified, so that an update leaves it as it is
        """
        if args.enabled and args.disabled:
            raise ValueError("The quota cannot be both enabled and disabled")
        if args.disabled:
            return False
        return True if args.enabled else None

    @classmethod
    def _minutes_to_millis(cls, minutes) -> int:
        """
        Convert a limit expressed in minutes into milliseconds
        """
        if minutes is None or minutes == "":
            return None
        return int(round(float(minutes) * 60000))

    def _make_ws(self, id=None):
        """
        Build the web service resource path
        """
        if id:
            return core.UriBuilder(["service", "r", "quota", str(id)]).build()
        return core.UriBuilder(["service", "r", "quotas"]).query("context", self.CONTEXT).build()

    def _make_request(self, fail_safe: bool = False):
        request = self.client.new_request()
        if not fail_safe:
            request.fail_on_error()
        if self.verbose:
            request.verbose()
        return request
