import pytest
from obiba_opal import DataSHIELDQuotaService, HTTPError
from tests.utils import make_client


class TestClass:
    @classmethod
    def setup_class(cls):
        cls.client = make_client()

    @classmethod
    def teardown_class(cls):
        cls.client.close()

    def _service(self):
        """
        The quotas API is only available from Opal 6, skip when the test server does not have it
        """
        service = DataSHIELDQuotaService(self.client)
        try:
            service.get_quotas()
        except HTTPError as e:
            if e.code == 404:
                pytest.skip("The Opal server does not support DataSHIELD quotas")
            raise
        return service

    @pytest.mark.integration
    def test_quotas(self):
        service = self._service()
        quotas = service.get_quotas()
        assert isinstance(quotas, list)
        for quota in quotas:
            assert quota["context"] == "DataSHIELD"

    @pytest.mark.integration
    def test_add_update_delete_quota(self):
        service = self._service()
        quota = service.add_quota(60 * 60000, "user", "dsuser", "session-time", "daily")
        try:
            assert quota["context"] == "DataSHIELD"
            assert quota["subjectType"] == "USER"
            assert quota["principal"] == "dsuser"
            assert quota["metric"] == "SESSION_TIME"
            assert quota["period"] == "DAILY"
            assert quota["limitMillis"] == 3600000
            assert quota["enabled"]

            fetched = service.get_quota(quota["id"])
            assert fetched["id"] == quota["id"]

            updated = service.update_quota(quota["id"], limit_millis=120 * 60000, enabled=False)
            assert updated["limitMillis"] == 7200000
            assert not updated["enabled"]
            # the values that were not provided are left unchanged
            assert updated["principal"] == "dsuser"
            assert updated["period"] == "DAILY"
        finally:
            service.delete_quota(quota["id"])

    @pytest.mark.integration
    def test_current_usage(self):
        service = self._service()
        usages = service.get_current_usage()
        assert isinstance(usages, list)
        for usage in usages:
            assert usage["context"] == "DataSHIELD"
            assert "metric" in usage
            assert "usedMillis" in usage


def test_invalid_quota():
    """
    The quota values are validated before any request is sent, so no server is needed here
    """
    service = DataSHIELDQuotaService(None)
    with pytest.raises(ValueError):
        # a group quota requires a subject name
        service.add_quota(60000, "group")
    with pytest.raises(ValueError):
        service.add_quota(60000, "nosuchtype", "dsuser")
    with pytest.raises(ValueError):
        service.add_quota(60000, "user", "dsuser", "nosuchmetric")
    with pytest.raises(ValueError):
        service.add_quota(60000, "user", "dsuser", "session-time", "nosuchperiod")
    with pytest.raises(ValueError):
        service.add_quota(-1)
    with pytest.raises(ValueError):
        service.get_quota(None)
    with pytest.raises(ValueError):
        service.delete_quota(None)
    with pytest.raises(ValueError):
        service.get_usage(None)
