from obiba_opal import DataService, EntityService
from tests.utils import make_client


class TestClass:
    @classmethod
    def setup_class(cls):
        client = make_client()
        cls.client = client

    @classmethod
    def teardown_class(cls):
        cls.client.close()

    def test_entities(self):
        client = self.client
        res = DataService(client).get_entities("CNSIM", "CNSIM1")
        assert isinstance(res, list)
        assert len(res) == 2163

    def test_valueset(self):
        client = self.client
        res = DataService(client).get_valueset("CNSIM", "CNSIM1", id="1604")
        assert isinstance(res["valueSets"], list)
        assert res["valueSets"][0]["identifier"] == "1604"
        assert len(res["valueSets"][0]["values"]) == 11
        assert isinstance(res["variables"], list)
        assert len(res["variables"]) == 11

    def test_value(self):
        client = self.client
        res = DataService(client).get_value("CNSIM", "CNSIM1", "GENDER", id="1604")
        assert res["value"] == "1"

    def test_entity(self):
        client = self.client
        res = EntityService(client).get_entity("1604")
        assert isinstance(res, dict)
        assert res["entityType"] == "Participant"
        assert res["identifier"] == "1604"

    def test_entity_tables(self):
        client = self.client
        res = EntityService(client).get_entity_tables("1604")
        assert isinstance(res, list)
        assert len(res) > 0
