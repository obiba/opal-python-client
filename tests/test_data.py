from obiba_opal.data import DataService, EntityService
from tests.utils import make_client

class TestClass:

    @classmethod
    def setup_class(cls):
        client = make_client()
        setattr(cls, 'client', client)

    @classmethod
    def teardown_class(cls):
        cls.client.close()

    def test_entities(self):
        client = self.client
        res = DataService(client).get_entities('CNSIM', 'CNSIM1')
        assert type(res) == list
        assert len(res) == 2163

    def test_valueset(self):
        client = self.client
        res = DataService(client).get_valueset('CNSIM', 'CNSIM1', id='1604')
        assert type(res['valueSets']) == list
        assert res['valueSets'][0]['identifier'] == '1604'
        assert len(res['valueSets'][0]['values']) == 11
        assert type(res['variables']) == list
        assert len(res['variables']) == 11

    def test_value(self):
        client = self.client
        res = DataService(client).get_value('CNSIM', 'CNSIM1', 'GENDER', id='1604')
        assert res['value'] == '1'

    def test_entity(self):
        client = self.client
        res = EntityService(client).get_entity('1604')
        assert type(res) == dict
        assert res['entityType'] == 'Participant'
        assert res['identifier'] == '1604'

    def test_entity_tables(self):
        client = self.client
        res = EntityService(client).get_entity_tables('1604')
        assert type(res) == list
        assert len(res) > 0
