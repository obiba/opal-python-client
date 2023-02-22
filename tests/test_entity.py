from obiba_opal.entity import EntityService
from tests.utils import make_client

def test_entity():
    client = make_client()
    res = EntityService(client).get_entity('1604')
    assert type(res) == dict
    assert res['entityType'] == 'Participant'
    assert res['identifier'] == '1604'

def test_entity_tables():
    client = make_client()
    res = EntityService(client).get_entity_tables('1604')
    assert type(res) == list
    assert len(res) > 0
    