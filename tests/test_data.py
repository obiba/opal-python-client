from obiba_opal.data import get_data
from tests.utils import make_client

def test_entities():
    client = make_client()
    res = get_data(client, 'CNSIM.CNSIM1')
    assert type(res) == list
    assert len(res) == 2163

def test_valueset():
    client = make_client()
    res = get_data(client, 'CNSIM.CNSIM1', id='1604')
    assert type(res['valueSets']) == list
    assert res['valueSets'][0]['identifier'] == '1604'
    assert len(res['valueSets'][0]['values']) == 11
    assert type(res['variables']) == list
    assert len(res['variables']) == 11

def test_value():
    client = make_client()
    res = get_data(client, 'CNSIM.CNSIM1:GENDER', id='1604')
    assert res['value'] == '1'
