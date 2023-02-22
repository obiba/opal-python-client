from obiba_opal.dictionary import DictionaryService
from tests.utils import make_client

def test_datasource():
    client = make_client()
    res = DictionaryService(client).get_datasource('CNSIM')
    assert res['name'] == 'CNSIM'

def test_datasources():
    client = make_client()
    res = DictionaryService(client).get_datasources()
    assert type(res) == list
    assert len(res) == 31

def test_table():
    client = make_client()
    res = DictionaryService(client).get_table('CNSIM', 'CNSIM1')
    assert res['name'] == 'CNSIM1'
    assert res['datasourceName'] == 'CNSIM'
    assert res['link'] == '/datasource/CNSIM/table/CNSIM1'

def test_tables():
    client = make_client()
    res = DictionaryService(client).get_tables('CNSIM')
    assert type(res) == list
    assert len(res) == 3

def test_variable():
    client = make_client()
    res = DictionaryService(client).get_variable('CNSIM', 'CNSIM1', 'GENDER')
    assert res['name'] == 'GENDER'
    assert res['parentLink']['link'] == '/datasource/CNSIM/table/CNSIM1'

def test_variables():
    client = make_client()
    res = DictionaryService(client).get_variables('CNSIM', 'CNSIM1')
    assert type(res) == list
    assert len(res) == 11
