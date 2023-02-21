from obiba_opal.backup_project import backup_project
from tests.utils import make_client

def test_command():
    client = make_client()
    res = backup_project(client, 'CNSIM', '/tmp/test', force=True)
    assert res['command'] == 'backup'
    assert res['name'] == 'backup'
    assert res['project'] == 'CNSIM'
    assert 'status' in res
    assert 'id' in res
