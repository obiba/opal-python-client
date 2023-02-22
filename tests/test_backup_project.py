from obiba_opal.backup_project import BackupProjectCommand
from tests.utils import make_client

def test_command():
    client = make_client()
    res = BackupProjectCommand(client).backup_project('CNSIM', '/tmp/test', force=True)
    assert res['command'] == 'backup'
    assert res['name'] == 'backup'
    assert res['project'] == 'CNSIM'
    assert 'status' in res
    assert 'id' in res
