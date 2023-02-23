from obiba_opal.project import ProjectService, BackupProjectCommand
from tests.utils import make_client
import random

class TestClass:

    @classmethod
    def setup_class(cls):
        client = make_client()
        setattr(cls, 'client', client)

    @classmethod
    def teardown_class(cls):
        cls.client.close()

    def test_project(self):
        client = self.client
        service = ProjectService(client)
        projects = service.get_projects()
        assert type(projects) == list
        assert len(projects) > 0
        assert 'CNSIM' in [x['name'] for x in projects]
        project = service.get_project('CNSIM')
        assert type(project) == dict
        assert project['name'] == 'CNSIM'

    def test_add_delete_project(self):
        client = self.client
        service = ProjectService(client)
        id = random.choice(list(range(1, 999, 1)))
        name = 'test%s' % id
        service.add_project(name)
        project = service.get_project(name)
        assert project['name'] == name
        service.delete_project(name)
        project = service.get_project(name)
        assert project is None

    def test_backup_command(self):
        client = self.client
        res = BackupProjectCommand(client).backup_project('CNSIM', '/tmp/test', force=True)
        assert res['command'] == 'backup'
        assert res['name'] == 'backup'
        assert res['project'] == 'CNSIM'
        assert 'status' in res
        assert 'id' in res
