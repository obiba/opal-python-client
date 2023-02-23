from obiba_opal import ExportCSVCommand, TaskService
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

    def test_csv(self):
        client = self.client
        service = ExportCSVCommand(client)
        id = random.choice(list(range(1, 999, 1)))
        output = '/tmp/test%s' % id
        task = service.export_data('CNSIM', ['CNSIM1'], output)
        assert task['command'] == 'copy'
        assert 'id' in task
        status = TaskService(client).wait_task(task['id'])
        assert status in ['SUCCEEDED', 'CANCELED', 'FAILED']