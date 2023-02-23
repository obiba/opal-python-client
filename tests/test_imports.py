from obiba_opal import ImportCSVCommand, TaskService, FileService, DictionaryService, DeleteTableService
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
        fs = FileService(client)
        fs.upload_file('./tests/resources/data.csv', '/tmp')
        assert fs.file_info('/tmp/data.csv') is not None
        service = ImportCSVCommand(client)
        task = service.import_data('/tmp/data.csv', 'CNSIM')
        assert 'id' in task
        status = TaskService(client).wait_task(task['id'])
        assert status in ['SUCCEEDED', 'CANCELED', 'FAILED']
        dico = DictionaryService(client)
        table = dico.get_table('CNSIM', 'data')
        assert table is not None
        DeleteTableService(client).delete_tables('CNSIM', ['data'])
        ds = dico.get_datasource('CNSIM')
        assert 'data' not in ds['table']