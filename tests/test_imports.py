from obiba_opal import ImportCSVCommand, TaskService, FileService, DictionaryService
from tests.utils import make_client
import random
import shutil
import os

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
        id = random.choice(list(range(1, 999, 1)))
        inname = 'data%s' % id
        inpath = '/tmp/%s.csv' % inname
        shutil.copy('./tests/resources/data.csv', inpath)
        fs.upload_file(inpath, '/tmp')
        os.remove(inpath)
        assert fs.file_info(inpath) is not None
        service = ImportCSVCommand(client)
        task = service.import_data(inpath, 'CNSIM')
        assert 'id' in task
        status = TaskService(client).wait_task(task['id'])
        assert status in ['SUCCEEDED', 'CANCELED', 'FAILED']
        fs.delete_file(inpath)
        dico = DictionaryService(client)
        table = dico.get_table('CNSIM', inname)
        assert table is not None
        dico.delete_tables('CNSIM', [inname])
        ds = dico.get_datasource('CNSIM')
        assert inname not in ds['table']