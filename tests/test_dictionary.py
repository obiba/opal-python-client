from obiba_opal.dictionary import DictionaryService, ExportAnnotationsService
from tests.utils import make_client
import io

class TestClass:

    @classmethod
    def setup_class(cls):
        client = make_client()
        setattr(cls, 'client', client)

    @classmethod
    def teardown_class(cls):
        cls.client.close()

    def test_datasource(self):
        client = self.client
        res = DictionaryService(client).get_datasource('CNSIM')
        assert res['name'] == 'CNSIM'

    def test_datasources(self):
        client = self.client
        res = DictionaryService(client).get_datasources()
        assert type(res) == list
        assert len(res) == 31

    def test_table(self):
        client = self.client
        res = DictionaryService(client).get_table('CNSIM', 'CNSIM1')
        assert res['name'] == 'CNSIM1'
        assert res['datasourceName'] == 'CNSIM'
        assert res['link'] == '/datasource/CNSIM/table/CNSIM1'

    def test_tables(self):
        client = self.client
        res = DictionaryService(client).get_tables('CNSIM')
        assert type(res) == list
        assert len(res) == 3

    def test_variable(self):
        client = self.client
        res = DictionaryService(client).get_variable('CNSIM', 'CNSIM1', 'GENDER')
        assert res['name'] == 'GENDER'
        assert res['parentLink']['link'] == '/datasource/CNSIM/table/CNSIM1'

    def test_variables(self):
        client = self.client
        res = DictionaryService(client).get_variables('CNSIM', 'CNSIM1')
        assert type(res) == list
        assert len(res) == 11

    def test_variable_annotations(self):
        client = self.client
        output = io.StringIO()
        ExportAnnotationsService(client).export_variable_annotations('CLSA', 'Tracking_60min_R1', 'WGHTS_PROV_TRM', output, taxonomies=['Mlstr_area'])
        rows = output.getvalue().split('\r\n')
        rows = [line.split('\t') for line in rows if len(line) > 0]
        assert len(rows) == 3
        assert len(rows[0]) == 6
        row = rows[2]
        assert row[0] == 'CLSA'
        assert row[1] == 'Tracking_60min_R1'
        assert row[2] == 'WGHTS_PROV_TRM'
        assert row[3] == 'Mlstr_area'
