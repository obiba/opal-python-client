import unittest
from tests.utils import make_client
from obiba_opal.system import TaxonomyService
from obiba_opal.file import FileService
from obiba_opal.core import HTTPError

class TestClass(unittest.TestCase):

  TEST_FILE = '/tmp/data.csv'
  TEST_ZIPPED_FILE = '/tmp/data.zip'

  @classmethod
  def setup_class(cls):
    cls.service = TaxonomyService(make_client())

  def test_1_importFile(self):
    try:
      fileService  = FileService(make_client())
      fileService.upload_file('./tests/resources/OBiBa_taxonomyTest.yml', '/tmp')
      response = fileService.file_info('/tmp/OBiBa_taxonomyTest.yml')
      if response['name'] == 'OBiBa_taxonomyTest.yml':
        response = self.service.importFile('/tmp/OBiBa_taxonomyTest.yml', True)
        fileService.delete_file('/tmp/OBiBa_taxonomyTest.yml')
        assert response.code == 201
      else:
        assert False

    except Exception as e:
      assert False


  def test_2_downloadTaxonomy(self):
    try:
      response = self.service.download('OBiBa_taxonomyTest')
      assert response.code == 200 and 'OBiBa_taxonomyTest' in str(response)

    except Exception as e:
      assert False


  def test_3_taxonomiesSummary(self):
    try:
      name = 'OBiBa_taxonomyTest'
      response = self.service.summaries()
      assert response.code == 200 and len(list(filter(lambda t: t['name'] == name, response.from_json()['summaries']))) > 0
    except Exception as e:
      assert False

  def test_4_deleteTaxonomy(self):
    try:
      name = 'OBiBa_taxonomyTest'
      # keep around for interactive test
      # response = self.service.confirmAndDelete(name, lambda: self.service.delete(name))
      response = self.service.delete(name)
      assert response.code == 200

    except Exception as e:
      assert False