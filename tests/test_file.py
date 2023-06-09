import unittest
from tests.utils import make_client
from obiba_opal.file import FileService
from obiba_opal.core import HTTPError
import os

class TestClass(unittest.TestCase):

  TEST_FILE = '/tmp/data.csv'
  TEST_ZIPPED_FILE = '/tmp/data.zip'

  @classmethod
  def setup_class(cls):
    cls.service = FileService(make_client())

  def test_1_fileUpload(self):
    try:
      self.service.upload_file('./tests/resources/data.csv', '/tmp')
      response = self.service.file_info(self.TEST_FILE)
      if response['name'] == 'data.csv':
        assert True
      else:
        assert False
    except Exception as e:
      assert False

  def test_2_fileDownload(self):
    try:
      outfile = open(self.TEST_FILE, "wb")
      fd = outfile.fileno()
      self.service.download_file(self.TEST_FILE, fd)
      if os.path.exists(self.TEST_FILE):
        os.remove(self.TEST_FILE)
        assert True
      else:
        assert False
    except Exception as e:
      assert False

  def test_3_fileDownloadWithPassword(self):
    try:
      outfile = open(self.TEST_ZIPPED_FILE, "wb")
      fd = outfile.fileno()
      self.service.download_file(self.TEST_FILE, fd, "12345678")
      stat = os.stat(self.TEST_ZIPPED_FILE)
      if stat.st_size > 0:
        os.remove(self.TEST_ZIPPED_FILE)
        assert True
      else:
        assert False
    except Exception as e:
      assert False

  def test_4_deleteUpload(self):
    try:
      self.service.delete_file('/tmp/data.csv')
      self.service.file_info(self.TEST_FILE)
    except HTTPError as e:
      assert e.code == 404
    except Exception as e:
      assert False
