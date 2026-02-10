from turtle import fd
import unittest
from tests.utils import make_client
from obiba_opal.file import FileService
from obiba_opal.core import HTTPError
import os
import shutil
from uuid import uuid4


class TestClass(unittest.TestCase):
    TEST_FILE = "/tmp/data.csv"
    TEST_ZIPPED_FILE = "/tmp/data.zip"
    TEST_FILENAME = "data.csv"
    LOCAL_UPLOAD_FILE = "/tmp/data.csv"

    @classmethod
    def setup_class(cls):
        cls.service = FileService(make_client())
        suffix = uuid4().hex
        cls.TEST_FILENAME = f"data_{suffix}.csv"
        cls.TEST_FILE = f"/tmp/{cls.TEST_FILENAME}"
        cls.TEST_ZIPPED_FILE = f"/tmp/data_{suffix}.zip"
        cls.LOCAL_UPLOAD_FILE = f"/tmp/{cls.TEST_FILENAME}"

    def test_1_fileUpload(self):
        try:
            print(f"Uploading file to {self.TEST_FILE}...")
            shutil.copyfile("./tests/resources/data.csv", self.LOCAL_UPLOAD_FILE)
            try:
                self.service.upload_file(self.LOCAL_UPLOAD_FILE, "/tmp")
                response = self.service.file_info(self.TEST_FILE)
                if response["name"] == self.TEST_FILENAME:
                    assert True
                else:
                    raise AssertionError(
                        "Failed to upload file, check if the file exists and if the name is correct."
                    ) from None
            finally:
                if os.path.exists(self.LOCAL_UPLOAD_FILE):
                    os.remove(self.LOCAL_UPLOAD_FILE)
        except Exception as e:
            raise AssertionError("Failed to upload file, check if the file exists and if the name is correct.") from e

    def test_2_fileDownload(self):
        try:
            print(f"Downloading file to {self.TEST_FILE}...")
            # New: pythonic way
            with open(self.TEST_FILE, "wb") as outfile:
                self.service.download_file(self.TEST_FILE, outfile)
            if os.path.exists(self.TEST_FILE):
                os.remove(self.TEST_FILE)
                assert True
            else:
                raise AssertionError(
                    "Failed to download file, check if the file exists and if the name is correct."
                ) from None
        except Exception as e:
            raise AssertionError("Failed to download file, check if the file exists and if the name is correct.") from e

    def test_3_fileDownloadWithPassword(self):
        try:
            print(f"Downloading file with password to {self.TEST_ZIPPED_FILE}...")
            # New: pythonic way
            with open(self.TEST_ZIPPED_FILE, "wb") as outfile:
                self.service.download_file(self.TEST_FILE, outfile, "12345678")
            stat = os.stat(self.TEST_ZIPPED_FILE)
            if stat.st_size > 0:
                os.remove(self.TEST_ZIPPED_FILE)
                assert True
            else:
                raise AssertionError(
                    "Failed to download file with password, check if the file exists and if the name is correct."
                ) from None
        except Exception as e:
            raise AssertionError(
                "Failed to download file with password, check if the file exists and if the name is correct."
            ) from e

    def test_4_deleteUpload(self):
        try:
            print(f"Deleting file {self.TEST_FILE}...")
            self.service.delete_file(self.TEST_FILE)
            self.service.file_info(self.TEST_FILE)
        except HTTPError as e:
            assert e.code == 404
        except Exception as e:
            raise AssertionError("Failed to delete file, check if the file exists and if the name is correct.") from e
