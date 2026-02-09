import unittest
from tests.utils import make_client
from obiba_opal.system import TaxonomyService
from obiba_opal.file import FileService
import os
from uuid import uuid4


class TestClass(unittest.TestCase):
    TEST_FILE = "/tmp/data.csv"
    TEST_ZIPPED_FILE = "/tmp/data.zip"
    TEST_TAXONOMY_FILENAME = "OBiBa_taxonomyTest.yml"
    TEST_TAXONOMY_FILE = "/tmp/OBiBa_taxonomyTest.yml"
    LOCAL_TAXONOMY_FILE = "/tmp/OBiBa_taxonomyTest.yml"
    TEST_TAXONOMY_NAME = "OBiBa_taxonomyTest"

    @classmethod
    def setup_class(cls):
        cls.service = TaxonomyService(make_client())
        suffix = uuid4().hex
        cls.TEST_TAXONOMY_FILENAME = f"OBiBa_taxonomyTest_{suffix}.yml"
        cls.TEST_TAXONOMY_FILE = f"/tmp/{cls.TEST_TAXONOMY_FILENAME}"
        cls.LOCAL_TAXONOMY_FILE = f"/tmp/{cls.TEST_TAXONOMY_FILENAME}"
        cls.TEST_TAXONOMY_NAME = f"OBiBa_taxonomyTest_{suffix}"

    def test_1_importFile(self):
        try:
            fileService = FileService(make_client())
            # Read and modify the taxonomy file to use randomized name
            with open("./tests/resources/OBiBa_taxonomyTest.yml") as f:
                content = f.read()
            content = content.replace('"OBiBa_taxonomyTest"', f'"{self.TEST_TAXONOMY_NAME}"')
            with open(self.LOCAL_TAXONOMY_FILE, "w") as f:
                f.write(content)
            try:
                fileService.upload_file(self.LOCAL_TAXONOMY_FILE, "/tmp")
                response = fileService.file_info(self.TEST_TAXONOMY_FILE)
                if response["name"] == self.TEST_TAXONOMY_FILENAME:
                    response = self.service.importFile(self.TEST_TAXONOMY_FILE, True)
                    fileService.delete_file(self.TEST_TAXONOMY_FILE)
                    assert response.code == 201
                else:
                    raise AssertionError(
                        "Failed to import taxonomy, check if the file exists and if the name is correct."
                    ) from None
            finally:
                if os.path.exists(self.LOCAL_TAXONOMY_FILE):
                    os.remove(self.LOCAL_TAXONOMY_FILE)
        except Exception as e:
            raise AssertionError(
                "Failed to import taxonomy, check if the file exists and if the name is correct."
            ) from e

    def test_2_downloadTaxonomy(self):
        try:
            response = self.service.download(self.TEST_TAXONOMY_NAME)
            assert response.code == 200 and self.TEST_TAXONOMY_NAME in str(response)

        except Exception as e:
            raise AssertionError(
                "Failed to download taxonomy, check if the name is correct and if the taxonomy was properly imported."
            ) from e

    def test_3_taxonomiesSummary(self):
        try:
            name = self.TEST_TAXONOMY_NAME
            response = self.service.summaries()
            assert (
                response.code == 200
                and len(
                    list(
                        filter(
                            lambda t: t["name"] == name,
                            response.from_json()["summaries"],
                        )
                    )
                )
                > 0
            )
        except Exception as e:
            raise AssertionError(
                "Failed to get taxonomies summaries, check if the taxonomy was properly imported."
            ) from e

    def test_4_deleteTaxonomy(self):
        try:
            name = self.TEST_TAXONOMY_NAME
            # keep around for interactive test
            # response = self.service.confirmAndDelete(name, lambda: self.service.delete(name))
            response = self.service.delete(name)
            assert response.code == 200

        except Exception as e:
            raise AssertionError(
                "Failed to delete taxonomy, check if it was already deleted or if the name is correct."
            ) from e