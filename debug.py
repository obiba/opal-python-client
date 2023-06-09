# from obiba_opal.console  import run
# run()


# from  tests.test_core import TestClass
# from  tests.test_file import TestClass
from tests.test_taxonomy import TestClass

tester = TestClass()
tester.setup_class()

tester.test_1_importFile()
# tester.test_2_downloadTaxonomy()
tester.test_3_taxonomiesSummary()
# tester.test_4_deleteTaxonomy()


# tester.test_sendRestBadCredentials()

# tester.test_1_fileUpload()
# tester.test_2_fileDownload()
# tester.test_3_fileDownloadWithPassword()
# tester.test_4_deleteUpload()
