from obiba_opal import OpalClient

TEST_SERVER = 'https://mica-demo.obiba.org'
# TEST_SERVER = 'http://localhost:8080'
TEST_USER = 'administrator'
TEST_PASSWORD = 'password'


def make_client():
  return OpalClient.buildWithAuthentication(server=TEST_SERVER, user=TEST_USER, password=TEST_PASSWORD)
