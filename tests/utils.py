from obiba_opal import OpalClient

def make_client():
  return OpalClient.buildWithAuthentication(server='https://opal-demo.obiba.org', user='administrator', password='password')
