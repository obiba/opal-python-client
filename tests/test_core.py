from argparse import Namespace
import unittest
from obiba_opal import OpalClient
from os.path import exists
from tests.utils import TEST_SERVER, TEST_USER, TEST_PASSWORD


class TestClass(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        # Make sure to place your own certificate files
        cls.SSL_CERTIFICATE = "./resources/certificates/publickey.pem"
        cls.SSL_KEY = "./resources/certificates/privatekey.pem"

    def test_sendRestBadServer(self):
        # FIXME for some reason, the cookie_file is not removed (despite the os.remove() is called and os.path.exists() says it was removed)
        try:
            # this one will make a request to check if an OTP is needed
            OpalClient.buildWithAuthentication(
                server="http://deadbeef:8080", user=TEST_USER, password=TEST_PASSWORD
            )
            assert False
        except Exception:
            assert True

    def test_sendRestBadCredentials(self):
        client = OpalClient.buildWithAuthentication(
            server=TEST_SERVER, user="admin", password=TEST_PASSWORD
        )

        try:
            self.assertRaises(Exception, self.__sendSimpleRequest, client.new_request())
        finally:
            client.close()

    def test_sendRest(self):
        client = None
        try:
            client = OpalClient.buildWithAuthentication(
                server=TEST_SERVER, user=TEST_USER, password=TEST_PASSWORD
            )
            self.__sendSimpleRequest(client.new_request())
        except Exception as e:
            self.fail(e)
        finally:
            if client:
                client.close()

    def test_sendSecuredRest(self):
        if exists(self.SSL_CERTIFICATE):
            client = None
            try:
                client = OpalClient.buildWithCertificate(
                    server=TEST_SERVER, cert=self.SSL_CERTIFICATE, key=self.SSL_KEY
                )
                self.__sendSimpleRequest(client.new_request())
            except Exception as e:
                self.fail(e)
            finally:
                if client:
                    client.close()

    def test_validAuthLoginInfo(self):
        client = None
        try:
            args = Namespace(opal=TEST_SERVER, user=TEST_USER, password=TEST_PASSWORD)
            client = OpalClient.build(loginInfo=OpalClient.LoginInfo.parse(args))
            self.__sendSimpleRequest(client.new_request())
        except Exception as e:
            self.fail(e)
        finally:
            if client:
                client.close()

    def test_validSslLoginInfo(self):
        if exists(self.SSL_CERTIFICATE):
            client = None
            try:
                args = Namespace(
                    opal=TEST_SERVER,
                    ssl_cert=self.SSL_CERTIFICATE,
                    ssl_key=self.SSL_KEY,
                )
                client = OpalClient.build(loginInfo=OpalClient.LoginInfo.parse(args))
                self.__sendSimpleRequest(client.new_request())
            except Exception as e:
                self.fail(e)
            finally:
                if client:
                    client.close()

    def test_invalidServerInfo(self):
        args = Namespace(opl=TEST_SERVER, user=TEST_USER, password=TEST_PASSWORD)
        self.assertRaises(Exception, OpalClient.LoginInfo.parse, args)

    def test_invalidLoginInfo(self):
        args = Namespace(opal=TEST_SERVER, usr="administrator", password=TEST_PASSWORD)
        self.assertRaises(Exception, OpalClient.LoginInfo.parse, args)

    def __sendSimpleRequest(self, request):
        request.fail_on_error()
        request.accept_json()
        # uncomment for debugging
        # request.verbose()

        # send request
        request.method("GET").resource("/projects")
        response = request.send()

        # format response
        res = response.content

        # output to stdout
        print(res)
