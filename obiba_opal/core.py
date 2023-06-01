"""
Based on PyCurl http://pycurl.sourceforge.net/
See also http://www.angryobjects.com/2011/10/15/http-with-python-pycurl-by-example/
Curl options http://curl.haxx.se/libcurl/c/curl_easy_setopt.html
"""

import random
import base64
import getpass
import json
import os
from requests import Session, Request, Response
import pycurl
import urllib.error
import urllib.parse
import urllib.request
from functools import reduce
from http import HTTPStatus
import logging

class OpalClient:
    """
    OpalClient holds the configuration for connecting to Opal.
    """

    def __init__(self, server=None):
        self.session = Session()
        self.headers = {}
        self.base_url = self.__ensure_entry('Opal address', server)
        self.id = None
        self.rid = None

    def __del__(self):
        self.close()

    @classmethod
    def build(cls, loginInfo):
        if loginInfo.isSsl():
            return OpalClient.buildWithCertificate(loginInfo.data['server'], loginInfo.data['cert'],
                                                   loginInfo.data['key'], loginInfo.data['no_ssl_verify'])
        elif loginInfo.isToken():
            return OpalClient.buildWithToken(loginInfo.data['server'], loginInfo.data['token'], loginInfo.data['no_ssl_verify'])
        else:
            return OpalClient.buildWithAuthentication(loginInfo.data['server'], loginInfo.data['user'],
                                                      loginInfo.data['password'], loginInfo.data['no_ssl_verify'])

    @classmethod
    def buildWithCertificate(cls, server, cert, key, no_ssl_verify: bool = False):
        client = cls(server)
        if client.base_url.startswith('https:'):
            client.session.verify = False if no_ssl_verify else True
        client.session.cert = (cert, key)

        return client

    @classmethod
    def buildWithAuthentication(cls, server, user, password, no_ssl_verify: bool = False):
        client = cls(server)
        if client.base_url.startswith('https:'):
            client.session.verify = False if no_ssl_verify else True
        client.credentials(user, password)

        # need to know whether a OTP is needed
        try:
            client.init_otp()
        except Exception as e:
            # do the close as the exception is raised in the builder
            client.close()
            raise e

        return client

    @classmethod
    def buildWithToken(cls, server, token, no_ssl_verify: bool = False):
        client = cls(server)
        if client.base_url.startswith('https:'):
            client.session.verify = False if no_ssl_verify else True
        client.token(token)
        return client

    def __ensure_entry(self, text, entry, pwd=False):
        e = entry
        if not entry:
            if pwd:
                e = getpass.getpass(prompt=text + ': ')
            else:
                e = input(text + ': ')
        return e

    def credentials(self, user, password):
        u = self.__ensure_entry('User name', user)
        p = self.__ensure_entry('Password', password, True)
        return self.header('Authorization', 'Basic ' + base64.b64encode('{}:{}'.format(u, p).encode('utf-8')).decode('utf-8'))

    def token(self, token):
        tk = self.__ensure_entry('Token', token, True)
        return self.header('X-Opal-Auth', tk)

    def init_otp(self):
        # check if an OTP is needed
        request = self.new_request()
        profile_url = '/system/subject-profile/_current'
        response = request.accept_json().get().resource(profile_url).send()
        if response.code == 401:
            otp_header = response.get_header('WWW-Authenticate').split(' ')[0]
            if otp_header == 'X-Opal-TOTP' or otp_header == 'X-Obiba-TOTP':
                val = input('Enter 6-digits code: ')
                # validate code and get the opalsid cookie for further requests
                request = self.new_request()
                request.header(otp_header, val).accept_json().get().resource(profile_url).send()

    def verify(self, value):
        """
        Ignore or validate certificate

        :param value = True/False to validation or not. Value can also be a CA_BUNDLE file or directory (e.g. 'verify=/etc/ssl/certs/ca-certificates.crt')
        """
        self.session.verify = value
        return self

    def ssl_version(self, version):
        return self.curl_option(pycurl.SSLVERSION, version)

    def curl_option(self, opt, value):
        self.curl_options[opt] = value
        return self

    def header(self, key, value):
        """
        Adds a header to session headers used by the request

        :param key - header key
        :param value - header value
        """
        header = {}
        header[key] = value

        self.session.headers.update(header)
        return self

    def new_request(self):
        return OpalRequest(self)

    def close(self):
        if self.id is not None:
            # request to close session
            try:
                self.new_request().resource('/auth/session/_current').delete().send()
                self.session.close()
            except Exception as e:
                pass
            self.id = None

    class LoginInfo:
        data = None

        @classmethod
        def parse(cls, args):
            data = {}
            argv = vars(args)

            if argv.get('opal'):
                data['server'] = argv['opal']
            else:
                raise ValueError('Opal server information is missing.')

            data['no_ssl_verify'] = argv.get('no_ssl_verify')

            if argv.get('user') and argv.get('password'):
                data['user'] = argv['user']
                data['password'] = argv['password']
            elif argv.get('token'):
                data['token'] = argv['token']
            elif argv.get('ssl_cert') and argv.get('ssl_key'):
                data['cert'] = argv['ssl_cert']
                data['key'] = argv['ssl_key']
            else:
                raise ValueError(
                    'Invalid login information. Requires user-password or token or certificate-key information')

            setattr(cls, 'data', data)
            return cls()

        def isToken(self):
            if self.data.keys() & {'token'}:
                return True
            return False

        def isSsl(self):
            if self.data.keys() & {'cert', 'key'}:
                return True
            return False


class OpalClientOld:
    """
    OpalClient holds the configuration for connecting to Opal.
    """

    def __init__(self, server=None):
        self.curl_options = {}
        self.headers = {}
        self.base_url = self.__ensure_entry('Opal address', server)
        self.id = None
        self.rid = None

    @classmethod
    def build(cls, loginInfo):
        if loginInfo.isSsl():
            return OpalClient.buildWithCertificate(loginInfo.data['server'], loginInfo.data['cert'],
                                                   loginInfo.data['key'])
        elif loginInfo.isToken():
            return OpalClient.buildWithToken(loginInfo.data['server'], loginInfo.data['token'])
        else:
            return OpalClient.buildWithAuthentication(loginInfo.data['server'], loginInfo.data['user'],
                                                      loginInfo.data['password'])

    @classmethod
    def buildWithCertificate(cls, server, cert, key):
        client = cls(server)
        if client.base_url.startswith('https:'):
            client.verify_peer(0)
            client.verify_host(0)
        client.keys(cert, key)
        return client

    @classmethod
    def buildWithAuthentication(cls, server, user, password):
        client = cls(server)
        if client.base_url.startswith('https:'):
            client.verify_peer(0)
            client.verify_host(0)
        client.credentials(user, password)

        # need to know whether a OTP is needed
        try:
            client.init_otp()
        except Exception as e:
            # do the close as the exception is raised in the builder
            client.close()
            raise e

        return client

    @classmethod
    def buildWithToken(cls, server, token):
        client = cls(server)
        if client.base_url.startswith('https:'):
            client.verify_peer(0)
            client.verify_host(0)
        client.token(token)
        return client

    def __ensure_entry(self, text, entry, pwd=False):
        e = entry
        if not entry:
            if pwd:
                e = getpass.getpass(prompt=text + ': ')
            else:
                e = input(text + ': ')
        return e

    def credentials(self, user, password):
        u = self.__ensure_entry('User name', user)
        p = self.__ensure_entry('Password', password, True)
        return self.header('Authorization', 'Basic ' + base64.b64encode((u + ':' + p).encode('utf-8')).decode('utf-8'))

    def token(self, token):
        tk = self.__ensure_entry('Token', token, True)
        return self.header('X-Opal-Auth', tk)

    def init_otp(self):
        # check if an OTP is needed
        request = self.new_request()
        profile_url = '/system/subject-profile/_current'
        response = request.accept_json().get().resource(profile_url).send()
        if response.code == 401:
            otp_header = response.get_header('WWW-Authenticate').split(' ')[0]
            if otp_header == 'X-Opal-TOTP' or otp_header == 'X-Obiba-TOTP':
                val = input('Enter 6-digits code: ')
                # validate code and get the opalsid cookie for further requests
                request = self.new_request()
                request.header(otp_header, val).accept_json().get().resource(profile_url).send()

    def keys(self, cert_file, key_file, key_pwd=None, ca_certs=None):
        self.curl_option(pycurl.SSLCERT, cert_file)
        self.curl_option(pycurl.SSLKEY, key_file)
        if key_pwd:
            self.curl_option(pycurl.KEYPASSWD, key_pwd)
        if ca_certs:
            self.curl_option(pycurl.CAINFO, ca_certs)
        self.headers.pop('Authorization', None)
        return self

    def verify_peer(self, verify):
        return self.curl_option(pycurl.SSL_VERIFYPEER, verify)

    def verify_host(self, verify):
        return self.curl_option(pycurl.SSL_VERIFYHOST, verify)

    def ssl_version(self, version):
        return self.curl_option(pycurl.SSLVERSION, version)

    def curl_option(self, opt, value):
        self.curl_options[opt] = value
        return self

    def header(self, key, value):
        self.headers[key] = value
        return self

    def new_request(self):
        if self.id is None:
            # iterate until file does not exists
            self.id = random.choice(list(range(1, 999999, 1)))
            self.cookie_file = "%s/opal-cookie-%s.dat" % (self.get_app_workdir(), self.id)
            while os.path.exists(self.cookie_file):
                self.id = random.choice(list(range(1, 999999, 1)))
                self.cookie_file = "/tmp/opal-cookie-%s.dat" % self.id
        return OpalRequest(self)

    def get_app_home(self):
        app_home = os.environ['OBIBA_HOME'] if 'OBIBA_HOME' in os.environ else None
        if not app_home:
            app_home = '/tmp/.obiba'
            try:
                app_home = os.path.expanduser('~/.obiba')
            except Exception as e:
                pass

        os.makedirs(app_home, exist_ok=True)
        return app_home

    def get_app_workdir(self):
        app_home = self.get_app_home()
        workdir = '%s/work' % app_home
        os.makedirs(workdir, exist_ok=True)
        return workdir

    def close(self):
        if self.id is not None:
            # request to close session
            try:
                self.new_request().resource('/auth/session/_current').delete().send()
            except Exception as e:
                pass
            self.id = None
        if self.cookie_file:
            try:
                os.remove(self.cookie_file)
            except OSError:
                pass

    class LoginInfo:
        data = None

        @classmethod
        def parse(cls, args):
            data = {}
            argv = vars(args)

            if argv.get('opal'):
                data['server'] = argv['opal']
            else:
                raise ValueError('Opal server information is missing.')

            if argv.get('user') and argv.get('password'):
                data['user'] = argv['user']
                data['password'] = argv['password']
            elif argv.get('token'):
                data['token'] = argv['token']
            elif argv.get('ssl_cert') and argv.get('ssl_key'):
                data['cert'] = argv['ssl_cert']
                data['key'] = argv['ssl_key']
            else:
                raise ValueError(
                    'Invalid login information. Requires user-password or token or certificate-key information')

            setattr(cls, 'data', data)
            return cls()

        def isToken(self):
            if self.data.keys() & {'token'}:
                return True
            return False

        def isSsl(self):
            if self.data.keys() & {'cert', 'key'}:
                return True
            return False

class OpalRequest:
    """
    Opal request.
    """

    def __init__(self, opal_client):
        self.client = opal_client
        self.options = {}
        self.headers = {'Accept': 'application/json'}
        self._verbose = False
        self.params = {}
        self._fail_on_error = False
        self.files = None
        self.data = None

    def timeout(self, value):
        """
        Sets the connection and read timeout
        Note: value can be a tupple to have different timeouts for connection and reading (connTimout, readTimeout)

        :param value - connection/read timout
        """
        self.options['timeout'] = value
        return self

    def verbose(self):
        """
        Enables the verbose mode

        Note: Requests library logging requires a log-level DEBUG
        """
        logging.basicConfig(level=logging.DEBUG)
        self._verbose = True
        return self

    def fail_on_error(self):
        self._fail_on_error = True
        return self

    def header(self, key, value):
        """
        Adds a header to session headers used by the request

        :param key - header key
        :param value - header value
        """
        if value:
            header = {}
            header[key] = value
            self.headers.update(header)
        return self

    def accept(self, value):
        self.headers.update({'Accept': value})
        return self

    def accept_json(self):
        return self.accept('application/json')

    def accept_xml(self):
        return self.accept('application/xml')

    def accept_text_csv(self):
        return self.accept('text/csv')

    def content_type(self, value):
        return self.headers.update({'Content-Type': value})

    def content_type_json(self):
        return self.content_type('application/json')

    def content_type_text_plain(self):
        return self.content_type('text/plain')

    def content_type_form_urlencoded(self):
        return self.content_type('application/x-www-form-urlencoded')

    def content_type_rscript(self):
        return self.content_type('application/x-rscript')

    def method(self, method):
        if not method:
            self.method = 'GET'
        elif method in ['GET', 'DELETE', 'PUT', 'POST', 'OPTIONS']:
            self.method = method
        else:
            raise ValueError('Not a valid method: ' + method)
        return self

    def get(self):
        return self.method('GET')

    def put(self):
        return self.method('PUT')

    def post(self):
        return self.method('POST')

    def delete(self):
        return self.method('DELETE')

    def options(self):
        return self.method('OPTIONS')

    def resource(self, ws):
        self.resource = ws
        return self

    def content(self, content):
        """
        Stores the request body

        :param content - request body
        """
        if self._verbose:
            print('* Content:')
            print(content)

        self.data = content
        return self

    def content_upload(self, filename):
        """
        Sets the file associate with the upload

        Note: Requests library takes care of mutlti-part setting in the header
        """
        if self._verbose:
            logging.info('* File Content:')
            logging.info('[file=' + filename + ', size=' + str(os.path.getsize(filename)) + ']')
        self.files = {'file': (os.path.basename(filename), open(filename, 'rb'))}
        return self

    def __build_request(self):
        request = Request()
        request.method = self.method if self.method else 'GET'

        for option in self.options:
            setattr(request, option, self.options[option])

        # Combine the client and the request headers
        request.headers = {}
        request.headers.update(self.client.session.headers)
        request.headers.update(self.headers)

        if self.resource:
            path = self.resource
            request.url = self.client.base_url + '/ws' + path

            if self.params:
                request.params = self.params
        else:
            raise ValueError('Resource is missing')

        if self.files is not None:
            request.files = self.files

        if self.data is not None:
            request.data = self.data

        return request

    def send(self):
        """
        Sends the request via client session object
        """
        request = self.__build_request()
        response = OpalResponse(self.client.session.send(request.prepare()))

        if self._fail_on_error and response.code >= 400:
            raise HTTPError(response)

        return response

class OpalRequestOld:
    """
    Opal request.
    """

    def __init__(self, opal_client):
        self.client = opal_client
        self.curl_options = {}
        self.headers = {'Accept': 'application/json'}
        self._verbose = False
        self._fail_on_error = False

    def curl_option(self, opt, value):
        self.curl_options[opt] = value
        return self

    def timeout(self, value):
        return self.curl_option(pycurl.TIMEOUT, value)

    def connection_timeout(self, value):
        return self.curl_option(pycurl.CONNECTTIMEOUT, value)

    def verbose(self):
        self._verbose = True
        return self.curl_option(pycurl.VERBOSE, True)

    def fail_on_error(self):
        # return self.curl_option(pycurl.FAILONERROR, True)
        self._fail_on_error = True
        return self

    def header(self, key, value):
        if value:
            self.headers[key] = value
        return self

    def accept(self, value):
        return self.header('Accept', value)

    def accept_json(self):
        return self.accept('application/json')

    def accept_xml(self):
        return self.accept('application/xml')

    def accept_text_csv(self):
        return self.accept('text/csv')

    def content_type(self, value):
        return self.header('Content-Type', value)

    def content_type_json(self):
        return self.content_type('application/json')

    def content_type_text_plain(self):
        return self.content_type('text/plain')

    def content_type_form_urlencoded(self):
        return self.content_type('application/x-www-form-urlencoded')

    def content_type_rscript(self):
        return self.content_type('application/x-rscript')

    def method(self, method):
        if not method:
            self.method = 'GET'
        elif method in ['GET', 'DELETE', 'PUT', 'POST', 'OPTIONS']:
            self.method = method
        else:
            raise ValueError('Not a valid method: ' + method)
        return self

    def get(self):
        return self.method('GET')

    def put(self):
        return self.method('PUT')

    def post(self):
        return self.method('POST')

    def delete(self):
        return self.method('DELETE')

    def options(self):
        return self.method('OPTIONS')

    def __build_request(self):
        curl = pycurl.Curl()
        # curl options
        for o in self.client.curl_options:
            curl.setopt(o, self.client.curl_options[o])
        for o in self.curl_options:
            curl.setopt(o, self.curl_options[o])
            # headers
        hlist = []
        for h in self.client.headers:
            hlist.append(h + ": " + self.client.headers[h])
        for h in self.headers:
            hlist.append(h + ": " + self.headers[h])
        curl.setopt(pycurl.HTTPHEADER, hlist)
        if self.method:
            curl.setopt(pycurl.CUSTOMREQUEST, self.method)
        if self.resource:
            curl.setopt(pycurl.URL, self.client.base_url + '/ws' + self.resource)
        else:
            raise ValueError('Resource is missing')
        return curl

    def resource(self, ws):
        self.resource = ws
        return self

    def content(self, content):
        if self._verbose:
            print('* Content:')
            print(content)
        encodedContent = content.encode('utf-8')
        self.curl_option(pycurl.POST, 1)
        self.curl_option(pycurl.POSTFIELDSIZE, len(encodedContent))
        self.curl_option(pycurl.POSTFIELDS, encodedContent)
        return self

    def content_file(self, filename):
        if self._verbose:
            print('* File Content:')
            print('[file=' + filename + ', size=' + str(os.path.getsize(filename)) + ']')
        self.curl_option(pycurl.POST, 1)
        self.curl_option(pycurl.POSTFIELDSIZE, os.path.getsize(filename))
        reader = open(filename, 'rb')
        self.curl_option(pycurl.READFUNCTION, reader.read)
        return self

    def content_upload(self, filename):
        path = os.path.expanduser(filename)
        if self._verbose:
            print('* File Content:')
            print('[file=' + filename + ', size=' + str(os.path.getsize(path)) + ']')
            # self.curl_option(pycurl.POST,1)
        self.curl_option(pycurl.HTTPPOST, [('file1', (pycurl.FORM_FILE, path))])
        return self

    def send(self, buffer=None):
        curl = self.__build_request()
        hbuf = HeaderStorage()
        cbuf = Storage()
        if buffer:
            curl.setopt(curl.WRITEFUNCTION, buffer.write)
        else:
            curl.setopt(curl.WRITEFUNCTION, cbuf.store)
        curl.setopt(curl.HEADERFUNCTION, hbuf.store)
        curl.setopt(curl.COOKIEJAR, self.client.cookie_file)
        curl.setopt(curl.COOKIEFILE, self.client.cookie_file)
        curl.perform()
        response = OpalResponse(curl.getinfo(pycurl.HTTP_CODE), hbuf.headers, cbuf.content.decode('utf-8'))
        curl.close()

        if self._fail_on_error and response.code >= 400:
            raise HTTPError(response)

        return response

class Storage:
    """
    Content storage.
    """

    def __init__(self):
        self.content = bytearray()
        self.line = 0

    def store(self, buf):
        self.line = self.line + 1
        self.content = self.content + buf

    def __str__(self):
        return self.contents.decode('utf-8')


class HeaderStorage(Storage):
    """
    Store response headers in a dictionary: key is the header name,
    value is header value or the list of header values.
    """

    def __init__(self):
        Storage.__init__(self)
        self.headers = {}

    def store(self, buf):
        Storage.store(self, buf)
        header = buf.decode('utf-8').partition(':')
        if header[1]:
            value = header[2].rstrip().strip()
            if header[0] in self.headers:
                current_value = self.headers[header[0]]
                if isinstance(current_value, str):
                    self.headers[header[0]] = list(current_value)
                self.headers[header[0]].append(value)
            else:
                self.headers[header[0]] = value

class OpalResponse:
    """
    Response from Opal: code, headers and content
    """

    def __init__(self, response: Response):
        self.response = response

    @property
    def code(self):
        return self.response.status_code

    @property
    def headers(self):
        return self.response.headers

    @property
    def content(self):
        return self.response.content

    def from_json(self):
        if self.response is None or self.response.content is None:
            return None
        else:
            try:
                return self.response.json()
            except Exception as e:
                if type(self.content) == str:
                    return self.response.content
                else:
                    # FIXME silently fail
                    return None

    def pretty_json(self):
        return json.dumps(self.from_json(), sort_keys=True, indent=2)

    def get_header(self, header: str) -> str:
        value = None
        if header in self.response.headers:
            value = self.response.headers[header]
        elif header.lower() in self.response.headers:
            value = self.response.headers[header.lower()]
        return value

    def get_location(self):
        return self.get_header('Location')

    def extract_cookie_value(self, name: str):
        if 'set-cookie' in self.response.headers:
            if type(self.response.headers['set-cookie']) == str:
                return self._extract_cookie_single_value(name, self.response.headers['set-cookie'])
            else:
                for header in self.response.headers['set-cookie']:
                    rval = self._extract_cookie_single_value(name, header)
                    if rval is not None:
                        return rval
        return None

    def _extract_cookie_single_value(self, name: str, header: str):
        cookie_parts = header.split(';')
        if len(cookie_parts) > 0:
            cookie_parts = cookie_parts[0].split('=')
            if len(cookie_parts) == 2 and cookie_parts[0] == name:
                return cookie_parts[1]
        return None

    def __str__(self):
        return self.response.content

class OpalResponseOld:
    """
    Response from Opal: code, headers and content
    """

    def __init__(self, code, headers, content):
        self.code = code
        self.headers = headers
        self.content = content

    def from_json(self):
        if self.content is None:
            return None
        else:
            try:
                return json.loads(self.content)
            except Exception as e:
                if type(self.content) == str:
                    return self.content
                else:
                    # FIXME silently fail
                    return None

    def pretty_json(self):
        return json.dumps(self.from_json(), sort_keys=True, indent=2)

    def get_header(self, header: str) -> str:
        value = None
        if header in self.headers:
            value = self.headers[header]
        elif header.lower() in self.headers:
            value = self.headers[header.lower()]
        return value

    def get_location(self):
        return self.get_header('Location')

    def extract_cookie_value(self, name: str):
        if 'set-cookie' in self.headers:
            if type(self.headers['set-cookie']) == str:
                return self._extract_cookie_single_value(name, self.headers['set-cookie'])
            else:
                for header in self.headers['set-cookie']:
                    rval = self._extract_cookie_single_value(name, header)
                    if rval is not None:
                        return rval
        return None

    def _extract_cookie_single_value(self, name: str, header: str):
        cookie_parts = header.split(';')
        if len(cookie_parts) > 0:
            cookie_parts = cookie_parts[0].split('=')
            if len(cookie_parts) == 2 and cookie_parts[0] == name:
                return cookie_parts[1]
        return None

    def __str__(self):
        return self.content

class Formatter:

    @classmethod
    def to_json(self, data: any, pretty: bool = False):
        if pretty:
            return json.dumps(data, sort_keys=True, indent=2)
        else:
            return json.dumps(data, sort_keys=True)

    @classmethod
    def print_json(self, data: any, pretty: bool = False):
        if data is not None:
            print(self.to_json(data, pretty))

class MagmaNameResolver:
    """
    Decode Magma fully qualified names.
    """

    def __init__(self, name):
        self.name = name
        self.datasource, sep, remain = name.partition('.')
        self.table, sep, self.variable = remain.partition(':')
        if len(self.table) == 0:
            self.table = None
        if len(self.variable) == 0:
            self.variable = None

    def is_datasources(self):
        return self.datasource == None or self.datasource == '*'

    def is_datasource(self):
        if self.table:
            return False
        else:
            return True

    def is_tables(self):
        return self.table == '*'

    def is_table(self):
        if self.table and not self.variable:
            return True
        else:
            return False

    def is_variables(self):
        return self.variable == '*'

    def is_variable(self):
        if self.variable:
            return True
        else:
            return False

    def get_ws(self):
        if self.is_datasources():
            if self.is_tables():
                return UriBuilder(['datasource', 'tables']).build()
            else:
                return UriBuilder(['datasources']).build()
        elif self.is_datasource():
            return UriBuilder(['datasource', self.datasource]).build()
        elif self.is_tables():
            return UriBuilder(['datasource', self.datasource, 'tables']).build()
        elif self.is_table():
            return self.get_table_ws()
        elif self.is_variables():
            return UriBuilder(['datasource', self.datasource, 'table', self.table, 'variables']).build()
        else:
            return self.get_variable_ws()

    def get_table_ws(self):
        return UriBuilder(['datasource', self.datasource, 'table', self.table]).build()

    def get_variable_ws(self):
        return UriBuilder(['datasource', self.datasource, 'table', self.table, 'variable', self.variable]).build()


class UriBuilder:
    """
    Build a valid Uri.
    """

    def __init__(self, path=[], params={}):
        self.path = path
        self.params = params

    def path(self, path):
        self.path = path
        return self

    def segment(self, seg):
        self.path.append(seg)
        return self

    def params(self, params):
        self.params = params
        return self

    def query(self, key, value):
        val = '%s' % value
        if type(value) == bool:
            val = val.lower()
        self.params.update([(key, val), ])
        return self

    def __str__(self):
        def concat_segment(p, s):
            return '%s/%s' % (p, s)

        def concat_params(k):
            return '%s=%s' % (urllib.parse.quote(k), urllib.parse.quote(str(self.params[k])))

        def concat_query(q, p):
            return '%s&%s' % (q, p)

        p = urllib.parse.quote('/' + reduce(concat_segment, self.path))
        if len(self.params):
            q = reduce(concat_query, list(map(concat_params, list(self.params.keys()))))
            return '%s?%s' % (p, q)
        else:
            return p

    def build(self):
        return self.__str__()

class HTTPError(Exception):
    def __init__(self, response: OpalResponse, message: str = None):
        # Call the base class constructor with the parameters it needs
        super().__init__(message if message else 'HTTP Error: %s' % response.code)
        self.code = response.code
        http_status = [x for x in list(HTTPStatus) if x.value == response.code][0]
        self.message = message if message else '%s: %s' % (http_status.phrase, http_status.description)
        self.error = response.from_json() if response.content else {'code': response.code, 'status': self.message}
        # case the reported error is not a dict
        if type(self.error) != dict:
            self.error = {'code': response.code, 'status': self.error}

    def is_client_error(self) -> bool:
        return self.code >= 400 and self.code < 500

    def is_server_error(self) -> bool:
        return self.code >= 500
