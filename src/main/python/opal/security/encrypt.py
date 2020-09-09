"""
Opal Encryption
"""

import sys
import opal.core


def add_arguments(parser):
    parser.add_argument('plain', help='Plain text to encrypt')


def do_command(args):
    try:
        request = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args)).new_request()
        request.fail_on_error()

        if args.verbose:
            request.verbose()

        response = request.get().resource("/system/crypto/encrypt/" + args.plain).send()
        print(response.content)

    except Exception as e:
        print(e)
        sys.exit(2)
    except pycurl.error as error:
        errno, errstr = error
        print('An error occurred: ', errstr, file=sys.stderr)
        sys.exit(2)
