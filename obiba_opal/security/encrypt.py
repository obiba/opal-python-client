"""
Opal Encryption
"""

import obiba_opal.core as core


def add_arguments(parser):
    parser.add_argument('plain', help='Plain text to encrypt')


def do_command(args):
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error()

    if args.verbose:
        request.verbose()

    response = request.get().resource("/system/crypto/encrypt/" + args.plain).send()
    print(response.content)
