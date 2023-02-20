"""
Opal user management.
"""

import json
import obiba_opal.core as core


def add_arguments(parser):
    """
    Add data command specific options
    """
    parser.add_argument('--name', '-n', required=False, help='User name.')
    parser.add_argument('--upassword', '-upa', required=False, help='User password of at least six characters.')
    parser.add_argument('--ucertificate', '-uc', required=False, help='User certificate (public key) file')
    parser.add_argument('--disabled', '-di', action='store_true', required=False,
                        help='Disable user account (if omitted the user is enabled by default).')
    parser.add_argument('--groups', '-g', nargs='+', required=False, help='User groups')

    parser.add_argument('--fetch', '-fe', action='store_true', required=False,
                        help='Fetch one or multiple user(s).')
    parser.add_argument('--add', '-a', action='store_true', help='Add a user.')
    parser.add_argument('--update', '-ud', action='store_true', required=False, help='Update a user.')
    parser.add_argument('--delete', '-de', action='store_true', required=False,
                        help='Delete a user.')
    parser.add_argument('--json', '-j', action='store_true', help='Pretty JSON formatting of the response')


def do_ws(args):
    """
    Build the web service resource path
    """
    if args.add or (args.fetch and not args.name):
        ws = "/system/subject-credentials"
    else:
        ws = "/system/subject-credential/" + args.name

    return ws


def get_user_information(args):
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error()
    userInfo = request.get().resource(do_ws(args)).send().from_json()
    return userInfo


def do_command(args):
    """
    Execute group command
    """
    # Build and send request
    request = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error()

    if args.verbose:
        request.verbose()

    if args.fetch:
        # send request
        response = request.get().resource(do_ws(args)).send()
    elif args.add:
        if not args.name:
            raise Exception('A user name is required.')
        if not args.upassword and not args.ucertificate:
            raise Exception('A user password or a certificate file is required.')

        # create user
        user = {'name': args.name}
        if args.upassword:
            if len(args.upassword) < 6:
                raise Exception('Password must contain at least 6 characters.')
            user['authenticationType'] = 'PASSWORD'
            user['password'] = args.upassword
        else:
            user['authenticationType'] = 'CERTIFICATE'
            with open(args.ucertificate, 'rb') as cert:
                user['certificate'] = cert.read()

        if args.disabled:
            user.enabled = False

        if args.groups:
            user.groups.extend(args.groups)

        request.fail_on_error().accept_json().content_type_json()
        response = request.post().resource(do_ws(args)).content(json.dumps(user)).send()
    elif args.update:
        if not args.name:
            raise Exception('A user name is required.')

        userInfo = get_user_information(args)
        user = {'name': args.name}

        if args.upassword:
            if userInfo['authenticationType'] == "CERTIFICATE":
                raise Exception("%s requires a certificate (public key) file" % user.name)
            if len(args.upassword) < 6:
                raise Exception('Password must contain at least 6 characters.')
            user['authenticationType'] = 'PASSWORD'
            user['password'] = args.upassword
        elif args.ucertificate:
            if userInfo['authenticationType'] == "PASSWORD":
                raise Exception("%s requires a password" % user.name)

            user['authenticationType'] = 'CERTIFICATE'
            with open(args.ucertificate, 'rb') as cert:
                user['certificate'] = cert.read()
        else:
            user['authenticationType'] = userInfo['authenticationType']

        if args.disabled:
            user['enabled'] = False
        if args.groups:
            user['groups'] = args.groups

        request.fail_on_error().accept_json().content_type_json()
        response = request.put().resource(do_ws(args)).content(json.dumps(user)).send()
    elif args.delete:
        if not args.name:
            raise Exception('A user name is required.')

        response = request.delete().resource(do_ws(args)).send()

    # format response
    res = response.content
    if args.json:
        res = response.pretty_json()

    # output to stdout
    print(res)
