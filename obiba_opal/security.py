import obiba_opal.core as core


class EncryptService:
    """
    Encryption by Opal.
    """

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument("plain", help="Plain text to encrypt")

    @classmethod
    def do_command(cls, args):
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            request = client.new_request()
            request.fail_on_error()

            if args.verbose:
                request.verbose()

            response = (
                request.get().resource("/system/crypto/encrypt/" + args.plain).send()
            )
            print(response.content)
        finally:
            client.close()


class DecryptService:
    """
    Decryption by Opal.
    """

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument("encrypted", help="Encrypted text to decrypt")

    @classmethod
    def do_command(cls, args):
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            request = client.new_request()
            request.fail_on_error()

            if args.verbose:
                request.verbose()

            response = (
                request.get()
                .resource("/system/crypto/decrypt/" + args.encrypted)
                .send()
            )
            print(response.content)
        finally:
            client.close()
