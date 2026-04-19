"""
Opal file management.
"""

import json
import sys
import os

import obiba_opal.core as core
from obiba_opal.system import TaskService


class FileService:
    """
    File management service.
    """

    def __init__(self, client: core.OpalClient, verbose: bool = False):
        self.client = client
        self.verbose = verbose

    @classmethod
    def add_arguments(self, parser):
        """
        Add file command specific options
        """
        parser.add_argument("path", help="File path in Opal file system.")
        parser.add_argument(
            "--download",
            "-dl",
            action="store_true",
            help="Download file, or folder (as a zip file).",
        )
        parser.add_argument("--download-password", "-dlp", help="Password to encrypt the file content.")
        parser.add_argument(
            "--upload",
            "-up",
            required=False,
            help="Upload a local file to a folder in Opal file system.",
        )
        parser.add_argument(
            "--delete",
            "-dt",
            action="store_true",
            help="Delete a file on Opal file system.",
        )
        parser.add_argument("--force", "-f", action="store_true", help="Skip confirmation.")
        parser.add_argument(
            "--json",
            "-j",
            action="store_true",
            help="Pretty JSON formatting of the response",
        )

    @classmethod
    def do_command(self, args):
        """
        Execute file command
        """
        # Build and send request
        client = core.OpalClient.build(core.OpalClient.LoginInfo.parse(args))
        try:
            service = FileService(client, args.verbose)

            # send request
            if args.download or args.download_password:
                service.download_file(args.path, sys.stdout.fileno(), args.download_password)
            else:
                if args.upload:
                    service.upload_file(args.upload, args.path)
                elif args.delete:
                    # confirm
                    if args.force:
                        service.delete_file(args.path)
                    else:
                        confirmed = input('Delete the file "' + args.path + '"? [y/N]: ')
                        if confirmed == "y":
                            service.delete_file(args.path)
                        else:
                            print("Aborted.")
                            sys.exit(0)
                else:
                    res = service.file_info(args.path)
                    core.Formatter.print_json(res, args.json)
        finally:
            client.close()

    def download_file(self, path: str, fd: int | os.PathLike, download_password: str = None):
        """
        Download a file.

        :param path: The file path in Opal
        :param fd: The file descriptor or path to the destination file
        :param download_password: The password to use to encrypt the
                                downloaded zip archive
        :raises Exception: If the file is not readable
        """
        info = self.file_info(path)
        if not info["readable"]:
            raise Exception(f"File {path} is not readable")
        request = self.client.new_request()
        request.fail_on_error()

        if self.verbose:
            request.verbose()

        file = FileService.OpalFile(path)
        fp = os.fdopen(fd, "wb") if isinstance(fd, int) else fd

        if self.client.compare_version("5.7.0") < 0:
            # File download before Opal 5.7.0
            request.get().resource(file.get_ws()).accept("*/*").header("X-File-Key", download_password).send(fp)
            fp.flush()
        else:
            # File download with Opal 5.7.0 or later
            if download_password or info["type"] == "FOLDER":
                # File to be bundled as zip archive
                options = file.make_bundle_options(download_password)
                bundle_request = self.client.new_request().fail_on_error()
                if self.verbose:
                    bundle_request.verbose()
                response = bundle_request.post().resource("/shell/commands/_file-bundle").accept_json().content_type_json().content(json.dumps(options)).send()
                task = response.from_json()
                task_service = TaskService(self.client)
                try:
                    status = task_service.wait_task(task["id"], True)
                    if status == "SUCCEEDED":
                        # Task succeeded, downloading file
                        download_request = self.client.new_request().fail_on_error()
                        if self.verbose:
                            download_request.verbose()
                        download_request.get().resource(f"/shell/command/{task['id']}/_result").accept("*/*").send(fp)
                        fp.flush()
                    else:
                        raise Exception(f"File bundle task failed with status: {status}")
                finally:
                    # Ensure task is deleted in any case to avoid leaving pending tasks on the server
                    delete_request = self.client.new_request().fail_on_error()
                    if self.verbose:
                        delete_request.verbose()
                    delete_request.delete().resource(f"/shell/command/{task['id']}").send()
            else:
                # File to be downloaded as is
                request.get().resource(file.get_ws()).accept("*/*").header("X-File-Key", download_password).send(fp)
                fp.flush()

    def upload_file(self, upload: str, path: str):
        """
        Upload a file to Opal.

        :param path: The destination folder path in Opal
        :param upload: The source file path to upload
        """
        request = self.client.new_request()
        request.fail_on_error().accept_json()

        if self.verbose:
            request.verbose()

        file = FileService.OpalFile(path)

        request.content_upload(upload).accept("text/html")
        request.post().resource(file.get_ws()).send()

    def delete_file(self, path: str):
        """
        Delete a file in Opal.

        :param path: The destination file path in Opal
        """
        request = self.client.new_request()
        request.fail_on_error().accept_json()

        if self.verbose:
            request.verbose()

        file = FileService.OpalFile(path)

        request.delete().resource(file.get_ws()).send()

    def file_info(self, path) -> dict:
        """
        Get information about a file in Opal.

        :param path: The destination file path in Opal
        """
        request = self.client.new_request()
        request.fail_on_error().accept_json()

        if self.verbose:
            request.verbose()

        file = FileService.OpalFile(path)

        response = request.get().resource(file.get_meta_ws()).send()
        return response.from_json()

    class OpalFile:
        """
        File on Opal file system
        """

        def __init__(self, path):
            self.path = path

        def get_meta_ws(self):
            return f"/files/_meta{self.path}"

        def get_ws(self):
            return f"/files{self.path}"

        def make_bundle_options(self, download_password):
            options = {
                "paths": [self.path]
            }
            if download_password:
                options["password"] = download_password
            return options
