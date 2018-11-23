"""
Opal analysis plugin.
"""

import sys

import opal.core
import opal.io
import opal.protobuf.Commands_pb2


def do_ws(args):
  """
  Build the web service resource path
  """
  if args.table is None:
    ws = "/project/%s/_export-analysis" % args.project
  else:
    ws = "/project/%s/table/%s/_export-analysis" % (args.project, args.table)

  return ws


def add_arguments(parser):
  """
  Add export analysis command specific options
  """
  parser.add_argument('--project', '-pr', required=True, help='Project name for which analysis data will be exported.')
  parser.add_argument('--table', '-t', required=False, help='Table name for which analysis data will be exported.')


def do_command(args):
  """
  Execute export anaysis command
  """
  # Build and send request
  try:
    request = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error().accept("application/zip")
    response = request.post().resource(do_ws(args)).send()
    print response.content

  except Exception, e:
    print >> sys.stderr, e
    sys.exit(2)
  except pycurl.error, error:
    errno, errstr = error
    print >> sys.stderr, 'An error occurred: ', errstr
    sys.exit(2)


