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
  ws = "/project/%s/commands/_export-analysis" % args.project

  return ws


def add_arguments(parser):
  """
  Add export analysis command specific options
  """
  parser.add_argument('--project', '-pr', required=True, help='Project name for which analysis data will be exported.')
  parser.add_argument('--tables', '-t', nargs='+', required=False, help='Table names for which analysis data will be exported.')


def do_command(args):
  """
  Execute export anaysis command
  """
  # Build and send request
  try:
    dto = opal.protobuf.Commands_pb2.ExportAnalysisCommandOptionsDto()
    dto.project = args.project

    if args.tables is not None:
      dto.tables.extend(args.tables)

    request = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error().accept_json().content_type_protobuf()
    request.post().resource(do_ws(args)).content(dto.SerializeToString()).send()

  except Exception, e:
    print e
    sys.exit(2)
  except pycurl.error, error:
    errno, errstr = error
    print >> sys.stderr, 'An error occurred: ', errstr
    sys.exit(2)


