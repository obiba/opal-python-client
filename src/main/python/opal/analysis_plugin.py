"""
Opal analysis plugin.
"""

import opal.core
import opal.io
import sys
import json
import opal.protobuf.Commands_pb2


def do_ws(args):
  """
  Build the web service resource path
  """
  ws = "/project/%s/commands/_analyse" % args.project

  return ws


def add_arguments(parser):
  """
  Add analyse command specific options
  """
  parser.add_argument('--project', '-pr', required=True, help='Project name')
  parser.add_argument('--config', '-c', required=True, help='A JSON file containing the analysis configuration')


def do_command(args):
  """
  Execute analysis
  """
  # Build and send request
  try:
    dto = OpalAnalysisDtoFactory.create(args.project, args.config)
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


def to_analyse_dto(config):
    dto = opal.protobuf.Commands_pb2.AnalyseCommandOptionsDto.AnalyseDto()
    dto.table = config['table']
    dto.name = config['name']
    dto.plugin = config['plugin']
    dto.template = config['template']
    dto.params = config['params']
    dto.variables = config['variables']
    return dto


class OpalAnalysisDtoFactory():

  @classmethod
  def create(self, project, config):
    """
    Create an analysis option DTO
    """

    dto = opal.protobuf.Commands_pb2.AnalyseCommandOptionsDto()
    dto.project = project
    configJson = json.loads(open(config, 'r').read())
    analyses = []

    if type(configJson) is list:
      for conf in configJson:
        analyses.append(to_analyse_dto(conf))
    else:
      analyses.append(to_analyse_dto(configJson))

    dto.analyses.extend(analyses)

    return dto
