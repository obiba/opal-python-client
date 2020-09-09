"""
Opal dictionary annotations extraction.
"""

import argparse
import csv
import opal.core
import pprint
import sys
import urllib.request, urllib.parse, urllib.error


def add_arguments(parser):
    """
    Add command specific options
    """
    parser.add_argument('--input', '-in', help='CSV/TSV input file, typically the output of the "export-annot" command (default is stdin)',
                        type=argparse.FileType('r'), default=sys.stdin)
    parser.add_argument('--locale', '-l', required=False,
                        help='Destination annotation locale (default is none)')
    parser.add_argument('--separator', '-s', required=False,
                        help='Separator char for CSV/TSV format (default is the tabulation character)')
    parser.add_argument('--destination', '-d', required=False,
                        help='Destination datasource name (default is the one(s) specified in the input file)')
    parser.add_argument('--tables', '-t', nargs='+', required=False,
                        help='The list of tables which variables are to be annotated (defaults to all that are found in the input file)')
    parser.add_argument('--taxonomies', '-tx', nargs='+', required=False,
                        help='The list of taxonomy names of interest (default is any that is found in the input file)')


def do_command(args):
    """
    Execute command
    """
    # Build and send request
    try:
        sep = csv_separator(args)
        reader = csv.reader(args.input, delimiter=sep)
        next(reader)  # skip header
        value_map = {}
        for row in reader:
            append_row(value_map, row, tables=args.tables, taxonomies=args.taxonomies)
        if args.verbose:
            pp = pprint.PrettyPrinter(indent=2)
            pp.pprint(value_map)
        for datasource in value_map:
            for table in value_map[datasource]:
                if not args.tables or table in args.tables:
                    for namespace in value_map[datasource][table]:
                        for name in value_map[datasource][table][namespace]:
                            for value in value_map[datasource][table][namespace][name]:
                                ds = args.destination if args.destination else datasource
                                variables = value_map[datasource][table][namespace][name][value]
                                annotate(args, ds, table, namespace, name, value, variables)
    except Exception as e:
        print(e)
        sys.exit(2)
    except pycurl.error as error:
        errno, errstr = error
        print('An error occurred: ', errstr, file=sys.stderr)
        sys.exit(2)


def annotate(args, datasource, table, namespace, name, value, variables):
    request = opal.core.OpalClient.build(opal.core.OpalClient.LoginInfo.parse(args)).new_request()
    request.fail_on_error().accept_json()
    params = {'namespace': namespace, 'name': name, 'value': value}

    if args.locale:
        params['locale'] = args.locale

    builder = opal.core.UriBuilder(['datasource', datasource, 'table', table, 'variables', '_attribute'], params=params)
    form = '&'.join([urllib.parse.urlencode({'variable': x}) for x in variables])
    if args.verbose:
        request.verbose()
    try:
        request.put().resource(builder.build()).content(form).send()
    except Exception as e:
        print('Error: Annotation failed for table ' + datasource + '.' + table)
        print(e)


def append_row(dictionary, row, tables=None, taxonomies=None):
    if row[0] not in dictionary:
        dictionary[row[0]] = {}
    append_table(dictionary, row, tables, taxonomies)


def append_table(dictionary, row, tables=None, taxonomies=None):
    if not tables or row[1] in tables:
        if row[1] not in dictionary[row[0]]:
            dictionary[row[0]][row[1]] = {}
        if not taxonomies or row[3] in taxonomies:
            append_taxonomy(dictionary, row)


def append_taxonomy(dictionary, row):
    if row[3] not in dictionary[row[0]][row[1]]:
        dictionary[row[0]][row[1]][row[3]] = {}
    append_vocabulary(dictionary, row)


def append_vocabulary(dictionary, row):
    if row[4] not in dictionary[row[0]][row[1]][row[3]]:
        dictionary[row[0]][row[1]][row[3]][row[4]] = {}
    append_value(dictionary, row)


def append_value(dictionary, row):
    if row[5] not in dictionary[row[0]][row[1]][row[3]][row[4]]:
        dictionary[row[0]][row[1]][row[3]][row[4]][row[5]] = []
    if row[2] not in dictionary[row[0]][row[1]][row[3]][row[4]][row[5]]:
        dictionary[row[0]][row[1]][row[3]][row[4]][row[5]].append(row[2])


def csv_separator(args):
    return args.separator if args.separator else '\t'
