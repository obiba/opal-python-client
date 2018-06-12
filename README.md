# Opal Python [![Build Status](https://travis-ci.org/obiba/opal-python-client.svg?branch=master)](https://travis-ci.org/obiba/opal-python-client)

This Python-based command line tool allows to access to a Opal server through its REST API. This is the perfect tool
for automating tasks in Opal. This will be the preferred client developed when new features are added to the REST API.

* Read the [documentation](http://opaldoc.obiba.org).
* Have a bug or a question? Please create an issue on [GitHub](https://github.com/obiba/opal-python-client/issues).
* Continuous integration is on [Travis](https://travis-ci.org/obiba/opal-python-client).

## Usage

To get the options of the command line:

```
opal --help
```

This command will display which sub-commands are available. For each sub-command you can get the help message as well:

```
opal <subcommand> --help
```

The objective of having sub-command is to hide the complexity of applying some use cases to the Opal REST API. More
sub-commands will be developed in the future.

## Development

Opal Python client can be easily extended by using the classes defined in `core.py` and in `protobuf/*.py` files.

## Mailing list

Have a question? Ask on our mailing list!

obiba-users@googlegroups.com

[http://groups.google.com/group/obiba-users](http://groups.google.com/group/obiba-users)

## License

OBiBa software are open source and made available under the [GPL3 licence](http://www.obiba.org/pages/license/). OBiBa software are free of charge.

# OBiBa acknowledgments

If you are using OBiBa software, please cite our work in your code, websites, publications or reports.

"The work presented herein was made possible using the OBiBa suite (www.obiba.org), a  software suite developed by Maelstrom Research (www.maelstrom-research.org)"
