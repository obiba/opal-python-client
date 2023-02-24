# Opal Python [![Build Status](https://app.travis-ci.com/obiba/opal-python-client.svg?branch=master)](https://app.travis-ci.com/github/obiba/opal-python-client)

This Python-based command line tool allows to access to a Opal server through its REST API. This is the perfect tool
for automating tasks in Opal using shell scripts. 

See also the [Opal R Client](https://github.com/obiba/opalr) which offers a comprehensive programming interface. 

* Read the [documentation](http://opaldoc.obiba.org).
* Have a bug or a question? Please create an issue on [GitHub](https://github.com/obiba/opal-python-client/issues).
* Continuous integration is on [Travis](https://travis-ci.org/obiba/opal-python-client).

## Usage

Install with:

```shell
pip install obiba-opal
```

Note: `obiba-opal` depends on `pycurl` which itself depends on system libraries (mostly OpenSSL related). In case `pycurl` installation
fails with **pip**, a quick fix is to install it via a system package:

```shell
# on Debian systems
sudo apt-get install python3-pycurl

# on RPM systems
sudo yum install python3-pycurl
```

To get the options of the command line:

```shell
opal --help
```

This command will display which sub-commands are available. For each sub-command you can get the help message as well:

```shell
opal <subcommand> --help
```

The objective of having sub-command is to hide the complexity of applying some use cases to the Opal REST API. More
sub-commands will be developed in the future.

## Development

Opal Python client can be easily extended by using the [exposed classes](https://github.com/obiba/opal-python-client/blob/master/obiba_opal/__init__.py). The classes `*Command` return an Opal task object, to be followed with the `TaskService`. The classes `*Service` perform immediate operations. 

```python
from obiba_opal import OpalClient, HTTPError, Formatter, ImportCSVCommand, TaskService, FileService, DictionaryService

# if 2-factor auth is enabled, user will be asked for the secret code
# Personal access token authentication is also supported (and recommended)
client = OpalClient.buildWithAuthentication(server='https://opal-demo.obiba.org', user='administrator', password='password')

try:
    # upload a local CSV data file into Opal file system
    fs = FileService(client)
    fs.upload_file('./data.csv', '/tmp')

    # import this CSV file into a project
    task = ImportCSVCommand(client).import_data('/tmp/data.csv', 'CNSIM')
    status = TaskService(client).wait_task(task['id'])
    
    # clean data file from Opal
    fs.delete_file('/tmp/data.csv')

    if status == 'SUCCEEDED':
        dico = DictionaryService(client)
        table = dico.get_table('CNSIM', 'data')
        # do something ...
        dico.delete_tables('CNSIM', ['data'])
    else:
        print('Import failed!')
        # do something ...
except HTTPError as e:
    Formatter.print_json(e.error, True)
finally:
    client.close()
```

## Mailing list

Have a question? Ask on our mailing list!

obiba-users@googlegroups.com

[http://groups.google.com/group/obiba-users](http://groups.google.com/group/obiba-users)

## License

OBiBa software are open source and made available under the [GPL3 licence](http://www.obiba.org/pages/license/). OBiBa software are free of charge.

## OBiBa acknowledgments

If you are using OBiBa software, please cite our work in your code, websites, publications or reports.

"The work presented herein was made possible using the OBiBa suite (www.obiba.org), a  software suite developed by Maelstrom Research (www.maelstrom-research.org)"
