<API> Integrated Variant Information System (I-VIS)
===================================================

I-VIS has been developed as part of the [PREDICT] project which aims to
develop software solutions for **comPREhensive Data Integration for Cancer Treatment**.

I-VIS features an extensible framework build on top of [flask-2.0] to integrate relevant knowledge from diverse data sources
to guide the decision process in precision oncology.

Architecture
------------

The main architecture consists of:
- task based ETL process engine
- MySQL database
- API to access integrated data

The integration process of I-VIS is focused on essential concepts (entities), currently:
- genes
- variants
- cancer-types
- drugs

Entities are normalized with a modified version of [preon] and stored in a database. 
An REST-API allows to access the data and explore the relationship of entities across initially separate knowledge bases.
  
New entities and data sources can be added by implementing respective interfaces.

I-VIS consists of the following python packages:
* [i-vis-api] (main):
  Creates and monitors an I-VIS API server to provide and integrate data from multiple data sources.
* [i-vis-core] (required):
  Core functionality that is shared between the API and report generation.
* [i-vis-report] (optional):
  WEB-interface that utilizes I-VIS API to generate reports (under heavy development).

Further reading
---------------

Check the [manual][i-vis-api-doc] for further details on:
- recommended [installation](https://i-vis-api.readthedocs.org/en/latest/installation) and [configuration](https://i-vis-api.readthedocs.org/en/latest/configuration) 
  procedure
- list of [data sources](https://i-vis-api.readthedocs.org/en/latest/plugins/i-vis)


Requirements
------------

I-VIS has been developed and tested with Python 3.9

Among others, I-VIS makes use of the following python packages:
- [flask-2.0]
- [sqlalchemy-1.4]
- [dask]
- [pandas]

Recommended PC specs for running I-VIS API:
- more than 32GB RAM
- fast storage space, depending on plugin/data sources choice ~ 150GB
- docker client installed (optional)

Installation
------------

Currently, I-VIS API can be installed from the [repository][i-vis-api].
Clone the repository with:
<!--- TODO add docker installation --->

```console
$ git clone https://www.github.com/piechottam/i-vis-api
```

Use a [virtual environment](https://docs.python.org/3/tutorial/venv.html) to manage python package dependencies.
Now, install `i-vis-api` python package:

```console
$ pip install i-vis-api
```

Configuration
-------------

I-VIS API requires access to a MySQL database.
Check the [manual][i-vis-api-doc] for details on how to create and setup a database.

Use `conf/i-vis-minimal.conf` within the repository as a template and adjust the settings of your I-VIS API server:
- configure database -> `SQLALCHEMY_URI`
- set directories for data and logs -> `I_VIS_DATA_DIR` and `I_VIS_LOG_DIR`
- enable plugins -> `I_VIS_PLUGINS_IGNORE`
- configure plugins

Next, you need to set the environment variable `I_VIS_CONF` to point to the location of your configuration file.
Use absolute path and replace `<PATH>` placeholder: 

```console
$ export I_VIS_CONF = "<PATH>/i-vis-minimal.conf"
```

<!--- TODO Options that control logging can be adjusted in: `logging.yaml`. --->

Quickstart
----------

I-VIS API is shipped with the following convenience bash scripts to interact with [flask2.0]:
- `i-vis-api.sh`: Commands to start/stop I-VIS API
- `i-vis-cli.sh`: Commands to control and monitor the ETL process of I-VIS API

Running either script will give a brief usage description and available commands.

### Init database

The first step, is to initialize the database. This will create the necessary tables:
```console
$ i-vis-cli.sh init db
```

### Update

I-VIS features a simple version repository to update data sources to the newest available version. 
Each data source has three types of versions:
- *newest* version of (remote)
- *installed* version (local)
- *pending* version (local)

A *pending* version of a plugin is the version that is currently being installed and if the process succeeds, 
this version will become the new *installed* version.

Update the simple version repository and check for new available versions of data sources:

```console
$ i-vis-cli.sh plugin update
```

### Upgrade

Start upgrading data sources to the newest version with:

```console
$ i-vis-cli.sh plugin upgrade
```

Depending on your internet connection this might take some time.

### Authentification

The access to the API is restricted and requires a valid API token. The token needs to be attached to each individual API request:

* GET: `/api/<API request>?token=<API token>`
* POST: `token=<API token>`

Create a token by issuing the following command and providing values for `<user>` and `<mail>` placeholde:

```console
$ i-vis-cli.sh user create <user> <mail>
```

View token with:

```
$ i-vis-cli.sh user list
```

### Start API

Start the I-VIS API server with:

```console
$ i-vis-api.sh run
```

Depending on your setup, your I-VIS API instance should be reachable at: `http://localhost:5000/api`

# Swagger API

Explore the I-VIS API by studying your local swagger documentation at: `http://localhost:5000/api/swagger-ui`

If you are interested in the API and you want to have a peek without installing your own I-VIS API instance,
check out our [demo server][i-vis-api-swagger].

[i-vis-api]: https://www.github.com/piechottam/i-vis-api/
[i-vis-core]: https://www.github.com/piechottam/i-vis-core/
[i-vis-report]: https://www.github.com/piechottam/i-vis-report/
[i-vis-api-swagger]: https://predict.informatik.hu-berlin.de:4143/api/swagger-ui
[i-vis-api-doc]: https://i-vis-api.readthedocs.org/
[PREDICT]: https://predict.informatik.hu-berlin.de/
[preon]: https://github.com/ermshaua/preon/
[flask-2.0]: https://flask.palletsprojects.com/en/2.0.x/
[sqlalchemy-1.4]: https://docs.sqlalchemy.org/en/14/
[dask]: https://dask.org/
[pandas]: https://pandas.pydata.org/