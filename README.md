## dbsink

Read from a kafka topic and sink to a database table, one row per message.

This is not unlike the Kafka Connect JdbcConnector. This project has a much lower bar of entry and doesn't require diving into the Kafka Connect ecosystem. I wrote the equivilent to this project using a custom JdbcConnector and it was getting out of control and was basically un-testable. So here we are.

You can choose to unpack the data as `avro`, `msgpack` or the default `json`. `avro` requires an additional `registry` parameter.

This assumes a certain structure of the data (see `schema.avsc`).


## WHY?

I needed to read from well-defined kafka topics and store the results in a database table so collaborators could interact with the data in a more farmiliar way.

It is also a very convienent and easy to setup PostgREST on top of the resulting tables to get a quick read-only REST API on top of the tabled messages.

## Mapping messages to tables

You can define your topic to table mapping in `tables.py` using SQLAlchemy syntax. There is no way to load external mappings at this time but it is something I would like to add. For now, if you have a new topic you will need to edit the `topic_to_func` dictionary and create a function that maps your message format to a flat database table.

## Configuration

This program uses [`Click`](https://click.palletsprojects.com/) for the CLI interface.

```sh
$ dbsink --help
Usage: dbsink [OPTIONS]

Options:
  --brokers TEXT                  Kafka broker string (comman separated)
                                  [required]
  --topic TEXT                    Kafka topic to send the data to. '-value' is
                                  auto appended if using avro packing
                                  [required]
  --db TEXT                       SQLAlchemy compatible postgres connection
                                  string  [required]
  --schema TEXT                   Database schema to use (default: public)
                                  [required]
  --consumer TEXT                 Consumer group to listen with  [required]
  --packing [json|avro|msgpack]   The data unpacking algorithm to use
  --registry TEXT                 URL to a Schema Registry if avro packing is
                                  requested
  --drop / --no-drop              Drop the table first
  --logfile TEXT                  File to log messages to. Defaults to stdout.
  --setup-only / --no-setup-only  Setup mode will setup tables but not consume
                                  messages
  -v, --verbose
  --help                          Show this message and exit.
```

## Environmental Variables

All configuration options can be specified with environmental variables using the pattern `DBSINK_[argument_name]=[value]`. For more information see [the click documentation](https://click.palletsprojects.com/en/7.x/options/?highlight=auto_envvar_prefix#values-from-environment-variables).

```bash
DBSINK_TOPIC="axds-netcdf-replayer-data" \
DBSINK_CONSUMER='myconsumer' \
DBSINK_PACKING='msgpack' \
    dbsink
```
