#!python
# coding=utf-8
import uuid
import pkg_resources
import simplejson as json

import click
import msgpack
import sqlalchemy as sql
from sqlalchemy.dialects.postgresql import insert
from easyavro import EasyAvroConsumer, EasyConsumer

import logging
log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream = logging.StreamHandler()
stream.setFormatter(log_format)

ea = logging.getLogger('easyavro')
ea.setLevel(logging.INFO)
ea.addHandler(stream)

L = logging.getLogger()
L.setLevel(logging.INFO)
L.handlers = [stream]


def get_mappings():
    return {
        e.name: e.resolve() for e in pkg_resources.iter_entry_points('dbsink.maps')
    }


@click.command()
@click.option('--brokers',  type=str, required=True, default='localhost:4001', help="Kafka broker string (comman separated).")
@click.option('--topic',    type=str, required=True, default='axds-netcdf-replayer-data', help="Kafka topic to send the data to. '-value' is auto appended if using avro packing.")
@click.option('--table',    type=str, required=False, default='', help="Name of the table to sink into. Defaults to the topic name.")
@click.option('--lookup',   type=str, required=False, default='JsonMap', help="Lookup name to use to find the correct table format (default: the topic name).")
@click.option('--db',       type=str, required=True, default='postgresql+psycopg2://sink:sink@localhost:30300/sink', help="SQLAlchemy compatible postgres connection string.")
@click.option('--schema',   type=str, required=True, default='public', help="Database schema to use (default: public).")
@click.option('--consumer', type=str, default='', help="Consumer group to listen with (default: random).")
@click.option('--offset',   type=str, required=True, default='largest', help="Kafka offset to start with (default: largest).")
@click.option('--packing',  type=click.Choice(['json', 'avro', 'msgpack']), default='json', help="The data unpacking algorithm to use (default: json).")
@click.option('--registry', type=str, default='http://localhost:4002', help="URL to a Schema Registry if avro packing is requested")
@click.option('--drop/--no-drop', default=False, help="Drop the table first")
@click.option('--logfile',  type=str, default='', help="File to log messages to (default: stdout).")
@click.option('--listen/--no-listen', default=True, help="Whether to listen for messages.")
@click.option('--do-inserts/--no-do-inserts', default=True, help="Whether to insert data into a database.")
@click.option('--datafile', type=str, default='', help="File to pull messages from instead of listening for messages.")
@click.option('-v', '--verbose', count=True, help="Control the output verbosity, use up to 3 times (-vvv)")
def setup(brokers, topic, table, lookup, db, schema, consumer, offset, packing, registry, drop, logfile, listen, do_inserts, datafile, verbose):

    if logfile:
        handler = logging.FileHandler(logfile)
        handler.setFormatter(log_format)
        ea.addHandler(handler)
        L.addHandler(handler)

    if verbose == 0:
        ea.setLevel(logging.INFO)
        L.setLevel(logging.INFO)
    elif verbose >= 1:
        ea.setLevel(logging.DEBUG)
        L.setLevel(logging.DEBUG)

    # Generate a random consumer if one was not provided.
    # This guarentees a unique consumer ID for each run
    if not consumer:
        consumer = f'dbsink-{topic}-{uuid.uuid4().hex[0:20]}'
        L.info(f'Setting consumer to {consumer}')

    # If no specific table was specified, use the topic name
    if not table:
        table = topic

    # Setup the kafka consuimer
    if packing == 'avro':
        unpacking_func = None
        consumer_class = EasyAvroConsumer
        consumer_kwargs = {
            'schema_registry_url': registry,
            'kafka_brokers': brokers.split(','),
            'consumer_group': consumer,
            'kafka_topic': topic,
            'offset': offset
        }
    elif packing == 'msgpack':
        unpacking_func = lambda x: msgpack.loads(x, use_list=False, raw=False)  # noqa
        packing_func = lambda x: msgpack.packb(x, use_bin_type=True)  # noqa
        consumer_class = EasyConsumer
        consumer_kwargs = {
            'kafka_brokers': brokers.split(','),
            'consumer_group': consumer,
            'kafka_topic': topic,
            'offset': offset
        }
    elif packing == 'json':
        unpacking_func = json.loads
        packing_func = lambda x: json.dumps(x, ignore_nan=True)  # noqa
        consumer_class = EasyConsumer
        consumer_kwargs = {
            'kafka_brokers': brokers.split(','),
            'consumer_group': consumer,
            'kafka_topic': topic,
            'offset': offset
        }

    # Get the mapping object from the lookup parameter
    mappings = get_mappings()
    mapping = mappings[lookup](topic, table=table)
    L.debug(f'Using mapping: {lookup}, topic: {topic}, table: {mapping.table}')

    if do_inserts is True:
        """ Database connection and setup
        """

        engine = sql.create_engine(
            db,
            pool_size=5,
            max_overflow=100,
            pool_recycle=3600,
            pool_pre_ping=True,
            client_encoding='utf8',
            use_native_hstore=True,
            echo=verbose >= 2
        )
        # Create schema
        engine.execute(f"CREATE SCHEMA if not exists {schema}")

        # Add HSTORE extension
        engine.execute("CREATE EXTENSION if not exists hstore cascade")

        if drop is True:
            L.info(f'Dropping table {mapping.table}')
            engine.execute(sql.text(f'DROP TABLE IF EXISTS \"{mapping.table}\"'))

        # Reflect to see if this table already exists. Create or update it.
        meta = sql.MetaData(engine, schema=schema)
        meta.reflect()
        if f'{schema}.{mapping.table}' not in meta.tables:
            sqltable = sql.Table(mapping.table, meta, *mapping.schema)
        else:
            sqltable = sql.Table(
                mapping.table,
                meta,
                *mapping.schema,
                autoload=True,
                keep_existing=False,
                extend_existing=True
            )
        meta.create_all(tables=[sqltable])

    def on_recieve(k, v):
        if v is not None and unpacking_func:
            try:
                v = unpacking_func(v)
            except BaseException:
                L.error(f'Error unpacking message using {packing}: {v}')
                return

        # Custom conversion function for the table
        try:
            newkey, newvalues = mapping.message_to_values(k, v)
        except BaseException as e:
            L.error(f'Skipping {v}, message could not be converted to a row - {repr(e)}')
            return

        if do_inserts:
            # I wonder if we can just do set_=v? Other seem to extract the
            # exact columns to update but this method is currently working...
            # https://gist.github.com/bhtucker/c40578a2fb3ca50b324e42ef9dce58e1
            insert_cmd = insert(sqltable).values(newvalues)
            if mapping.upsert_constraint_name is not None:
                upsert_cmd = insert_cmd.on_conflict_do_update(
                    constraint=mapping.upsert_constraint_name,
                    set_=newvalues
                )
                res = engine.execute(upsert_cmd)
            else:
                res = engine.execute(insert_cmd)
            res.close()
            L.debug(f'inserted/updated row {res.inserted_primary_key}')

    if datafile:
        with open(datafile) as f:
            messages = json.load(f)
            for m in messages:
                on_recieve(None, packing_func(m))
    elif listen is True:
        c = consumer_class(**consumer_kwargs)
        c.consume(
            on_recieve=on_recieve,
            initial_wait=1,
            timeout=10,
            cleanup_every=100,
            loop=True
        )



def run():
    setup(auto_envvar_prefix='DBSINK')


if __name__ == '__main__':
    run()
