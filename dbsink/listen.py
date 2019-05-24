#!python
# coding=utf-8
import json
import uuid

import sqlalchemy as sql
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert

import click
import msgpack
from easyavro import EasyAvroConsumer, EasyConsumer

from dbsink.tables import columns_and_message_conversion

import logging
log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream = logging.StreamHandler()
stream.setFormatter(log_format)

ea = logging.getLogger('easyavro')
ea.setLevel(logging.INFO)
ea.addHandler(stream)

L = logging.getLogger()
L.setLevel(logging.INFO)
L.addHandler(stream)


@click.command()
@click.option('--brokers',  type=str, required=True, default='localhost:4001', help="Kafka broker string (comman separated)")
@click.option('--topic',    type=str, required=True, default='axds-netcdf-replayer-data', help="Kafka topic to send the data to. '-value' is auto appended if using avro packing")
@click.option('--db',       type=str, required=True, default='postgresql+psycopg2://sink:sink@localhost:30300/sink', help="SQLAlchemy compatible postgres connection string")
@click.option('--schema',   type=str, required=True, default='public', help="Database schema to use (default: public)")
@click.option('--consumer', type=str, required=True, help="Consumer group to listen with")
@click.option('--packing',  type=click.Choice(['json', 'avro', 'msgpack']), default='json', help="The data unpacking algorithm to use")
@click.option('--registry', type=str, default='http://localhost:4002', help="URL to a Schema Registry if avro packing is requested")
@click.option('--drop/--no-drop', default=False, help="Drop the table first")
@click.option('--logfile',  type=str, default='', help="File to log messages to. Defaults to stdout.")
@click.option('--mockfile',  type=str, default='', help="File to pull messages from, for testsing.")
@click.option('--setup-only/--no-setup-only', default=False, help="Setup or drop tables but do not consume messages")
@click.option('-v', '--verbose', count=True)
def setup(brokers, topic, db, schema, consumer, packing, registry, drop, logfile, mockfile, setup_only, verbose):

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

    # Setup the kafka consuimer
    if packing == 'avro':
        unpacking_func = None
        c = EasyAvroConsumer(
            schema_registry_url=registry,
            kafka_brokers=brokers.split(','),
            consumer_group=consumer,
            kafka_topic=topic,
        )
    elif packing == 'msgpack':
        unpacking_func = lambda x: msgpack.loads(x, use_list=False, raw=False)  # noqa
        packing_func = lambda x: msgpack.packb(x, use_bin_type=True)  # noqa
        c = EasyConsumer(
            kafka_brokers=brokers.split(','),
            consumer_group=consumer,
            kafka_topic=topic,
        )
    elif packing == 'json':
        unpacking_func = json.loads
        packing_func = json.dumps
        c = EasyConsumer(
            kafka_brokers=brokers.split(','),
            consumer_group=consumer,
            kafka_topic=topic,
        )

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

    # Get the column definitions and the message to table conversion function
    newtopic, cols, message_to_values = columns_and_message_conversion(topic)

    if drop is True:
        L.info(f'Dropping table {newtopic}')
        engine.execute(sql.text(f'DROP TABLE IF EXISTS \"{newtopic}\"'))

    # Reflect to see if this table already exists. Create or update it.
    meta = sql.MetaData(engine, schema=schema)
    meta.reflect()
    if f'{schema}.{newtopic}' not in meta.tables:
        table = sql.Table(newtopic, meta, *cols)
    else:
        table = sql.Table(
            newtopic,
            meta,
            *cols,
            autoload=True,
            keep_existing=False,
            extend_existing=True
        )
    meta.create_all(tables=[table])

    def on_recieve(k, v):
        if v is not None and unpacking_func:
            try:
                v = unpacking_func(v)
            except BaseException:
                L.error(f'Error unpacking message using {packing}: {v}')
                return

        # Custom conversion function for the table
        newkey, newvalues = message_to_values(k, v)

        # I wonder if we can just do set_=v? Other seem to extract the
        # exact columns to update but this method is currently working...
        # https://gist.github.com/bhtucker/c40578a2fb3ca50b324e42ef9dce58e1
        insert_cmd = insert(table).values(newvalues)
        upsert_cmd = insert_cmd.on_conflict_do_update(
            constraint=f'{newtopic}_unique_constraint'.replace('-', '_'),
            set_=newvalues
        )
        res = engine.execute(upsert_cmd)
        res.close()
        L.debug(f'inserted/updated row {res.inserted_primary_key}')

    if not mockfile and not setup_only:
        c.consume(
            on_recieve=on_recieve,
            initial_wait=1,
            timeout=10,
            cleanup_every=100,
            loop=True
        )
    elif mockfile:
        # Purposly undocumented
        with open(mockfile) as f:
            messages = json.load(f)
            for m in messages:
                on_recieve(None, packing_func(m))


def run():
    setup(auto_envvar_prefix='DBSINK')


if __name__ == '__main__':
    run()
