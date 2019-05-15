#!python
# coding=utf-8
import json
import uuid

import sqlalchemy as sql
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
@click.option('--setup-only/--no-setup-only', default=False, help="Setup mode will setup tables but not consume messages")
@click.option('-v', '--verbose', count=True)
def setup(brokers, topic, db, schema, consumer, packing, registry, drop, logfile, setup_only, verbose):

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
        echo=False
    )

    # Create schema
    engine.execute(f"CREATE SCHEMA if not exists {schema}")

    # Add HSTORE extension
    engine.execute("CREATE EXTENSION if not exists hstore cascade")

    # Get the column definitions and the message to table conversion function
    cols, message_to_values = columns_and_message_conversion(topic)

    if drop is True:
        L.info(f'Dropping column {topic}')
        engine.execute(sql.text(f'DROP TABLE IF EXISTS \"{topic}\"'))

    # Reflect to see if this table already exists. Create or update it.
    meta = sql.MetaData(engine, schema=schema)
    meta.reflect()
    if f'{schema}.{topic}' not in meta.tables:
        table = sql.Table(topic, meta, *cols)
    else:
        table = sql.Table(topic, meta, *cols, autoload=True, keep_existing=False, extend_existing=True)
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
            constraint=f'{topic}_unique_constraint'.replace('-', '_'),
            set_=newvalues
        )
        res = engine.execute(upsert_cmd)
        res.close()
        L.debug(f'inserted/updated row {res.inserted_primary_key}')

    # recs = [
    #     packing_func({'uid': '1', 'gid': None, 'time': '2019-05-07T19:57:56', 'lat': 33.9266471862793, 'lon': -118.7137451171875, 'z': None, 'values': {'float_id': 47645, '_7': 55800.0, '_8': -118.7137451171875, '_9': 33.9266471862793, '_10': 0.0, '_11': 25.598445892333984, '_12': 33.13822937011719, '_13': 0.0, '_14': 21.14401626586914, '_15': 25.598445892333984, '_16': 0.0, '_17': 20.705978393554688, '_18': 21.14401626586914, '_19': 0.0, '_20': 16.726125717163086, '_21': 20.705978393554688, '_22': 0.0, '_23': 16.674354553222656, '_24': 16.726125717163086, '_25': 0.0, '_26': 16.57587432861328, '_27': 16.674354553222656, '_28': 0.0, '_29': 14.853745460510254, '_30': 16.57587432861328, '_31': 0.0, '_32': 14.835172653198242, '_33': 14.853745460510254, '_34': 0.0, '_35': 14.226363182067871, '_36': 14.835172653198242}}),
    #     packing_func({'uid': '1', 'gid': None, 'time': '2019-05-07T19:57:56', 'lat': 33.925960540771484, 'lon': -118.71289825439453, 'z': None, 'values': {'float_id': 47645, '_7': 56700.0, '_8': -118.71289825439453, '_9': 33.925960540771484, '_10': 0.0, '_11': 25.681798934936523, '_12': 33.28643798828125, '_13': 0.0, '_14': 21.236486434936523, '_15': 25.681798934936523, '_16': 0.0, '_17': 20.78787612915039, '_18': 21.236486434936523, '_19': 0.0, '_20': 16.809663772583008, '_21': 20.78787612915039, '_22': 0.0, '_23': 16.67538833618164, '_24': 16.809663772583008, '_25': 0.0, '_26': 16.65082550048828, '_27': 16.67538833618164, '_28': 0.0, '_29': 14.887101173400879, '_30': 16.65082550048828, '_31': 0.0, '_32': 14.873950004577637, '_33': 14.887101173400879, '_34': 0.0, '_35': 14.287884712219238, '_36': 14.873950004577637}}),
    #     packing_func({'uid': '1', 'gid': None, 'time': '2019-05-07T19:57:56', 'lat': 33.9253044128418, 'lon': -118.71210479736328, 'z': None, 'values': {'float_id': 47645, '_7': 57600.0, '_8': -118.71210479736328, '_9': 33.9253044128418, '_10': 0.0, '_11': 25.760086059570312, '_12': 33.425289154052734, '_13': 0.0, '_14': 21.31116485595703, '_15': 25.760086059570312, '_16': 0.0, '_17': 20.872238159179688, '_18': 21.31116485595703, '_19': 0.0, '_20': 16.889360427856445, '_21': 20.872238159179688, '_22': 0.0, '_23': 16.721893310546875, '_24': 16.889360427856445, '_25': 0.0, '_26': 16.679943084716797, '_27': 16.721893310546875, '_28': 0.0, '_29': 14.944218635559082, '_30': 16.679943084716797, '_31': 0.0, '_32': 14.900228500366211, '_33': 14.944218635559082, '_34': 0.0, '_35': 14.355060577392578, '_36': 14.900228500366211}}),
    #     packing_func({'uid': '1', 'gid': None, 'time': '2019-05-07T19:57:56', 'lat': 33.92466735839844, 'lon': -118.71137237548828, 'z': None, 'values': {'float_id': 47645, '_7': 58500.0, '_8': -118.71137237548828, '_9': 33.92466735839844, '_10': 0.0, '_11': 25.832063674926758, '_12': 33.55484390258789, '_13': 0.0, '_14': 21.33756446838379, '_15': 25.832063674926758, '_16': 0.0, '_17': 20.961849212646484, '_18': 21.33756446838379, '_19': 0.0, '_20': 16.96350860595703, '_21': 20.961849212646484, '_22': 0.0, '_23': 16.789194107055664, '_24': 16.96350860595703, '_25': 0.0, '_26': 16.67693328857422, '_27': 16.789194107055664, '_28': 0.0, '_29': 15.00782585144043, '_30': 16.67693328857422, '_31': 0.0, '_32': 14.932941436767578, '_33': 15.00782585144043, '_34': 0.0, '_35': 14.422245979309082, '_36': 14.932941436767578}}),
    # ]

    # for r in recs:
    #     on_recieve(None, r)

    if not setup_only:
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
