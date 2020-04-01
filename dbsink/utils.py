#!python
# coding=utf-8
import uuid
import simplejson as json

import msgpack

from easyavro import EasyAvroConsumer, EasyConsumer

from dbsink import L


class MessageFiltered(Exception):
    pass


def get_kafka_consumer(brokers, topic, offset, packing, consumer=None, registry=None):

    # Generate a random consumer if one was not provided.
    # This guarentees a unique consumer ID for each run
    if not consumer:
        consumer = f'dbsink-{topic}-{uuid.uuid4().hex[0:20]}'
        L.info(f'Setting consumer to {consumer}')

    consumer_kwargs = {
        'kafka_brokers': brokers,
        'consumer_group': consumer,
        'kafka_topic': topic,
        'offset': offset
    }

    # Setup the kafka consuimer
    if packing == 'avro':
        unpacking_func = None
        packing_func = None
        consumer_class = EasyAvroConsumer
        if not registry:
            raise ValueError('Avro packing requestd but no schema registry url was found!')
        consumer_kwargs.update({
            'schema_registry_url': registry,
        })
    elif packing == 'msgpack':
        unpacking_func = lambda x: msgpack.loads(x, use_list=False, raw=False)  # noqa
        packing_func = lambda x: msgpack.packb(x, use_bin_type=True)  # noqa
        consumer_class = EasyConsumer
    elif packing == 'json':
        unpacking_func = json.loads
        packing_func = lambda x: json.dumps(x, ignore_nan=True)  # noqa
        consumer_class = EasyConsumer

    return consumer_class, consumer_kwargs, unpacking_func, packing_func


def listen_unpack(brokers, topic, offset, packing, mapping, consumer=None, registry=None, on_receive=None, loop=False):

    consume_cls, consume_kw, unpack, _ = get_kafka_consumer(
        brokers=brokers.split(','),
        topic=topic,
        offset=offset,
        packing=packing,
        consumer=consumer,
        registry=registry
    )

    if on_receive is None:
        def on_receive(k, v):
            L.info("Recieved message:\nKey: {}\nValue: {}".format(k, v))

    def unpack_receive(k, v):
        if v is not None and unpack:
            try:
                v = unpack(v)
            except BaseException:
                L.error(f'Error unpacking message using {packing}: {v}')
                return

        try:
            nk, nv = mapping.message_to_values(k, v)
        except MessageFiltered as e:
            L.debug(e)
            return
        except BaseException as e:
            L.error(f'Skipping {v} - {repr(e)}')
            return

        on_receive(nk, nv)

    c = consume_cls(**consume_kw)
    c.consume(
        on_recieve=unpack_receive,
        initial_wait=1,
        timeout=10,
        cleanup_every=1000,
        loop=loop
    )
