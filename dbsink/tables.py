#!python
# coding=utf-8
import re
from copy import copy
import simplejson as json
from datetime import datetime

from dateutil.parser import parse as dtparse

import sqlalchemy as sql
from sqlalchemy.dialects.postgresql import UUID, HSTORE, JSON, JSONB

import logging
log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream = logging.StreamHandler()
stream.setFormatter(log_format)

L = logging.getLogger()
L.setLevel(logging.INFO)
L.handlers = [stream]

xx = re.compile(r'[\x00-\x1f\\"]')
ux = re.compile(r'[\\u[0-9A-Fa-f]]')


def make_valid_string(obj):
    try:
        return ux.sub(
            '',
            xx.sub(
                '',
                obj
            )
        ).replace(
            '\x80',
            ''
        ).replace(
            '\x00',
            ''
        )
    except BaseException:
        return obj


def payload_parse(payload):
    # Make sure we have valid JSON and remove any
    # Infinity and NaN values in the process
    try:
        return json.loads(json.dumps(payload, ignore_nan=True))
    except BaseException as e:
        raise ValueError(f'Could not parse message as valid JSON - {repr(e)}')


def columns_and_message_conversion(topic, lookup=None):

    lookup = lookup or topic

    if lookup in topic_to_func:
        return topic_to_func[lookup](topic)
    else:
        return default_func(topic)


def generic_cols(topic):

    newtopic = topic.replace('.', '-')

    constraint_name = f'{newtopic}_unique_constraint'.replace('-', '_')

    cols = [
        sql.Column('id',       sql.Integer, sql.Sequence(f'{newtopic}_id_seq'), primary_key=True),
        sql.Column('uid',      sql.String, index=True),
        sql.Column('gid',      sql.String, default='', index=True),
        sql.Column('time',     sql.DateTime(timezone=False), index=True),
        sql.Column('reftime',  sql.DateTime(timezone=False), index=True),
        sql.Column('lat',      sql.REAL, index=True, default=0),
        sql.Column('lon',      sql.REAL, index=True, default=0),
        sql.Column('z',        sql.REAL, default=0.0, index=True),
        sql.Column('values',   HSTORE, default={}),
        sql.Column('meta',     JSONB, default={}),
        sql.Column('payload',  sql.Text, default=''),
        sql.Index(
            f'{newtopic}_unique_idx'.replace('-', '_'),
            'uid',
            'gid',
            'time',
            'lat',
            'lon',
            'z',
            unique=True,
        ),
        sql.UniqueConstraint(
            'uid',
            'gid',
            'time',
            'lat',
            'lon',
            'z',
            name=constraint_name
        )
    ]

    return newtopic, cols, constraint_name


def generic_float_data(topic):

    newtopic, cols, constraint_name = generic_cols(topic)

    def message_to_values(key, value):
        payload = payload_parse(value)

        # All HSTORE values need to be strings
        if value['values']:
            value['values'] = { k: make_valid_string(str(x)) for k, x in value['values'].items() }

        value['payload'] = json.dumps(payload, allow_nan=False)

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in value.items() if v }

    return newtopic, cols, constraint_name, message_to_values


def arete_data(topic):
    newtopic, cols, constraint_name = generic_cols(topic)

    def message_to_values(key, value):

        value['json']['not_decoded'] = value['json']['not_decoded'].encode('utf-8').decode()

        payload = payload_parse(value)

        headers = value['headers'].copy()
        values = value['json'].copy()

        values['mfr'] = value['mfr']
        values['cdr_reference'] = value['cdr_reference']
        values['cep_radius'] = headers['location']['cep_radius']

        # Time - use float timestamp and fall back to Iridium
        reftime = datetime.utcfromtimestamp(headers['iridium_ts'])
        if 'status_ts' in values and values['status_ts']:
            timestamp = datetime.utcfromtimestamp(values['status_ts'])
        else:
            timestamp = reftime

        # Location - Use value locations and fall back to Iridium
        latdeg = float(headers['location']['latitude']['degrees'])
        latmin = float(headers['location']['latitude']['minutes'])
        values['iridium_lat'] = latdeg + (latmin / 60)
        if 'latitude' in values and values['latitude']:
            latdd = values['latitude']
        else:
            latdd = values['iridium_lat']

        londeg = float(headers['location']['longitude']['degrees'])
        lonmin = float(headers['location']['longitude']['minutes'])
        values['iridium_lon'] = londeg + (lonmin / 60)
        if 'longitude' in values and values['longitude']:
            londd = values['longitude']
        else:
            londd = values['iridium_lon']

        top_level = {
            'uid':     str(headers['imei']),
            'gid':     None,
            'time':    timestamp.isoformat(),
            'reftime': reftime.isoformat(),
            'lat':     latdd,
            'lon':     londd,
            'z':       None,
            'payload': json.dumps(payload, allow_nan=False)
        }

        del headers['imei']
        del headers['location']

        fullvalues = {
            **top_level,
            'values': {
                **values,
                **headers,
            }
        }

        # All HSTORE values need to be strings
        if fullvalues['values']:
            fullvalues['values'] = {
                k: make_valid_string(str(x)) if x else None
                for k, x in fullvalues['values'].items()
            }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v }

    return newtopic, cols, constraint_name, message_to_values


def numurus_status(topic):

    newtopic, cols, constraint_name = generic_cols(topic)

    def message_to_values(key, value):
        payload = payload_parse(value)

        skips = ['timestamp', 'imei', 'latitude', 'longitude']

        top_level = {
            'uid':     value['imei'],
            'gid':     None,
            'time':    dtparse(value['timestamp']).isoformat(),
            'reftime': dtparse(value['timestamp']).isoformat(),
            'lat':     value['latitude'],
            'lon':     value['longitude'],
            'z':       None,
            'payload': json.dumps(payload, allow_nan=False)
        }

        # All HSTORE values need to be strings
        values = { k: make_valid_string(str(x)) if x else None for k, x in value.items() if k not in skips }
        values['mfr'] = 'numurus'

        fullvalues = {
            **top_level,
            'values': values
        }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v }

    return newtopic, cols, constraint_name, message_to_values


def just_json(topic):
    """
    {
        ...whatever
    }
    """
    newtopic = topic.replace('.', '-')

    cols = [
        sql.Column('id',       sql.Integer, sql.Sequence(f'{newtopic}_id_seq'), primary_key=True),
        sql.Column('sinked',   sql.DateTime(timezone=False), index=True),
        sql.Column('key',      sql.String, default='', index=True),
        sql.Column('payload',  JSONB)
    ]

    def message_to_values(key, value):
        payload = payload_parse(value)

        values = {
            'sinked':  datetime.utcnow().isoformat(),
            'key':     key,
            'payload': payload,
        }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in values.items() if v }

    constraint_name = None
    return newtopic, cols, constraint_name, message_to_values


def float_reports(topic):

    newtopic, cols, constraint_name = generic_cols(topic)

    def message_to_values(key, value):
        payload = payload_parse(value)

        headers = value['headers'].copy()
        values = value['values'].copy()

        values['mfr'] = value['mfr']
        values['cdr_reference'] = value['cdr_reference']
        values['cep_radius'] = headers['location']['cep_radius']

        # Time - use float timestamp and fall back to Iridium
        reftime = datetime.utcfromtimestamp(headers['iridium_ts'])
        if 'status_ts' in values and values['status_ts']:
            timestamp = datetime.utcfromtimestamp(values['status_ts'])
        elif 'environmental_ts' in values and values['environmental_ts']:
            timestamp = datetime.utcfromtimestamp(values['environmental_ts'])
        elif 'mission_ts' in values and values['mission_ts']:
            timestamp = datetime.utcfromtimestamp(values['mission_ts'])
        else:
            timestamp = reftime

        # Location - Use value locations and fall back to Iridium
        latdeg = float(headers['location']['latitude']['degrees'])
        latmin = float(headers['location']['latitude']['minutes'])
        values['iridium_lat'] = latdeg + (latmin / 60)
        if 'latitude' in values and values['latitude']:
            latdd = values['latitude']
        else:
            latdd = values['iridium_lat']

        londeg = float(headers['location']['longitude']['degrees'])
        lonmin = float(headers['location']['longitude']['minutes'])
        values['iridium_lon'] = londeg + (lonmin / 60)
        if 'longitude' in values and values['longitude']:
            londd = values['longitude']
        else:
            londd = values['iridium_lon']

        top_level = {
            'uid':     str(headers['imei']),
            'gid':     None,
            'time':    timestamp.isoformat(),
            'reftime': reftime.isoformat(),
            'lat':     latdd,
            'lon':     londd,
            'z':       None,
            'payload': json.dumps(payload, allow_nan=False)
        }

        del headers['imei']
        del headers['location']

        misc = {}
        if 'misc' in values and values['misc']:
            misc = values['misc']
        del values['misc']

        fullvalues = {
            **top_level,
            'values': {
                **values,
                **headers,
                **misc
            }
        }

        # All HSTORE values need to be strings
        if fullvalues['values']:
            fullvalues['values'] = { k: make_valid_string(str(x)) if x else None for k, x in fullvalues['values'].items() }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v }

    return newtopic, cols, constraint_name, message_to_values


topic_to_func = {
    'arete.data':                    arete_data,
    'axds-netcdf-replayer-data':     generic_float_data,
    'float_reports':                 float_reports,
    'just_json':                     just_json,
    'netcdf_replayer':               generic_float_data,
    'numurus.status':                numurus_status,
    'oot.reports.environmental':     float_reports,
    'oot.reports.health_and_status': float_reports,
    'oot.reports.mission_sensors':   float_reports,
}


def default_func(topic):
    return generic_float_data(topic)
