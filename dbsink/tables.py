#!python
# coding=utf-8
from copy import copy
import simplejson as json
from datetime import datetime

from dateutil.parser import parse as dtparse

import sqlalchemy as sql
from sqlalchemy.dialects.postgresql import UUID, HSTORE, JSON, JSONB


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
        sql.Column('lat',      sql.REAL, index=True),
        sql.Column('lon',      sql.REAL, index=True),
        sql.Column('z',        sql.REAL, default=0.0, index=True),
        sql.Column('values',   HSTORE, default={}),
        sql.Column('meta',     JSON, default={}),
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
        # All HSTORE values need to be strings
        if value['values']:
            value['values'] = { k: str(x) for k, x in value['values'].items() }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in value.items() if v }

    return newtopic, cols, constraint_name, message_to_values


def numurus_status(topic):
    """
    {
        "float_id":54,
        "status_record_id":1,
        "software_revision":0,
        "heading":98,
        "latitude":91,
        "longitude":46.782272,
        "timestamp":"2019-04-29T21:35:20.000Z",
        "battery_charge":0,
        "bus_voltage":0,
        "temperature":25,
        "node_cfg":64,
        "geofence_cfg":0,
        "task_cfg":0,
        "rule_cfg":0,
        "trig_cfg":0,
        "sensor_cfg":0,
        "trigger_wake_count":0,
        "wake_event_id":1,
        "wake_event_type":0,
        "navsat_fix_time":"2019-04-28T00:00:00.000Z",
        "scuttle_state":null,
        "imei":"300234067991490"
    }
    """
    newtopic, cols, constraint_name = generic_cols(topic)

    def message_to_values(key, value):

        skips = ['timestamp', 'imei', 'latitude', 'longitude']

        top_level = {
            'uid':  value['imei'],
            'gid':  'numurus.status',
            'time': dtparse(value['timestamp']).isoformat(),
            'lat':  value['latitude'],
            'lon':  value['longitude'],
            'z':    None,
        }

        # All HSTORE values need to be strings
        values = { k: str(x) if x else None for k, x in value.items() if k not in skips }
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

        # Make sure we have valid JSON and remove any
        # Infinity and NaN values in the process
        try:
            value = json.loads(json.dumps(value, ignore_nan=True))
        except BaseException as e:
            raise ValueError(f'Could not parse message as valid JSON - {repr(e)}')

        values = {
            'sinked':  datetime.utcnow().isoformat(),
            'key':     key,
            'payload': value,
        }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in values.items() if v }

    constraint_name = None
    return newtopic, cols, constraint_name, message_to_values


def float_reports(topic):

    newtopic, cols, constraint_name = generic_cols(topic)

    def message_to_values(key, value):

        headers = value['headers'].copy()
        values = value['values'].copy()

        values['mfr'] = value['mfr']
        values['cdr_reference'] = value['cdr_reference']
        values['cep_radius'] = headers['location']['cep_radius']

        # Time - use float timestamp and fall back to Iridium
        if 'status_ts' in values and values['status_ts']:
            timestamp = datetime.utcfromtimestamp(values['status_ts'])
        else:
            timestamp = datetime.utcfromtimestamp(headers['iridium_ts'])

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
            'uid': str(headers['imei']),
            'gid': None,
            'time': timestamp.isoformat(),
            'lat': latdd,
            'lon': londd,
            'z': None,
        }

        del headers['imei']
        del headers['location']

        misc = {}
        if values['misc']:
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
            fullvalues['values'] = { k: str(x) if x else None for k, x in fullvalues['values'].items() }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v }

    return newtopic, cols, constraint_name, message_to_values


topic_to_func = {
    'just_json':                     just_json,
    'float_reports':                 float_reports,
    'netcdf_replayer':               generic_float_data,
    'axds-netcdf-replayer-data':     generic_float_data,
    'oot.reports.mission_sensors':   float_reports,
    'oot.reports.environmental':     float_reports,
    'oot.reports.health_and_status': float_reports,
    'numurus.status':                numurus_status,
}


def default_func(topic):
    return generic_float_data(topic)
