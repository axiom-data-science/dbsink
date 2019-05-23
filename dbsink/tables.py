#!python
# coding=utf-8
from datetime import datetime

import sqlalchemy as sql
from sqlalchemy.dialects.postgresql import UUID, HSTORE, JSON


def columns_and_message_conversion(topic):
    if topic in topic_to_func:
        return topic_to_func[topic](topic)
    else:
        return default_func(topic)


def generic_cols(topic):

    newtopic = topic.replace('.', '-')

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
            name=f'{newtopic}_unique_constraint'.replace('-', '_'),
        )
    ]

    return newtopic, cols


def generic_float_data(topic):

    newtopic, cols = generic_cols(topic)

    def message_to_values(key, value):
        # All HSTORE values need to be strings
        if value['values']:
            value['values'] = { k: str(x) for k, x in value['values'].items() }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in value.items() if v }

    return newtopic, cols, message_to_values


def float_reports(topic):

    newtopic, cols = generic_cols(topic)

    def message_to_values(key, value):

        headers = value['headers'].copy()
        values = value['values'].copy()

        values['mfr'] = value['mfr']
        values['cdr_reference'] = value['cdr_reference']
        values['cep_radius'] = headers['location']['cep_radius']

        # Location
        latdeg = float(headers['location']['latitude']['degrees'])
        latmin = float(headers['location']['latitude']['minutes'])
        londeg = float(headers['location']['longitude']['degrees'])
        lonmin = float(headers['location']['longitude']['minutes'])
        latdd = latdeg + (latmin / 60)
        londd = londeg + (lonmin / 60)

        top_level = {
            'uid': str(headers['imei']),
            'gid': None,
            'time': datetime.utcfromtimestamp(headers['iridium_ts']).isoformat(),
            'lat': latdd,
            'lon': londd,
            'z': None,
        }

        del headers['imei']
        del headers['location']
        del headers['iridium_ts']

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

    return newtopic, cols, message_to_values


topic_to_func = {
    'axds-netcdf-replayer-data':     generic_float_data,
    'oot.reports.mission_sensors':   float_reports,
    'oot.reports.environmental':     float_reports,
    'oot.reports.health_and_status': float_reports,
}


def default_func(topic):
    return generic_float_data(topic)
