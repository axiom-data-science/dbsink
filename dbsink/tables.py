#!python
# coding=utf-8
import re
import simplejson as json
from datetime import datetime

import pytz
import sqlalchemy as sql
from shapely.geometry import shape
from shapely.ops import unary_union
from geoalchemy2.types import Geography
from dateutil.parser import parse as dtparse
from sqlalchemy.dialects.postgresql import HSTORE, JSONB

from dbsink.maps import BaseMap, payload_parse

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


class GenericGeography(BaseMap):

    @property
    def schema(self):
        return [
            sql.Column('id',       sql.Integer, sql.Sequence(self.sequence_name), primary_key=True),
            sql.Column('uid',      sql.String, default='', index=True),
            sql.Column('gid',      sql.String, default='', index=True),
            sql.Column('time',     sql.DateTime(timezone=True), index=True),
            sql.Column('reftime',  sql.DateTime(timezone=True), index=True),
            sql.Column('values',   HSTORE, default={}),
            sql.Column('payload',  JSONB, default={}),
            sql.Column('geom',     Geography(srid=4326)),
            sql.Index(
                self.unique_index_name,
                'uid',
                'gid',
                'time',
                unique=True,
            ),
            sql.UniqueConstraint(
                'uid',
                'gid',
                'time',
                name=self.upsert_constraint_name
            )
        ]

    def message_to_values(self, key, value):
        payload = payload_parse(value)

        tops = ['id', 'uid', 'gid', 'time', 'reftime', 'values', 'payload', 'geom', 'geojson']
        top_level = value.copy()

        # Save GeoJSON
        if isinstance(top_level['geojson'], str):
            geojson = json.loads(top_level['geojson'])
        else:
            geojson = top_level['geojson']

        features = []
        if 'features' in geojson:
            # This is a FeatureCollection
            features = geojson['features']
        elif 'coordinates' in geojson:
            # This is a geometry object, make a feature
            features = [{ "type": "Feature", "properties": {}, "geometry": geojson }]
        elif 'geometry' in geojson:
            # This is a Feature, cool.
            features = [geojson]

        del top_level['geojson']
        # Merge any geometries into one
        top_level['geom'] = unary_union([ shape(f['geometry']) for f in features ]).wkt

        # Start values with the properties of each GeoJSON Feature.
        # There overwrite as they iterate. Finally they are overridden
        # with the passed in "values".
        values = {}
        for f in features:
            values.update(f['properties'])
        if 'values' in value:
            values.update(value['values'])

        for k, v in value.items():
            if k not in tops:
                # All HSTORE values need to be strings or None
                v = v if v is not None else None
                values[k] = make_valid_string(str(v))
                del top_level[k]  # Remove from the top level

        if 'reftime' not in top_level:
            top_level['reftime'] = top_level['time']

        top_level['time'] = dtparse(top_level['time']).replace(tzinfo=pytz.utc).isoformat()
        top_level['reftime'] = dtparse(top_level['reftime']).replace(tzinfo=pytz.utc).isoformat()
        top_level['values'] = values
        top_level['payload'] = payload

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in top_level.items() if v is not None }


class GenericFloat(BaseMap):

    @property
    def schema(self):
        return [
            sql.Column('id',       sql.Integer, sql.Sequence(self.sequence_name), primary_key=True),
            sql.Column('uid',      sql.String, index=True),
            sql.Column('gid',      sql.String, default='', index=True),
            sql.Column('time',     sql.DateTime(timezone=True), index=True),
            sql.Column('reftime',  sql.DateTime(timezone=True), index=True),
            sql.Column('lat',      sql.REAL, index=True, default=0),
            sql.Column('lon',      sql.REAL, index=True, default=0),
            sql.Column('z',        sql.REAL, default=0.0, index=True),
            sql.Column('values',   HSTORE, default={}),
            sql.Column('meta',     JSONB, default={}),
            sql.Column('payload',  JSONB, default={}),
            sql.Index(
                self.unique_index_name,
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
                name=self.upsert_constraint_name
            )
        ]

    def message_to_values(self, key, value):
        payload = payload_parse(value)

        # All HSTORE values need to be strings
        if value['values']:
            value['values'] = { k: make_valid_string(str(x)) for k, x in value['values'].items() }

        if 'reftime' not in value:
            value['reftime'] = value['time']

        value['payload'] = payload

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in value.items() if v is not None }


class AreteData(GenericFloat):

    def message_to_values(self, key, value):

        headers = value['headers'].copy()
        values = value['json'].copy()

        # Remove some randoms
        removes = ['not_decoded', 'Compressed_Data']
        for r in removes:
            if r in values:
                del values[r]

        payload = payload_parse(values)

        values['mfr'] = value['mfr']
        values['cdr_reference'] = value['cdr_reference']
        values['cep_radius'] = headers['location']['cep_radius']

        # Time - use float timestamp and fall back to Iridium
        reftime = datetime.fromtimestamp(headers['iridium_ts'], pytz.utc)
        # TODO: There is no status_ts yet, but this is here for
        # if one does show up eventually
        if 'status_ts' in values and values['status_ts']:
            timestamp = datetime.fromtimestamp(values['status_ts'], pytz.utc)
        else:
            timestamp = reftime

        # Location - Use value locations and fall back to Iridium
        latdeg = float(headers['location']['latitude']['degrees'])
        latmin = float(headers['location']['latitude']['minutes'])
        values['iridium_lat'] = latdeg + (latmin / 60)
        londeg = float(headers['location']['longitude']['degrees'])
        lonmin = float(headers['location']['longitude']['minutes'])
        values['iridium_lon'] = londeg + (lonmin / 60)

        if 'Full_ll' in values and isinstance(values['Full_ll'], list):
            latdd = values['Full_ll'][0]
            londd = values['Full_ll'][1]
            del values['Full_ll']
        else:
            latdd = values['iridium_lat']
            londd = values['iridium_lon']

        top_level = {
            'uid':     str(headers['imei']),
            'gid':     None,
            'time':    timestamp.isoformat(),
            'reftime': reftime.isoformat(),
            'lat':     latdd,
            'lon':     londd,
            'z':       None,
            'payload': payload
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
                k: make_valid_string(str(x)) if x is not None else None
                for k, x in fullvalues['values'].items()
            }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v is not None }


class NumurusData(GenericFloat):

    def message_to_values(self, key, value):
        payload = payload_parse(value)

        # We are skipping the "data_segment" portion, no idea how to interpret this. It is
        # carried through in the "payload" and could be used if someone wanted to
        skips = ['timestamp', 'navsat_fix_time', 'imei', 'latitude', 'longitude', 'data_segment']

        top_level = {
            'uid':     value['imei'],
            'gid':     None,
            'time':    dtparse(value['timestamp']).replace(tzinfo=pytz.utc).isoformat(),
            'reftime': dtparse(value['navsat_fix_time']).replace(tzinfo=pytz.utc).isoformat(),
            'lat':     value['latitude'],
            'lon':     value['longitude'],
            'z':       None,
            'payload': payload
        }

        # All HSTORE values need to be strings
        values = { k: make_valid_string(str(x)) if x is not None else None for k, x in value.items() if k not in skips }
        values['mfr'] = 'numurus'

        fullvalues = {
            **top_level,
            'values': values
        }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v is not None }


class NumurusStatus(GenericFloat):

    def message_to_values(self, key, value):
        payload = payload_parse(value)

        skips = ['timestamp', 'navsat_fix_time', 'imei', 'latitude', 'longitude']

        top_level = {
            'uid':     value['imei'],
            'gid':     None,
            'time':    dtparse(value['timestamp']).replace(tzinfo=pytz.utc).isoformat(),
            'reftime': dtparse(value['navsat_fix_time']).replace(tzinfo=pytz.utc).isoformat(),
            'lat':     value['latitude'],
            'lon':     value['longitude'],
            'z':       None,
            'payload': payload
        }

        # All HSTORE values need to be strings
        values = { k: make_valid_string(str(x)) if x is not None else None for k, x in value.items() if k not in skips }
        values['mfr'] = 'numurus'

        fullvalues = {
            **top_level,
            'values': values
        }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v is not None }


class NwicFloatReports(GenericFloat):

    def message_to_values(self, key, value):
        payload = payload_parse(value)

        headers = value['headers'].copy()
        values = value['values'].copy()

        values['mfr'] = value['mfr']
        values['cdr_reference'] = value['cdr_reference']
        values['cep_radius'] = headers['location']['cep_radius']

        # Time - use float timestamp and fall back to Iridium
        reftime = datetime.fromtimestamp(headers['iridium_ts'], pytz.utc)
        if 'status_ts' in values and values['status_ts']:
            timestamp = datetime.fromtimestamp(values['status_ts'], pytz.utc)
        elif 'environmental_ts' in values and values['environmental_ts']:
            timestamp = datetime.fromtimestamp(values['environmental_ts'], pytz.utc)
        elif 'mission_ts' in values and values['mission_ts']:
            timestamp = datetime.fromtimestamp(values['mission_ts'], pytz.utc)
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
            'payload': payload
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
            fullvalues['values'] = { k: make_valid_string(str(x)) if x is not None else None for k, x in fullvalues['values'].items() }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v is not None }
