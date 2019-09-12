#!python
# coding=utf-8
import re
import collections
import simplejson as json
from datetime import datetime

import pytz
import sqlalchemy as sql
from shapely.ops import unary_union
from geoalchemy2.types import Geography
from shapely.geometry import shape, Point
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


def flatten(d, parent_key='', sep='_'):
    # https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


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
            sql.Column('lat',      sql.REAL, index=True, default=0.0),
            sql.Column('lon',      sql.REAL, index=True, default=0.0),
            sql.Column('z',        sql.REAL, index=True, default=0.0),
            sql.Column('geom',     Geography(srid=4326)),
            sql.Column('values',   HSTORE, default={}),
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
        value['geom'] = Point(value['lon'], value['lat']).wkt

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in value.items() if v is not None }


class AreteData(GenericFloat):

    def message_to_values(self, key, value):

        values_copy = value.copy()

        # Remove some randoms
        removes = ['not_decoded', 'Compressed_Data']
        for r in removes:
            if r in value['json']:
                del values_copy['json'][r]

        payload = payload_parse(values_copy)
        values = flatten(values_copy)

        # Time - use float timestamp and fall back to Iridium
        reftime = datetime.fromtimestamp(values['headers_iridium_ts'], pytz.utc)
        # TODO: There is no status_ts yet, but this is here for
        # if one does show up eventually
        if 'headers_status_ts' in values and values['headers_status_ts']:
            timestamp = datetime.fromtimestamp(values['headers_status_ts'], pytz.utc)
        else:
            timestamp = reftime

        # Location - Use values locations and fall back to Iridium
        latdeg = float(values['headers_location_latitude_degrees'])
        latmin = float(values['headers_location_latitude_minutes'])
        latdd = latdeg + (latmin / 60)

        londeg = float(values['headers_location_longitude_degrees'])
        lonmin = float(values['headers_location_longitude_minutes'])
        londd = londeg + (lonmin / 60)

        if 'json_Full_ll' in values and isinstance(values['json_Full_ll'], list):
            latdd = values['json_Full_ll'][0]
            londd = values['json_Full_ll'][1]

        top_level = {
            'uid':     str(values['headers_imei']),
            'gid':     None,
            'time':    timestamp.isoformat(),
            'reftime': reftime.isoformat(),
            'lat':     latdd,
            'lon':     londd,
            'z':       None,
            'payload': payload
        }
        top_level['geom'] = Point(top_level['lon'], top_level['lat']).wkt

        fullvalues = {
            **top_level,
            'values': {
                **values
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

        values = flatten(value)

        top_level = {
            'uid':     values['imei'],
            'gid':     None,
            'time':    dtparse(values['timestamp']).replace(tzinfo=pytz.utc).isoformat(),
            'reftime': dtparse(values['navsat_fix_time']).replace(tzinfo=pytz.utc).isoformat(),
            'lat':     values['latitude'],
            'lon':     values['longitude'],
            'z':       None,
            'payload': payload,
        }
        top_level['geom'] = Point(top_level['lon'], top_level['lat']).wkt

        skips = [
            # No eacy ay to represent this as a flat dict. We can write a db view to extract this
            # data from the `payload` if required.
            'data_segment_data_product_pipeline'
        ]

        # All HSTORE values need to be strings
        values = {
            k: make_valid_string(str(x)) if x is not None else None for k, x in values.items()
            if k not in skips
        }
        values['mfr'] = 'numurus'

        fullvalues = {
            **top_level,
            'values': {
                **values
            }
        }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v is not None }


class NumurusStatus(GenericFloat):

    def message_to_values(self, key, value):
        payload = payload_parse(value)

        values = flatten(value)

        top_level = {
            'uid':     values['imei'],
            'gid':     None,
            'time':    dtparse(values['timestamp']).replace(tzinfo=pytz.utc).isoformat(),
            'reftime': dtparse(values['navsat_fix_time']).replace(tzinfo=pytz.utc).isoformat(),
            'lat':     values['latitude'],
            'lon':     values['longitude'],
            'z':       None,
            'payload': payload
        }
        top_level['geom'] = Point(top_level['lon'], top_level['lat']).wkt

        # All HSTORE values need to be strings
        values = { k: make_valid_string(str(x)) if x is not None else None for k, x in values.items() }
        values['mfr'] = 'numurus'

        fullvalues = {
            **top_level,
            'values': {
                **values
            }
        }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v is not None }


class NwicFloatReports(GenericFloat):

    def message_to_values(self, key, value):
        payload = payload_parse(value)

        values = flatten(value)

        # Time - use float timestamp and fall back to Iridium
        reftime = datetime.fromtimestamp(values['headers_iridium_ts'], pytz.utc)
        timestamp = reftime

        # Try to extract a better timestamp
        for k in ['values_status_ts', 'values_environmental_ts', 'values_mission_ts']:
            if values.get(k):
                timestamp = datetime.fromtimestamp(values[k], pytz.utc)
                break

        # Location - Use values locations and fall back to Iridium
        latdeg = float(values['headers_location_latitude_degrees'])
        latmin = float(values['headers_location_latitude_minutes'])
        latdd = latdeg + (latmin / 60)
        if values.get('values_latitude'):
            latdd = values['values_latitude']

        londeg = float(values['headers_location_longitude_degrees'])
        lonmin = float(values['headers_location_longitude_minutes'])
        londd = londeg + (lonmin / 60)
        if values.get('values_longitude'):
            londd = values['values_longitude']

        top_level = {
            'uid':     str(values['headers_imei']),
            'gid':     None,
            'time':    timestamp.isoformat(),
            'reftime': reftime.isoformat(),
            'lat':     latdd,
            'lon':     londd,
            'z':       None,
            'payload': payload
        }
        top_level['geom'] = Point(top_level['lon'], top_level['lat']).wkt

        # All HSTORE values need to be strings
        values = { k: make_valid_string(str(x)) if x is not None else None for k, x in values.items() }

        fullvalues = {
            **top_level,
            'values': {
                **values
            }
        }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v is not None }
