#!python
# coding=utf-8
import re
import ast
import collections
import simplejson as json
from datetime import datetime

import pytz
import sqlalchemy as sql
from shapely.ops import unary_union
from geoalchemy2.types import Geometry
from geoalchemy2.shape import from_shape
from shapely.geometry import shape, Point, box
from dateutil.parser import parse as dtparse
from sqlalchemy.dialects.postgresql import HSTORE, JSONB, DOUBLE_PRECISION

from dbsink.maps import BaseMap, payload_parse
from dbsink.utils import MessageFiltered
from dbsink import L  # noqa

xx = re.compile(r'[\x00-\x1f\\"]')
ux = re.compile(r'[\\u[0-9A-Fa-f]]')


WGS84_BBOX_180 = box(-180, -90, 180, 90)
WGS84_BBOX_360 = box(0, -90, 360, 90)


def flatten(d, parent_key='', sep='_'):
    # Adapted from:
    # https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
    # to support nested lists and decoding of JSON objects that might be strings.
    items = []
    if isinstance(d, collections.MutableMapping):
        for k, v in d.items():
            new_key = f'{parent_key}{sep}{k}' if parent_key else k
            items.extend(
                flatten(v, new_key, sep=sep
            ).items())
    elif isinstance(d, list):
        # add the list object and its members as indexed items
        items.append((parent_key, d))
        for i, litem in enumerate(d):
            new_key = f'{parent_key}{sep}{i}' if parent_key else i
            items.extend(
                flatten(litem, new_key, sep=sep
            ).items())
    elif isinstance(d, str):
        # Try to expand it
        try:
            decoded = expand_json_objects(d)
            items.extend(
                flatten(decoded, parent_key, sep=sep
            ).items())
        except ValueError:
            # Not decodable, just set the objects value
            items.append((parent_key, d))
    else:
        items.append((parent_key, d))

    return dict(items)


def expand_json_objects(str_value):
    decoders = [
        json.loads,
        lambda x: json.loads(json.dumps(ast.literal_eval(x)))
    ]

    for d in decoders:
        try:
            # If this decoder object was successful, return it
            return d(str_value)
        except BaseException:
            continue

    # Return the original value if nothing was decoded
    raise ValueError('Could not decode any JSON object')


def get_point_location_quality(loc_geom, inprecise_location=False, disallow_lon=None, disallow_lat=None):
    """ QARTOD Flags:
        1 - Good
        2 - Not evaluated
        3 - Questionable/suspect
        4 - Bad
        9 - Missing Data
    """

    # Avoid locations that are both small decimal numbers.
    if -1 < loc_geom.x < 1 and -1 < loc_geom.y < 1:
        return 4

    # Avoid "null island" locations
    if loc_geom.x == 0 or loc_geom.y == 0:
        return 4

    # if we need to ignore certain X values
    if isinstance(disallow_lon, list) and loc_geom.x in disallow_lon:
        return 4

    # if we need to ignore certain Y values
    if isinstance(disallow_lat, list) and loc_geom.y in disallow_lat:
        return 4

    # Make sure we have resonable coordinates
    if not any([
        loc_geom.within(WGS84_BBOX_180),
        loc_geom.within(WGS84_BBOX_360)
    ]):
        return 4

    # If using an inprecise location (ie. Iridium)
    if inprecise_location is True:
        return 3

    return 1


def apply_start_end_filter(message_time, starting, ending):
    if isinstance(starting, datetime) and message_time < starting:
        raise MessageFiltered(f'Filtering out message from {message_time} since it is before {starting}')
    elif isinstance(ending, datetime) and message_time > ending:
        raise MessageFiltered(f'Filtering out message from {message_time} since it is after {ending}')


def make_valid_string(obj):
    if isinstance(obj, str):
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
    else:
        return str(obj)


class GenericFieldStatistic(BaseMap):

    @property
    def schema(self):
        return [
            sql.Column('id',       sql.Integer, sql.Sequence(self.sequence_name), primary_key=True),
            sql.Column('source',   sql.String, index=True, nullable=False),
            sql.Column('period',   sql.String, default=''),
            sql.Column('starting', sql.DateTime(timezone=True), index=True, nullable=False),
            sql.Column('ending',   sql.DateTime(timezone=True), index=True, nullable=False),
            sql.Column('values',   JSONB),
            sql.Index(
                self.unique_index_name,
                'source',
                'period',
                'starting',
                'ending',
                unique=True,
            ),
            sql.UniqueConstraint(
                'source',
                'period',
                'starting',
                'ending',
                name=self.upsert_constraint_name
            )
        ]

    def message_to_values(self, key, value):
        value = payload_parse(value)

        # Throw away non-column data
        value = self.match_columns(value)

        value['starting'] = dtparse(value['starting']).replace(tzinfo=pytz.utc)
        value['ending'] = dtparse(value['ending']).replace(tzinfo=pytz.utc)

        # Filter to make sure the time is represented in the filter window
        # First filter the `starting` between min and `end_date` filter
        # Then filter the `ending` between `start_date` filter and max
        # After both filters we are left with the defined window.
        apply_start_end_filter(
            value['starting'],
            datetime.min.replace(tzinfo=pytz.utc),
            self.filters.get('end_date')
        )
        apply_start_end_filter(
            value['ending'],
            self.filters.get('start_date'),
            datetime.max.replace(tzinfo=pytz.utc),
        )

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in value.items() }


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
            sql.Column('geom',     Geometry(srid=4326)),
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

        top_time = dtparse(top_level['time']).replace(tzinfo=pytz.utc)
        apply_start_end_filter(
            top_time,
            self.filters.get('start_date'),
            self.filters.get('end_date')
        )

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
        top_level['geom'] = from_shape(
            unary_union([ shape(f['geometry']) for f in features ]),
            srid=4326
        )

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
                values[k] = make_valid_string(v)
                del top_level[k]  # Remove from the top level

        if 'reftime' not in top_level:
            top_level['reftime'] = top_level['time']

        # All HSTORE values need to be strings
        values = {
            k: make_valid_string(x) if x is not None else None
            for k, x in values.items()
        }

        top_level['time'] = top_time.isoformat()
        top_level['reftime'] = dtparse(top_level['reftime']).replace(tzinfo=pytz.utc).isoformat()
        top_level['values'] = values
        top_level['payload'] = payload

        # Throw away non-column data
        top_level = self.match_columns(top_level)
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
            sql.Column('lat',      DOUBLE_PRECISION, index=True),
            sql.Column('lon',      DOUBLE_PRECISION, index=True),
            sql.Column('z',        DOUBLE_PRECISION, index=True),
            sql.Column('geom',     Geometry('POINT', srid=4326)),
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

        top_time = dtparse(value['time']).replace(tzinfo=pytz.utc)
        apply_start_end_filter(
            top_time,
            self.filters.get('start_date'),
            self.filters.get('end_date')
        )

        value['lat'] = float(value['lat'])
        value['lon'] = float(value['lon'])
        pt = Point(value['lon'], value['lat'])
        value['geom'] = from_shape(pt, srid=4326)

        if not value['values']:
            value['values'] = {}
        value['values']['location_quality'] = get_point_location_quality(pt)
        # All HSTORE values need to be strings
        value['values'] = { k: make_valid_string(x) for k, x in value['values'].items() }

        value['time'] = top_time.isoformat()
        if 'reftime' in value:
            value['reftime'] = dtparse(value['reftime']).replace(tzinfo=pytz.utc).isoformat()
        else:
            value['reftime'] = value['time']

        value['payload'] = payload

        # Throw away non-column data
        value = self.match_columns(value)
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
        if values.get('headers_status_ts'):
            timestamp = datetime.fromtimestamp(values['headers_status_ts'], pytz.utc)
        else:
            timestamp = reftime

        apply_start_end_filter(
            timestamp,
            self.filters.get('start_date'),
            self.filters.get('end_date')
        )

        # Location - Use values locations and fall back to Iridium
        inprecise_location = True
        latdeg = float(values['headers_location_latitude_degrees'])
        latmin = float(values['headers_location_latitude_minutes'])
        latdd = latdeg + (latmin / 60)

        londeg = float(values['headers_location_longitude_degrees'])
        lonmin = float(values['headers_location_longitude_minutes'])
        londd = londeg + (lonmin / 60)

        if 'json_Full_ll' in values and isinstance(values['json_Full_ll'], list):
            # Older style positions in early data
            latdd = values['json_Full_ll'][0]
            londd = values['json_Full_ll'][1]
            inprecise_location = False
        elif 'json_position_latitude' in values and 'json_position_longitude' in values:
            # Newer style posititions as of Dec 11, 2019
            latdd = values['json_position_latitude']
            londd = values['json_position_longitude']
            inprecise_location = False

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
        pt = Point(top_level['lon'], top_level['lat'])
        top_level['geom'] = from_shape(pt, srid=4326)

        # Set additional values
        values['location_quality'] = get_point_location_quality(pt, inprecise_location=inprecise_location)
        values['mfr'] = 'arete'

        # All HSTORE values need to be strings
        values = {
            k: make_valid_string(x) if x is not None else None
            for k, x in values.items()
        }

        fullvalues = {
            **top_level,
            'values': {
                **values
            }
        }

        # Throw away non-column data
        fullvalues = self.match_columns(fullvalues)
        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v is not None }


class NumurusData(GenericFloat):

    def message_to_values(self, key, value):
        payload = payload_parse(value)

        values = flatten(value)

        top_time = dtparse(values['timestamp']).replace(tzinfo=pytz.utc)
        apply_start_end_filter(
            top_time,
            self.filters.get('start_date'),
            self.filters.get('end_date')
        )

        top_level = {
            'uid':     values['imei'],
            'gid':     None,
            'time':    top_time.isoformat(),
            'reftime': dtparse(values['navsat_fix_time']).replace(tzinfo=pytz.utc).isoformat(),
            'lat':     values['latitude'],
            'lon':     values['longitude'],
            'z':       None,
            'payload': payload,
        }
        pt = Point(top_level['lon'], top_level['lat'])
        top_level['geom'] = from_shape(pt, srid=4326)

        skips = [
            # No easy way to represent this as a flat dict. We can write a db view to extract this
            # data from the `payload` if required.
            'data_segment_data_product_pipeline',
            'data_segment_data_segment_data_product_pipeline'
        ]

        # Set additional values
        # Lat=91 and Lon=181 should be treated as bad location data
        values['location_quality'] = get_point_location_quality(
            pt,
            disallow_lon=[181],
            disallow_lat=[91]
        )
        values['mfr'] = 'numurus'

        # All HSTORE values need to be strings
        values = {
            k: make_valid_string(x) if x is not None else None
            for k, x in values.items()
            if k not in skips
        }

        fullvalues = {
            **top_level,
            'values': {
                **values
            }
        }

        # Throw away non-column data
        fullvalues = self.match_columns(fullvalues)
        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v is not None }


class NumurusStatus(GenericFloat):

    def message_to_values(self, key, value):
        payload = payload_parse(value)

        values = flatten(value)

        top_time = dtparse(values['timestamp']).replace(tzinfo=pytz.utc)
        apply_start_end_filter(
            top_time,
            self.filters.get('start_date'),
            self.filters.get('end_date')
        )

        top_level = {
            'uid':     values['imei'],
            'gid':     None,
            'time':    top_time.isoformat(),
            'reftime': dtparse(values['navsat_fix_time']).replace(tzinfo=pytz.utc).isoformat(),
            'lat':     values['latitude'],
            'lon':     values['longitude'],
            'z':       None,
            'payload': payload
        }
        pt = Point(top_level['lon'], top_level['lat'])
        top_level['geom'] = from_shape(pt, srid=4326)

        # Set additional values
        # Lat=91 and Lon=181 should be treated as bad location data
        values['location_quality'] = get_point_location_quality(
            pt,
            disallow_lon=[181],
            disallow_lat=[91]
        )
        values['mfr'] = 'numurus'

        # All HSTORE values need to be strings
        values = {
            k: make_valid_string(x) if x is not None else None
            for k, x in values.items()
        }

        fullvalues = {
            **top_level,
            'values': {
                **values
            }
        }

        # Throw away non-column data
        fullvalues = self.match_columns(fullvalues)
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

        apply_start_end_filter(
            timestamp,
            self.filters.get('start_date'),
            self.filters.get('end_date')
        )

        # Location - Use values locations and fall back to Iridium
        inprecise_location = True
        latdeg = float(values['headers_location_latitude_degrees'])
        latmin = float(values['headers_location_latitude_minutes'])
        latdd = latdeg + (latmin / 60)

        londeg = float(values['headers_location_longitude_degrees'])
        lonmin = float(values['headers_location_longitude_minutes'])
        londd = londeg + (lonmin / 60)

        if values.get('values_longitude') and values.get('values_latitude'):
            latdd = values['values_latitude']
            londd = values['values_longitude']
            inprecise_location = False

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
        pt = Point(top_level['lon'], top_level['lat'])
        top_level['geom'] = from_shape(pt, srid=4326)

        # Set additional values
        values['location_quality'] = get_point_location_quality(pt, inprecise_location=inprecise_location)

        # All HSTORE values need to be strings
        values = {
            k: make_valid_string(x) if x is not None else None
            for k, x in values.items()
        }

        fullvalues = {
            **top_level,
            'values': {
                **values
            }
        }

        # Throw away non-column data
        fullvalues = self.match_columns(fullvalues)
        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in fullvalues.items() if v is not None }
