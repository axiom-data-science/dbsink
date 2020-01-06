#!python
# coding=utf-8
import simplejson as json
from datetime import datetime

import pytz
import sqlalchemy as sql
from sqlalchemy.dialects.postgresql import JSONB

from dbsink import L


def payload_parse(payload):
    # Make sure we have valid JSON and remove any
    # Infinity and NaN values in the process
    try:
        return json.loads(json.dumps(payload, ignore_nan=True))
    except BaseException as e:
        raise ValueError(f'Could not parse message as valid JSON - {repr(e)}')


class BaseMap:
    def __init__(self, topic, **kwargs):
        self.topic = topic
        self.table = kwargs.get('table', topic).replace('.', '-')
        self.filters = kwargs.get('filters', {})

    @property
    def upsert_constraint_name(self):
        return f'{self.table}_unique_constraint'.replace('-', '_').lower()

    @property
    def unique_index_name(self):
        return f'{self.table}_unique_idx'.replace('-', '_').lower()

    @property
    def sequence_name(self):
        return f'{self.table}_id_seq'.replace('-', '_').lower()

    def _check_key(self, key):
        """ Check if the key is valid
            return True if valid
            raise a KeyError if invalid
        """
        raise NotImplementedError

    def _check_value(self, value):
        """ Check if the key is valid
            return True if valid
            raise a ValueError if invalid
        """
        raise NotImplementedError

    def check(self, key, value):
        """ Check if the key or value is valid """
        return self._check_key(key) is True and self._check_value(value) is True

    @property
    def schema(self):
        """ Return the SQLAlchemy table schema as well as the
            unique index check to upsert data
        """
        raise NotImplementedError

    def match_columns(self, inserts):
        """ Throws away insert data that does not match a defined
            column name.
        """
        # Get column names
        column_names = [ s.name for s in self.schema if isinstance(s, sql.Column) ]
        # Throw away keys that are not column names
        matched_inserts = { k: v for k, v in inserts.items() if k in column_names }

        if inserts.keys() != matched_inserts.keys():
            unmatched = [ x for x in inserts.keys() if x not in matched_inserts ]
            L.info(f'Threw away data with no columns: {unmatched}')

        return matched_inserts

    def message_to_values(self, key, value):
        raise NotImplementedError


class JsonMap(BaseMap):

    @property
    def upsert_constraint_name(self):
        return None

    def _check_key(self, key):
        return True

    def _check_value(self, value):
        _ = payload_parse(value)
        return True

    @property
    def schema(self):
        return [
            sql.Column('id',       sql.Integer, sql.Sequence(self.sequence_name), primary_key=True),
            sql.Column('sinked',   sql.DateTime(timezone=True), index=True),
            sql.Column('key',      sql.String, default='', index=True),
            sql.Column('payload',  JSONB)
        ]

    def message_to_values(self, key, value):

        # Raises if invalid
        value = payload_parse(value)

        self.check(key, value)

        values = {
            'sinked':  datetime.utcnow().replace(tzinfo=pytz.utc).isoformat(),
            'key':     key,
            'payload': value,
        }

        return key, values


class StringMap(BaseMap):

    @property
    def upsert_constraint_name(self):
        return None

    def _check_key(self, key):
        return True

    def _check_value(self, value):
        return True

    @property
    def schema(self):
        return [
            sql.Column('id',       sql.Integer, sql.Sequence(self.sequence_name), primary_key=True),
            sql.Column('sinked',   sql.DateTime(timezone=True), index=True),
            sql.Column('key',      sql.String, default='', index=True),
            sql.Column('payload',  sql.String)
        ]

    def message_to_values(self, key, value):

        # Raises if invalid
        self.check(key, value)

        values = {
            'sinked':  datetime.utcnow().replace(tzinfo=pytz.utc).isoformat(),
            'key':     key,
            'payload': json.dumps(value),
        }

        return key, values
