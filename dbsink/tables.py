#!python
# coding=utf-8
import sqlalchemy as sql
from sqlalchemy.dialects.postgresql import UUID, HSTORE, JSON


def columns_and_message_conversion(topic):
    if topic in topic_to_func:
        return topic_to_func[topic](topic)
    else:
        return default_func(topic)


def generic_float_data(topic):

    cols = [
        sql.Column('id',       sql.Integer, sql.Sequence(f'{topic}_id_seq'), primary_key=True),
        sql.Column('uid',      sql.String, index=True),
        sql.Column('gid',      sql.String, default='', index=True),
        sql.Column('time',     sql.DateTime(timezone=False), index=True),
        sql.Column('lat',      sql.REAL, index=True),
        sql.Column('lon',      sql.REAL, index=True),
        sql.Column('z',        sql.REAL, default=0.0, index=True),
        sql.Column('values',   HSTORE, default={}),
        sql.Column('meta',     JSON, default={}),
        sql.Index(
            f'{topic}_unique_idx'.replace('-', '_'),
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
            name=f'{topic}_unique_constraint'.replace('-', '_'),
        )
    ]

    def message_to_values(key, value):
        # All HSTORE values need to be strings
        if value['values']:
            value['values'] = { k: str(x) for k, x in value['values'].items() }

        # Remove None to use the defaults defined in the table definition
        return key, { k: v for k, v in value.items() if v }

    return cols, message_to_values


topic_to_func = {
    'axds-netcdf-replayer-data': generic_float_data,
}


def default_func(topic):
    return generic_float_data(topic)
