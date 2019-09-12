## dbsink

Read from a kafka topic and sink to a database table, one row per message.

This is not unlike the Kafka Connect JdbcConnector. This project has a much lower bar of entry and doesn't require diving into the Kafka Connect ecosystem. I wrote the equivilent to this project using a custom JdbcConnector and it was getting out of control and was basically un-testable. So here we are.

You can choose to unpack the data as `avro`, `msgpack` or the default `json`. `avro` requires an additional `registry` parameter.

Docker images: https://hub.docker.com/r/axiom/dbsink/builds

## WHY?

I needed to read from well-defined kafka topics and store the results in a database table so collaborators could interact with the data in a more familiar way.

It is also a very convienent and easy to setup PostgREST on top of the resulting tables to get a quick read-only REST API on top of the tabled messages.

## Mapping messages to tables

You can define custom mappings between messages and tables using a python class. You may register your custom mappings with the `dbsink.maps` entrypoint to have them available to `dbsink` at run-time.

```python
entry_points = {
    'dbsink.maps': [
        'YourCustomMap    = you.custom.map.module:CustomMapClass',
        # ...
    ]
}
```

Custom mapping classes should inherit from the `BaseMap` class in `dbsink` and override the following functions as needed:

* `upsert_constraint_name` - Name of the constraint to use for upserting data. Set to to `None` to disable upserting. Use this class property when creating the upsert constraint on your table (see example below).

* `unique_index_name` - Unique index name based on the table name. Use this if defining a single unique index on your table.

* `sequence_name` - Unique sequence name based on the table name. Use this if defining a single sequence column on your table.

* `_check_key` - Checks for validity of a message's `key` before trying to sink. Return `True` if valid and raise an error if not.

* `_check_value` - Checks for validity of a message's `value` before trying to sink. Return `True` if valid and raise an error if not.

* `schema` - A list of SQLAlchmy [Column](https://docs.sqlalchemy.org/en/13/core/metadata.html#sqlalchemy.schema.Column), [Index](https://docs.sqlalchemy.org/en/13/core/constraints.html?highlight=index#sqlalchemy.schema.Index), and [Constraint](https://docs.sqlalchemy.org/en/13/core/constraints.html?highlight=constraint#sqlalchemy.schema.Constraint) schema definitions to use in table creation and updating. This fully describes your table's schema.

* `message_to_values` - A function accepting `key` and `value` arguments and returning a tuple `key, dict` where the dict is the `values` to pass to SQLAlchemy's `insert().values` method. The `value` argument to this function will already be unpacked if `avro` or `msgpack` packing was specified.

    ```python
    insert(table).values(
      # dict_returned_ends_up_here
    )
    ```

#### Example

A simple example is the `StringMap` mapping included with `dbsink`

```python
from datetime import datetime

import pytz
import sqlalchemy as sql
import simplejson as json

from dbsink.maps import BaseMap


class StringMap(BaseMap):

    @property
    def upsert_constraint_name(self):
        return None  # Ignore upserts

    def _check_key(self, key):
        return True  # All keys are valid

    def _check_value(self, value):
        # Make sure values are JSON parsable
        _ = json.loads(json.dumps(value, ignore_nan=True))
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
        # Raises if invalid. This calls `._check_key` and `._check_value`
        self.check(key, value)

        values = {
            'sinked':  datetime.utcnow().replace(tzinfo=pytz.utc).isoformat(),
            'key':     key,
            'payload': json.dumps(value),
        }

        return key, values
```

#### Advanced Example

There are no restrictions on table schemas or how you map the message data into the schema. Take this example below that uses a `PostGIS` column.


```python
from datetime import datetime

import pytz
import sqlalchemy as sql
import simplejson as json
from shapely.geometry import shape
from geoalchemy2.types import Geography

from dbsink.maps import BaseMap


class NamedGenericGeography(BaseMap):

    def _check_key(self, key):
        return True  # All keys are valid

    def _check_value(self, value):
        # Make sure values are JSON parsable
        _ = json.loads(json.dumps(value, ignore_nan=True))
        return True

    @property
    def schema(self):
        return [
            sql.Column('id',       sql.Integer, sql.Sequence(self.sequence_name), primary_key=True),
            sql.Column('name',     sql.String, default='', index=True),
            sql.Column('time',     sql.DateTime(timezone=True), index=True),
            sql.Column('geom',     Geography(srid=4326)),
            sql.Index(
                self.unique_index_name,
                'name',
                'time',
                unique=True,
            ),
            sql.UniqueConstraint(
                'name',
                'time',
                name=self.upsert_constraint_name
            )
        ]

    def message_to_values(self, key, value):
        """ Assumes a message format of
        {
          "time": 1000000000, # unix epoch
          "name": "my cool thing",
          "geojson": {
            "geometry": {
              "type": "Polygon",
              "coordinates": [ [ [ -118.532116484818843, 32.107425500492766 ], [ -118.457544847012443, 32.107425500492702 ], [ -118.457544847012443, 32.054517056541435 ], [ -118.532116484818872, 32.054517056541464 ], [ -118.532116484818843, 32.107425500492766 ] ] ]
            }
          }
        }
        """
        # Raises if invalid
        self.check(key, value)

        # GeoJSON `geometry` attribute to WKT
        geometry = shape(value['geojson']['geometry']).wkt

        values = {
            'name': value['name']
            'time': datetime.fromtimestamp(value['time'], pytz.utc).isoformat()
            'geom': geometry
        }

        return key, values
```



## Configuration

This program uses [`Click`](https://click.palletsprojects.com/) for the CLI interface. For all options please use the `help`:

```sh
$ dbsink --help
```

#### Environmental Variables

All configuration options can be specified with environmental variables using the pattern `DBSINK_[argument_name]=[value]`. For more information see [the click documentation](https://click.palletsprojects.com/en/7.x/options/?highlight=auto_envvar_prefix#values-from-environment-variables).

```bash
DBSINK_TOPIC="topic-to-listen-to" \
DBSINK_LOOKUP="StringMap" \
DBSINK_TABLE="MyCoolTable" \
DBSINK_CONSUMER="myconsumer" \
DBSINK_PACKING="msgpack" \
DBSINK_OFFSET="earlist" \
DBSINK_DROP="true" \
DBSINK_VERBOSE="1" \
    dbsink
```

## Testing

You can run the tests using `pytest`. To run the integration tests, start a database with `docker run -p 30300:5432 --name dbsink-int-testing-db -e POSTGRES_USER=sink -e POSTGRES_PASSWORD=sink -e POSTGRES_DB=sink -d mdillon/postgis:11` and run `pytest -m integration`
