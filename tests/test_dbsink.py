#!python
# coding=utf-8
from pathlib import Path
import simplejson as json
from datetime import datetime

import pytest
from easyavro import EasyProducer
from click.testing import CliRunner

from dbsink.maps import *  # noqa
from dbsink.tables import *  # noqa
from dbsink import listen, utils


def test_listen_help():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--help'
    ])
    assert result.exit_code == 0


def test_ncreplayer():
    mapp = GenericFloat('axds-netcdf-replayer-data')

    to_send = []
    with open('./tests/replayer.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(mapp.message_to_values('fake', m))

    assert len(to_send) == 4
    assert to_send[0][1]['time'] == to_send[0][1]['reftime']


def test_mission_sensors():
    mapp = NwicFloatReports('oot.reports.mission_sensors')

    to_send = []

    with open('./tests/mission_sensors.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(mapp.message_to_values('fake', m))

    assert len(to_send) == 10


def test_environmental():

    mapp = NwicFloatReports('oot.reports.environmental')

    to_send = []

    with open('./tests/environmental.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(mapp.message_to_values('fake', m))

    assert len(to_send) == 10


def test_null_infinity():
    mapp = JsonMap('whatever')

    to_send = []

    with open('./tests/null_infinity.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(mapp.message_to_values('fake', m))

    assert len(to_send) == 2
    assert to_send[0][1]['payload']['bus_voltage'] is None
    assert to_send[1][1]['payload']['bus_voltage'] is None


def test_health_and_status():
    mapp = NwicFloatReports('foo')

    to_send = []

    with open('./tests/health_and_status.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(mapp.message_to_values('fake', m))

    assert len(to_send) == 516

    m1 = to_send[0][1]
    assert m1['uid'] == '300434063547170'
    assert m1['lat'] == 32.704426
    assert m1['lon'] == -117.23662
    assert m1['time'] == '2019-05-31T20:39:50+00:00'
    assert m1['values']['values_status_ts'] == '1559335190'
    assert m1['values']['headers_iridium_ts'] == '1559335196'
    assert m1['values']['headers_location_latitude_degrees'] == '32'
    assert m1['values']['headers_location_longitude_degrees'] == '-117'
    assert m1['values']['values_latitude'] == '32.704426'
    assert m1['values']['values_longitude'] == '-117.23662'
    assert m1['values']['values_misc_speed'] == '2.72'
    assert m1['values']['values_misc_test_num'] == 'T240'
    assert m1['values']['mfr'] == 'usna'

    m2 = to_send[-1][1]
    assert m2['uid'] == '300434063946390'
    assert m2['lat'] == 39.01338
    assert m2['lon'] == -75.47597
    assert m2['time'] == '2019-06-06T18:19:56+00:00'
    assert 'status_ts' not in m2['values']
    assert m2['values']['headers_iridium_ts'] == '1559845196'
    assert m2['values']['headers_location_latitude_degrees'] == '39'
    assert m2['values']['headers_location_longitude_degrees'] == '-76'
    assert 'latitude' not in m2['values']
    assert m2['values']['values_longitude'] is None
    assert m2['values']['values_misc_speed'] == '0.01'
    assert m2['values']['values_misc_test_num'] == 'T76'
    assert m2['values']['mfr'] == 'usna'


def test_numurus_status():
    mapp = NumurusStatus('topic')

    to_send = []

    with open('./tests/numurus.status.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(mapp.message_to_values('fake', m))
            except BaseException as e:
                listen.L.error(repr(e))

    assert len(to_send) == 87

    # Make sure a 91 latitude trips the quality flag
    assert to_send[-1][1]['values']['location_quality'] == '4'


def test_numurus_data():
    mapp = NumurusData('topic')

    to_send = []

    with open('./tests/numurus.data.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(mapp.message_to_values('fake', m))
            except BaseException as e:
                listen.L.error(repr(e))

    assert to_send[0][1]['values']['data_segment_data_0'] == '33'
    assert to_send[0][1]['values']['data_segment_data'] == '[33, 1, 1, 1, 0, 0, 0, 0, 0]'

    assert to_send[0][1]['lat'] == 47.550865
    assert to_send[0][1]['lon'] == -122.377328

    assert len(to_send) == 8


def test_numurus_data_filter_dates():
    mapp = NumurusData('topic', filters={
        'start_date': datetime(2019, 7, 18, 15).replace(tzinfo=pytz.utc),
        'end_date': datetime(2019, 7, 18, 16).replace(tzinfo=pytz.utc)
    })

    to_send = []

    with open('./tests/numurus.data.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(mapp.message_to_values('fake', m))
            except BaseException as e:
                listen.L.error(repr(e))

    assert len(to_send) == 2


def test_arete_data_parse():
    mapp = AreteData('topic')

    to_send = []

    with open('./tests/arete_data.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(mapp.message_to_values('fake', m))
            except BaseException as e:
                listen.L.error(repr(e))

    assert len(to_send) == 137

    msg = to_send[-5][1]
    assert msg['lat'] == 38.859378814697266
    assert msg['lon'] == -77.0494384765625

    msg = to_send[-4][1]
    assert msg['lat'] == 32.70533
    assert msg['lon'] == -117.23613

    msg = to_send[-3][1]
    assert msg['lat'] == 532.6271
    assert msg['lon'] == -117.89201
    assert msg['values']['location_quality'] == '4'  # bad

    msg = to_send[-2][1]
    assert msg['lat'] == 32.627373
    assert msg['lon'] == -117.91643

    msg = to_send[-1][1]
    assert msg['lat'] == 32.62755
    assert msg['lon'] == -117.94065


def test_arete_data_filter_dates():
    mapp = AreteData('topic', filters={
        'start_date': datetime(2019, 8, 9, 0).replace(tzinfo=pytz.utc)
    })

    to_send = []

    with open('./tests/arete_data.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(mapp.message_to_values('fake', m))
            except BaseException as e:
                listen.L.error(repr(e))

    assert len(to_send) == 14


def test_just_json():
    mapp = JsonMap('topic')

    to_send = []

    with open('./tests/mission_sensors.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(mapp.message_to_values('fake', m))
            except BaseException as e:
                listen.L.error(repr(e))

    assert len(to_send) == 10

    assert to_send[0][1]['key'] == 'fake'

    assert to_send[0][1]['payload'] == {
        "cdr_reference" : -5699810423388316158,
        "headers" : {
            "imei" : -1556323178,
            "iridium_ts" : 1558640014,
            "sbd_session_status" : "PROTOCOL_ANOMALY",
            "mo_msn" : -725951606,
            "mt_msn" : -419825455,
            "location" : {
                "cep_radius" : 158880407,
                "latitude" : {
                    "degrees" : 34,
                    "minutes" : 0.803512
                },
                "longitude" : {
                    "degrees" : -118,
                    "minutes" : 0.3486771
                }
            }
        },
        "values" : {
            "mission_ts" : 1194313350,
            "rf_ais_decoded_rssi" : 1825254200,
            "misc" : None
        },
        "mfr" : "Numerus"
    }


def test_geography_driftworker_trajectories_individual():
    mapp = GenericGeography('topic')

    to_send = []

    with open('./tests/driftworker-traj-ind.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(mapp.message_to_values('fake', m))
            except BaseException as e:
                listen.L.error(repr(e))
    assert len(to_send) == 10


def test_geography_driftworker_trajectories_multi():
    mapp = GenericGeography('topic')

    to_send = []

    with open('./tests/driftworker-traj-multi.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(mapp.message_to_values('fake', m))
            except BaseException as e:
                listen.L.error(repr(e))
    assert len(to_send) == 1


def test_geography_driftworker_envelopes():
    mapp = GenericGeography('topic')

    to_send = []

    with open('./tests/driftworker-envelopes.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(mapp.message_to_values('fake', m))
            except BaseException as e:
                listen.L.error(repr(e))
    assert len(to_send) == 4


def test_geography_scuttle_watch_regions():
    mapp = GenericGeography('topic')

    to_send = []

    with open('./tests/scuttle-watch-regions.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(mapp.message_to_values('fake', m))
            except BaseException as e:
                listen.L.error(repr(e))
    assert len(to_send) == 6

    assert to_send[0][1]['uid'] == "Keepin Hi"
    assert 'gid' not in to_send[0][1]
    assert to_send[0][1]['time'] == "2019-09-06T00:00:00+00:00"
    assert to_send[2][1]['values'] == {}

    assert to_send[1][1]['uid'] == "Keepin HiHi"
    assert 'gid' not in to_send[1][1]
    assert to_send[1][1]['time'] == "2019-09-06T00:00:00+00:00"
    assert to_send[2][1]['values'] == {}

    assert to_send[2][1]['uid'] == "Keepin Med"
    assert 'gid' not in to_send[1][1]
    assert to_send[2][1]['time'] == "2019-09-06T00:00:00+00:00"
    assert to_send[2][1]['values'] == {}


@pytest.mark.kafka
def test_simple_listen_to_return():

    producer = EasyProducer(
        kafka_brokers=['localhost:4001'],
        kafka_topic='arete-data',
        kafka_conf={
            'queue.buffering.max.messages': 50
        }
    )

    datafile = Path('tests/arete_data.json')
    with datafile.open() as f:
        messages = json.load(f)
        for m in messages:
            producer.produce([(None, json.dumps(m))], batch=10, flush_timeout=10)

    mapping = AreteData('arete-data', filters={
        'start_date': datetime(2019, 12, 1, 0).replace(tzinfo=pytz.utc)
    })

    _ = utils.listen_unpack(
        brokers='localhost:4001',
        topic='arete-data',
        offset='earliest',
        packing='json',
        mapping=mapping,
        consumer=None,
        registry=None,
        on_receive=None,
        loop=False
    )


@pytest.mark.integration
def test_numurus_status_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'topic',
        '--lookup', 'NumurusStatus',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--no-listen',
        '--no-do-inserts',
        '--datafile', str(Path('tests/numurus.status.json').resolve()),
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_numurus_data_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'topic',
        '--lookup', 'NumurusData',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--no-listen',
        '--no-do-inserts',
        '--datafile', str(Path('tests/numurus.data.json').resolve()),
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_numurus_data_live_filter_dates():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'topic',
        '--lookup', 'NumurusData',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--no-listen',
        '--no-do-inserts',
        '--start_date', '2019-07-18T15:00:00',
        '--end_date', '2019-07-18T16:00:00',
        '--datafile', str(Path('tests/numurus.data.json').resolve()),
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_arete_data_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'topic',
        '--lookup', 'AreteData',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--no-listen',
        '--no-do-inserts',
        '--datafile', str(Path('tests/arete_data.json').resolve()),
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_health_and_status_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'topic',
        '--lookup', 'NwicFloatReports',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--no-listen',
        '--no-do-inserts',
        '--datafile', str(Path('tests/health_and_status.json').resolve()),
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_ncreplayer_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'topic',
        '--lookup', 'GenericFloat',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--no-listen',
        '--no-do-inserts',
        '--datafile', str(Path('tests/replayer.json').resolve()),
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_environmental_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'topic',
        '--lookup', 'NwicFloatReports',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--no-listen',
        '--no-do-inserts',
        '--datafile', str(Path('tests/environmental.json').resolve()),
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_mission_sensors_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'topic',
        '--lookup', 'NwicFloatReports',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--no-listen',
        '--no-do-inserts',
        '--datafile', str(Path('tests/mission_sensors.json').resolve()),
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_json_payload():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'topic',
        '--lookup', 'JsonMap',
        '--packing', 'json',
        '--drop',
        '--no-listen',
        '--no-do-inserts',
        '--datafile', str(Path('tests/environmental.json').resolve()),
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_geography_integration():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'geography-integration-test',
        '--table', 'my-geography-table',
        '--lookup', 'GenericGeography',
        '--packing', 'json',
        '--drop',
        '--no-listen',
        '--datafile', str(Path('tests/scuttle-watch-regions.json').resolve()),
        '-v'
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_json_integration():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'json-integration-test',
        '--table', 'my-json-table',
        '--lookup', 'JsonMap',
        '--packing', 'json',
        '--drop',
        '--no-listen',
        '--datafile', str(Path('tests/environmental.json').resolve()),
        '-v'
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_genericfloat_integration():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'genericfloat-integration-test',
        '--table', 'my-genericfloat-table',
        '--lookup', 'GenericFloat',
        '--packing', 'json',
        '--drop',
        '--no-listen',
        '--datafile', str(Path('tests/replayer.json').resolve()),
        '-v'
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_nwicfloat_integration():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'nwicfloat-integration-test',
        '--table', 'my-nwicfloat-table',
        '--lookup', 'NwicFloatReports',
        '--packing', 'json',
        '--drop',
        '--no-listen',
        '--datafile', str(Path('tests/health_and_status.json').resolve()),
        '-v'
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_geography_envelopes():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'driftworker-envelopes-test',
        '--table', 'driftworker-envelopes',
        '--lookup', 'GenericGeography',
        '--packing', 'json',
        '--drop',
        '--no-listen',
        '--datafile', str(Path('tests/driftworker-envelopes.json').resolve()),
        '-v'
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_geography_traj_ind():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'driftworker-traj-ind-test',
        '--table', 'driftworker-traj-ind',
        '--lookup', 'GenericGeography',
        '--packing', 'json',
        '--no-drop',
        '--truncate',
        '--no-listen',
        '--datafile', str(Path('tests/driftworker-traj-ind.json').resolve()),
        '-v'
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_geography_traj_multi():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'driftworker-traj-multi-test',
        '--table', 'driftworker-traj-multi',
        '--lookup', 'GenericGeography',
        '--packing', 'json',
        '--no-drop',
        '--truncate',
        '--no-listen',
        '--datafile', str(Path('tests/driftworker-traj-multi.json').resolve()),
        '-v'
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_arete_geography():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'arete-data-test',
        '--table', 'arete-data',
        '--lookup', 'AreteData',
        '--packing', 'json',
        '--no-drop',
        '--truncate',
        '--no-listen',
        '--datafile', str(Path('tests/arete_data.json').resolve()),
        '-v'
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_numurus_data_geography():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'numurus-data-test',
        '--table', 'numurus-data',
        '--lookup', 'NumurusData',
        '--packing', 'json',
        '--no-drop',
        '--truncate',
        '--no-listen',
        '--datafile', str(Path('tests/numurus.data.json').resolve()),
        '-v'
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_numurus_data_geography_uppercase():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'numurus-data-test',
        '--table', 'Numurus-Data-Uppercase',
        '--lookup', 'NumurusData',
        '--packing', 'json',
        '--no-drop',
        '--truncate',
        '--no-listen',
        '--datafile', str(Path('tests/numurus.data.json').resolve()),
        '-v'
    ])
    print(result)
    assert result.exit_code == 0


@pytest.mark.integration
def test_numurus_status_geography():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'numurus-status-test',
        '--table', 'numurus-status',
        '--lookup', 'NumurusStatus',
        '--packing', 'json',
        '--no-drop',
        '--truncate',
        '--no-listen',
        '--datafile', str(Path('tests/numurus.status.json').resolve()),
        '-v'
    ])
    print(result)
    assert result.exit_code == 0


def test_flatten():
    to_send = []

    with open('./tests/test_expanded_objects.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(flatten(m))
            except BaseException as e:
                listen.L.error(repr(e))

    matches = {
        0: {
            'values_misc_Name': "pickup_detection",
            'values_misc_detection_results_correlated_movement': False,
        },
        1: {
            'values_misc_Name': "GPS_rdp_variable",
            'values_misc_points': [
                [1569230478.0, 29.2534, -90.6609],
                [1569230778.0, 29.2534, -90.6609],
                [1569231078.0, 29.2534, -90.6609],
                [1569231378.0, 29.2534, -90.6609],
                [1569231678.0, 29.2534, -90.6609],
                [1569231978.0, 29.2534, -90.6609],
                [1569232278.0, 29.2534, -90.6609],
                [1569232578.0, 29.2534, -90.6609],
                [1569232878.0, 29.2534, -90.6609]
            ],
            'values_misc_points_0': [1569230478.0, 29.2534, -90.6609],
            'values_misc_points_0_0': 1569230478.0,
            'values_misc_points_0_1': 29.2534,
            'values_misc_points_0_2': -90.6609,
            'values_misc_points_8': [1569232878.0, 29.2534, -90.6609],
            'values_misc_points_8_0': 1569232878.0,
            'values_misc_points_8_1': 29.2534,
            'values_misc_points_8_2': -90.6609,
        },
        2: {
            'data_segment_data': [33, 1, 1, 1, 0, 0, 0, 0, 0],
            'data_segment_data_0': 33,
        }
    }
    for record, m in matches.items():
        for k, v in m.items():
            assert to_send[record][k] == v


def test_parsing_string_json_fields():
    mapp = NwicFloatReports('foo')

    to_send = []

    with open('./tests/h_a_s_with_gps_points.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(mapp.message_to_values('fake', m))

    assert len(to_send) == 50
    values = to_send[0][1]['values']
    assert values['values_misc_detection_results_correlated_movement'] == "False"
    assert values['values_misc_detection_results_tilt_angle'] == "False"
    assert values['values_misc_detection_results_velocity_and_distance'] == "False"

    values = to_send[16][1]['values']
    assert values['values_misc_points'] == str([
        [1569230478.0, 29.2534, -90.6609],
        [1569230778.0, 29.2534, -90.6609],
        [1569231078.0, 29.2534, -90.6609],
        [1569231378.0, 29.2534, -90.6609],
        [1569231678.0, 29.2534, -90.6609],
        [1569231978.0, 29.2534, -90.6609],
        [1569232278.0, 29.2534, -90.6609],
        [1569232578.0, 29.2534, -90.6609],
        [1569232878.0, 29.2534, -90.6609]
    ])
    assert values['values_misc_points_0'] == str([1569230478.0, 29.2534, -90.6609])
    assert values['values_misc_points_0_0'] == str(1569230478.0)
    assert values['values_misc_points_0_1'] == str(29.2534)
    assert values['values_misc_points_0_2'] == str(-90.6609)
    assert values['values_misc_points_8'] == str([1569232878.0, 29.2534, -90.6609])
    assert values['values_misc_points_8_0'] == str(1569232878.0)
    assert values['values_misc_points_8_1'] == str(29.2534)
    assert values['values_misc_points_8_2'] == str(-90.6609)


def test_statistics():
    mapp = GenericFieldStatistic('topic')

    to_send = []

    with open('./tests/statistics.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(mapp.message_to_values('fake', m))

    assert len(to_send) == 4

    assert to_send[0][1] == {
        "source": "gom-02-combined",
        "period": "monthly",
        "starting": dtparse("2020-01-01T00:00:00Z"),
        "ending": dtparse("2020-02-01T00:00:00Z"),
        "values": {
            "Analysis_1": 10,
            "Analysis_2": 20,
            "Analysis_3": 30,
            "Analysis_4": 40,
            "Field_A": 50,
            "Field_B": 60,
            "Field_C": 70,
            "Field_D": 80
        }
    }

    assert to_send[1][1] == {
        "source": "gom-02-combined",
        "period": "daily",
        "starting": dtparse("2020-01-01T00:00:00Z"),
        "ending": dtparse("2020-01-02T00:00:00Z"),
        "values": {
            "Analysis_1": 10,
            "Analysis_2": 20,
            "Analysis_3": 30,
            "Analysis_4": 40,
            "Field_A": 50,
            "Field_B": 60,
            "Field_C": 70,
            "Field_D": 80
        }
    }

    assert to_send[2][1] == {
        "source": "gom-02-combined",
        "period": "instant",
        "starting": dtparse("2020-01-01T00:00:00Z"),
        "ending": dtparse("2020-01-01T00:00:00Z"),
        "values": {
            "Analysis_1": 10,
            "Analysis_2": 20,
            "Analysis_3": 30,
            "Analysis_4": 40,
            "Field_A": 50,
            "Field_B": 60,
            "Field_C": 70,
            "Field_D": 80
        }
    }


@pytest.mark.integration
def test_statistics_integration():

    # Drop
    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'field_statistics_testing',
        '--table', 'field_statistics',
        '--lookup', 'GenericFieldStatistic',
        '--packing', 'json',
        '--drop',
        '--truncate',
        '--no-listen',
        '--datafile', str(Path('tests/statistics.json').resolve()),
        '--start_date', '2019-01-01',
        '--end_date', '2022-01-01',
        '-v'
    ])
    print(result)
    assert result.exit_code == 0

    # Truncate
    result = runner.invoke(listen.setup, [
        '--topic', 'field_statistics_testing',
        '--table', 'field_statistics',
        '--lookup', 'GenericFieldStatistic',
        '--packing', 'json',
        '--no-drop',
        '--truncate',
        '--no-listen',
        '--datafile', str(Path('tests/statistics.json').resolve()),
        '--start_date', '2019-01-01',
        '--end_date', '2022-01-01',
        '-v'
    ])
    print(result)
    assert result.exit_code == 0

    # Update (no action)
    result = runner.invoke(listen.setup, [
        '--topic', 'field_statistics_testing',
        '--table', 'field_statistics',
        '--lookup', 'GenericFieldStatistic',
        '--packing', 'json',
        '--no-drop',
        '--no-truncate',
        '--no-listen',
        '--datafile', str(Path('tests/statistics.json').resolve()),
        '--start_date', '2019-01-01',
        '--end_date', '2022-01-01',
        '-v'
    ])
    print(result.stdout)
    assert result.exit_code == 0


def test_base64_images():
    mapp = GenericFloat('topic')

    to_send = []

    with open('./tests/base64_images.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(mapp.message_to_values('fake', m))

    assert len(to_send) == 3

    assert to_send[0][1]['values']['image_One'] == "data:image/jpeg;base64,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"
    assert to_send[1][1]['values']['image_One'] == "data:image/jpeg;base64,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"
    assert to_send[2][1]['values']['image_One'] == "data:image/jpeg;base64,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"
