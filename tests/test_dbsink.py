#!python
# coding=utf-8
from pathlib import Path
import simplejson as json

import pytest
from click.testing import CliRunner

from dbsink import listen
from dbsink.maps import *  # noqa
from dbsink.tables import *  # noqa


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

    assert len(to_send) == 134

    msg = to_send[-2][1]
    assert msg['lat'] == 38.859378814697266
    assert msg['lon'] == -77.0494384765625

    msg = to_send[-1][1]
    assert msg['lat'] == 32.70533
    assert msg['lon'] == -117.23613


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

    assert len(to_send) == 11


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


# def test_geography_scuttle_boundary_forecast():
#     mapp = GenericGeography('topic')

#     to_send = []

#     with open('./tests/scuttle-boundary-forecast.json') as f:
#         messages = json.load(f)
#         for m in messages:
#             try:
#                 to_send.append(mapp.message_to_values('fake', m))
#             except BaseException as e:
#                 listen.L.error(repr(e))


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
        '--drop',
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
        '--drop',
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
        '--drop',
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
        '--drop',
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
        '--drop',
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
