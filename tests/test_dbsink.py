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

    assert len(to_send) == 8


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

    assert len(to_send) == 133

    msg = to_send[-1][1]
    assert msg['lat'] == 38.859378814697266
    assert msg['lon'] == -77.0494384765625


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


# def test_geography_driftworker_trajectories():
#     mapp = GenericGeography('topic')

#     to_send = []

#     with open('./tests/driftworker-trajectories.json') as f:
#         messages = json.load(f)
#         for m in messages:
#             try:
#                 to_send.append(mapp.message_to_values('fake', m))
#             except BaseException as e:
#                 listen.L.error(repr(e))


# def test_geography_driftworker_envelopes():
#     mapp = GenericGeography('topic')

#     to_send = []

#     with open('./tests/driftworker-envelopes.json') as f:
#         messages = json.load(f)
#         for m in messages:
#             try:
#                 to_send.append(mapp.message_to_values('fake', m))
#             except BaseException as e:
#                 listen.L.error(repr(e))


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
        '-vvv'
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
