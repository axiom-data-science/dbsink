#!python
# coding=utf-8
from pathlib import Path
import simplejson as json

from click.testing import CliRunner

from dbsink import listen
from dbsink.tables import columns_and_message_conversion


def test_listen_help():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--help'
    ])
    assert result.exit_code == 0


def test_ncreplayer():
    _, _, _, message_to_value = columns_and_message_conversion('axds-netcdf-replayer-data')

    to_send = []

    with open('./tests/replayer.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(message_to_value('fake', m))
    assert len(to_send) == 4
    assert to_send[0][1]['time'] == to_send[0][1]['reftime']


def test_mission_sensors():
    _, _, _, message_to_value = columns_and_message_conversion('oot.reports.mission_sensors')

    to_send = []

    with open('./tests/mission_sensors.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(message_to_value('fake', m))
    assert len(to_send) == 10


def test_environmental():
    _, _, _, message_to_value = columns_and_message_conversion('oot.reports.environmental')

    to_send = []

    with open('./tests/environmental.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(message_to_value('fake', m))
    assert len(to_send) == 10


def test_health_and_status():
    _, _, _, message_to_value = columns_and_message_conversion('oot.reports.health_and_status')

    to_send = []

    with open('./tests/health_and_status.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(message_to_value('fake', m))
    assert len(to_send) == 516


def test_null_infinity():
    _, _, _, message_to_value = columns_and_message_conversion('whatever', lookup='just_json')

    to_send = []

    with open('./tests/null_infinity.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(message_to_value('fake', m))
    assert len(to_send) == 2
    assert to_send[0][1]['payload']['bus_voltage'] is None
    assert to_send[1][1]['payload']['bus_voltage'] is None


def test_health_and_status_with_lookup():
    _, _, _, message_to_value = columns_and_message_conversion(
        'somethingelse', lookup='float_reports'
    )

    to_send = []

    with open('./tests/health_and_status.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(message_to_value('fake', m))
    assert len(to_send) == 516

    f = to_send[0][1]
    assert f['uid'] == '300434063547170'
    assert f['lat'] == 32.704426
    assert f['lon'] == -117.23662
    assert f['time'] == '2019-05-31T20:39:50'
    assert f['values']['status_ts'] == '1559335190'
    assert f['values']['iridium_ts'] == '1559335196'
    assert f['values']['iridium_lat'] == '32.70308'
    assert f['values']['iridium_lon'] == '-116.72858'
    assert f['values']['latitude'] == '32.704426'
    assert f['values']['longitude'] == '-117.23662'
    assert f['values']['speed'] == '2.72'
    assert f['values']['test_num'] == 'T240'
    assert f['values']['mfr'] == 'usna'

    l = to_send[-1][1]
    assert l['uid'] == '300434063946390'
    assert l['lat'] == 39.01338
    assert l['lon'] == -75.47597
    assert l['time'] == '2019-06-06T18:19:56'
    assert 'status_ts' not in l['values']
    assert l['values']['iridium_ts'] == '1559845196'
    assert l['values']['iridium_lat'] == '39.01338'
    assert l['values']['iridium_lon'] == '-75.47597'
    assert 'latitude' not in l['values']
    assert l['values']['longitude'] is None
    assert l['values']['speed'] == '0.01'
    assert l['values']['test_num'] == 'T76'
    assert l['values']['mfr'] == 'usna'


def test_numurus_status():
    _, _, _, message_to_value = columns_and_message_conversion('numurus.status')

    to_send = []

    with open('./tests/numurus.status.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(message_to_value('fake', m))
            except BaseException as e:
                listen.L.error(repr(e))
    assert len(to_send) == 87


def test_arete_data_parse():
    _, _, _, message_to_value = columns_and_message_conversion('arete.data')

    to_send = []

    with open('./tests/arete_data.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(message_to_value('fake', m))
            except BaseException as e:
                listen.L.error(repr(e))
    assert len(to_send) == 6


def test_just_json():
    _, _, _, message_to_value = columns_and_message_conversion('just_json')

    to_send = []

    with open('./tests/mission_sensors.json') as f:
        messages = json.load(f)
        for m in messages:
            try:
                to_send.append(message_to_value('fake', m))
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


def test_numurus_status_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'numurus.status',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--mockfile', str(Path('tests/numurus.status.json').resolve()),
    ])
    assert result.exit_code == 0


def test_arete_data_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'arete.data',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--mockfile', str(Path('tests/arete_data.json').resolve()),
    ])
    assert result.exit_code == 0


def test_health_and_status_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'oot.reports.health_and_status',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--mockfile', str(Path('tests/health_and_status.json').resolve()),
    ])
    assert result.exit_code == 0


def test_ncreplayer_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'axds-netcdf-replayer-data',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--mockfile', str(Path('tests/replayer.json').resolve()),
    ])
    assert result.exit_code == 0


def test_environmental_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'oot.reports.environmental',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--mockfile', str(Path('tests/environmental.json').resolve()),
    ])
    assert result.exit_code == 0


def test_mission_sensors_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'oot.reports.mission_sensors',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--drop',
        '--mockfile', str(Path('tests/mission_sensors.json').resolve()),
    ])
    assert result.exit_code == 0


def test_lookup():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'something_not_in_lookup',
        '--packing', 'json',
        '--lookup', 'oot.reports.mission_sensors',
        '--consumer', 'dbsink-test-lookup',
        '--drop',
        '--mockfile', str(Path('tests/mission_sensors.json').resolve()),
    ])
    assert result.exit_code == 0


def test_json_payload():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'something_not_in_lookup',
        '--packing', 'json',
        '--lookup', 'just_json',
        '--consumer', 'dbsink-test-lookup',
        '--drop',
        '--mockfile', str(Path('tests/environmental.json').resolve()),
    ])
    assert result.exit_code == 0


def test_generate_consumer():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'something_not_in_lookup',
        '--packing', 'json',
        '--lookup', 'just_json',
        '--drop',
        '--mockfile', str(Path('tests/environmental.json').resolve()),
    ])
    assert result.exit_code == 0
