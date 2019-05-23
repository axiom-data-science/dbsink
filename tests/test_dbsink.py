#!python
# coding=utf-8
import json
from pathlib import Path

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
    newtopic, cols, message_to_value = columns_and_message_conversion('axds-netcdf-replayer-data')

    to_send = []

    with open('./tests/replayer.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(message_to_value('fake', m))
    assert len(to_send) == 4


def test_mission_sensors():
    newtopic, cols, message_to_value = columns_and_message_conversion('oot.reports.mission_sensors')

    to_send = []

    with open('./tests/mission_sensors.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(message_to_value('fake', m))
    assert len(to_send) == 10


def test_environmental():
    newtopic, cols, message_to_value = columns_and_message_conversion('oot.reports.environmental')

    to_send = []

    with open('./tests/environmental.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(message_to_value('fake', m))
    assert len(to_send) == 10


def test_health_and_status():
    newtopic, cols, message_to_value = columns_and_message_conversion('oot.reports.health_and_status')

    to_send = []

    with open('./tests/health_and_status.json') as f:
        messages = json.load(f)
        for m in messages:
            to_send.append(message_to_value('fake', m))
    assert len(to_send) == 10


def test_health_and_status_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'oot.reports.health_and_status',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--mockfile', str(Path('tests/health_and_status.json').resolve()),
    ])

    assert result.exit_code == 0


def test_ncreplayer_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'axds-netcdf-replayer-data',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--mockfile', str(Path('tests/replayer.json').resolve()),
    ])

    assert result.exit_code == 0


def test_environmental_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'oot.reports.environmental',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--mockfile', str(Path('tests/environmental.json').resolve()),
    ])

    assert result.exit_code == 0


def test_mission_sensors_live():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--topic', 'oot.reports.mission_sensors',
        '--packing', 'json',
        '--consumer', 'dbsink-test',
        '--mockfile', str(Path('tests/mission_sensors.json').resolve()),
    ])

    assert result.exit_code == 0
