#!python
# coding=utf-8
from click.testing import CliRunner

from dbsink import listen


def test_listen_help():

    runner = CliRunner()
    result = runner.invoke(listen.setup, [
        '--help'
    ])
    assert result.exit_code == 0
