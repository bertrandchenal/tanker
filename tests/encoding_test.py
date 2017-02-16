#coding: utf-8
from tanker import View

from .base_test import session


def test_read_write(session):
    japan = '日本'
    team_view = View('country', ['name'])
    team_view.write([(japan,)])

    row = team_view.read(filters={'name': japan}).next()
    assert row[0] == japan

    fltr = '(= name "%s")' % japan
    row = team_view.read(fltr).next()
    assert row[0] == japan
