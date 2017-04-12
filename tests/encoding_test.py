#coding: utf-8
from tanker import View

from .base_test import session


def test_str(session):
    japan = '日本'
    team_view = View('country', ['name'])
    team_view.write([(japan,)])

    row = team_view.read(filters={'name': japan}).next()
    assert row[0] == japan

    fltr = '(= name "%s")' % japan
    row = team_view.read(fltr).next()
    assert row[0] == japan


def test_unicode(session):
    korea = u'Corée'
    korea_str = korea.encode('utf-8')
    team_view = View('country', ['name'])
    team_view.write([(korea,)])

    row = team_view.read(filters={'name': korea}).next()
    assert row[0] == korea_str

    fltr = '(= name "%s")' % korea_str
    row = team_view.read(fltr).next()
    assert row[0] == korea_str
