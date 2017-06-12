#coding: utf-8
import sys

from tanker import View

from .base_test import session

PY2 = sys.version_info[0] == 2

def test_str(session):
    japan = '日本'
    team_view = View('country', ['name'])
    team_view.write([(japan,)])

    row = team_view.read(filters={'name': japan}).one()
    assert row[0] == japan

    fltr = '(= name "%s")' % japan
    row = team_view.read(fltr).one()
    assert row[0] == japan


def test_unicode(session):
    korea = u'Corée'
    if PY2:
        korea = korea.encode('utf-8')

    team_view = View('country', ['name'])
    team_view.write([(korea,)])

    row = team_view.read(filters={'name': korea}).one()
    assert row[0] == korea

    fltr = '(= name "%s")' % korea
    row = team_view.read(fltr).one()
    assert row[0] == korea
