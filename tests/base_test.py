import os
import pytest
import yaml

from tanker import (connect, create_tables, View, logger, yaml_load, fetch,
                    save, execute)

DB_FILE = ':memory:' #'test.db'
logger.setLevel('ERROR')

# Tables definitions can be written in yaml
yaml_def = '''
- table: team
  columns:
    name: varchar
    country: m2o country.id
  index:
    - name
- table: country
  columns:
    name: varchar
  index:
    - name
  values:
    - name: Belgium
    - name: France
    - name: Holland

- table: member
  columns:
    name: varchar
    registration_code: varchar
    team: m2o team.id
  index:
    - registration_code
'''

schema = yaml_load(yaml_def)
teams = [
    ['Red', 'Belgium'],
    ['Blue', 'France'],
]
members = [
    ['Bob', 'Belgium', 'Blue', '001'],
    ['Alice', 'Belgium', 'Red', '002'],
    ['Trudy', 'France', 'Blue', '003'],
]


@pytest.yield_fixture(scope='function', params=['sqlite', 'pg'])
def session(request):
    if request.param == 'sqlite':
        # Remove previous db
        if DB_FILE != ':memory:' and os.path.exists(DB_FILE):
            os.unlink(DB_FILE)

        cfg = {
            'db_uri': 'sqlite:///' + DB_FILE,
            'schema': schema,
        }

    elif request.param == 'pg':
        cfg = {
            'db_uri': 'postgresql:///tanker_test',
            'schema': schema,
        }
        with connect(cfg):
            execute('''
            DROP TABLE IF EXISTS member;
            DROP TABLE IF EXISTS team;
            DROP TABLE IF EXISTS country;
            ''')
    # for cfg in (sqlite_cfg, pg_cfg):
    with connect(cfg):
        create_tables()
        View('team', ['name', 'country.name']).write(teams)
        yield


def check(expected, result, check_order=False):
    result = list(result)
    if not check_order:
        result = sorted(result)
        expected = sorted(expected)
    assert result == expected


def test_load(session):
    expected = [('Belgium',), ('France',), ('Holland',)]
    check(expected, View('country', ['name']).read())


def test_write(session):
    team_view = View('team', ['name', 'country.name'])
    team_view.write([('Orange', 'Holland')])

    expected = [('Red', 'Belgium',),
                ('Blue', 'France',),
                ('Orange', 'Holland',)]
    res = team_view.read()
    check(expected, res)


def test_no_insert(session):
    team_view = View('team', ['name', 'country.name'])
    team_view.write([
        ('Orange', 'Holland'), # This is an insert
        ('Blue', 'Belgium'),
    ], insert=False)

    expected = [('Red', 'Belgium',),
                ('Blue', 'Belgium',),]
    res = team_view.read()
    check(expected, res)


def test_no_update(session):
    team_view = View('team', ['name', 'country.name'])
    team_view.write([
        ('Orange', 'Holland'),
        ('Blue', 'Belgium'), # This is an update of Blue team
    ], update=False)

    expected = [('Red', 'Belgium',),
                ('Blue', 'France',),
                ('Orange', 'Holland',)]
    res = team_view.read()
    check(expected, res)


def test_purge(session):
    team_view = View('team', ['name', 'country.name'])
    team_view.write([
        ('Orange', 'Holland'),
        ('Blue', 'France'),
    ], purge=True)

    expected = [('Blue', 'France',),
                ('Orange', 'Holland',)]
    res = team_view.read()
    check(expected, res)


def test_delete_data(session):
    team_view = View('team', ['name'])
    team_view.delete([('Blue',)])

    expected = [('Red',)]
    res = team_view.read()
    check(expected, res)

def test_delete_filter_by(session):
    team_view = View('team', ['name'])
    team_view.delete(filter_by={'name': 'Blue'})

    expected = [('Red',)]
    res = team_view.read()
    check(expected, res)

def test_delete_filter(session):
    team_view = View('team', ['name'])
    team_view.delete(filters='(= name "Blue")')

    expected = [('Red',)]
    res = team_view.read()
    check(expected, res)


def test_fetch_save(session):
    save('team', {
        'name': 'Red',
        'country.name': 'France',
    })

    country_view = View('country', ['id'])
    france_id, = next(country_view.read(filter_by={'name': 'France'}))
    assert fetch('team', {'name': 'Red'})['country'] == france_id
