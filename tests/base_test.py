import pytest

from tanker import (connect, create_tables, View, logger, yaml_load, fetch,
                    save, execute, Table)

SQLITE_FILE = 'test.db'
DB_TYPES = [
    'sqlite',
    'pg',
]

logger.setLevel('WARNING')

# Tables definitions can be written in yaml
yaml_def = '''
- table: team
  columns:
    name: varchar
    country: m2o country.id
    members: o2m member.team
  index:
    - name
    - country
- table: country
  columns:
    name: varchar
    teams: o2m team.country
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
    created_at: timestamp
    team: m2o team.id
  index:
    - registration_code
  defaults:
    created_at: current_timestamp
'''

SCHEMA = yaml_load(yaml_def)
teams = [
    ['Blue', 'Belgium'],
    ['Red', 'Belgium'],
    ['Blue', 'France'],
]
members = [
    ['Bob', 'Belgium', 'Blue', '001'],
    ['Alice', 'Belgium', 'Red', '002'],
    ['Trudy', 'France', 'Blue', '003'],
]


def get_config(db_type, schema=SCHEMA):
    if db_type == 'sqlite':
        db_uri = 'sqlite:///' + SQLITE_FILE
    elif db_type == 'pg':
        db_uri = 'postgresql:///tanker_test'

    cfg = {
        'db_uri': db_uri,
        'schema': schema,
    }

    with connect(cfg):
        to_clean = [t['table'] for t in schema]
        for table in to_clean:
            qr = 'DROP TABLE IF EXISTS %s' % table
            if db_type == 'pg':
                qr += ' CASCADE'
            execute(qr)
    return cfg


@pytest.yield_fixture(scope='function', params=DB_TYPES)
def session(request):
    cfg = get_config(request.param)
    with connect(cfg):
        create_tables()
        View('team', ['name', 'country.name']).write(teams)
        yield request.param


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
                ('Blue', 'Belgium',),
                ('Blue', 'France',),
                ('Orange', 'Holland',)]
    res = team_view.read()
    check(expected, res)

def test_fetch_save(session):
    save('member', {
        'registration_code': '007',
        'name': 'Bond'
    })

    assert fetch('member', {'registration_code': '007'})['name'] == 'Bond'

def test_next(session):
    expected = ('Belgium',)
    assert expected == next(View('country', ['name']).read())

    expected = None
    fltr = '(= name "Prussia")'
    assert expected == next(View('country', ['name']).read(fltr), None)

def test_link(session):
    member, country = Table.get('member'), Table.get('country')
    expected = '[[<Column team M2O>, <Column country M2O>]]'
    assert str(member.link(country)) == expected

    team = Table.get('team')
    expected = '[[<Column country M2O>, <Column teams O2M>],'\
               ' [<Column members O2M>, <Column team M2O>]]'
    assert str(team.link(team)) == expected
