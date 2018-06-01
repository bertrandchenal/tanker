import os

import pytest

from tanker import (connect, create_tables, View, logger, yaml_load, fetch,
                    save, execute, Table, Pool)


DB_URIS = [
    'sqlite:///test.db',
    'postgresql:///tanker_test',
    'postgresql:///tanker_test#test_schema',
]

verbose = pytest.config.getoption('verbose', 0) > 0
logger.setLevel('DEBUG' if verbose else 'WARNING')


# Tables definitions can be written in yaml
yaml_def = '''
- table: team
  columns:
    name: varchar
    country: m2o country.id
    members: o2m member.team
  key:
    - name
    - country
- table: country
  columns:
    name: varchar
    teams: o2m team.country
    licensees: o2m licensee.country
  key:
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
  key:
    - registration_code
  defaults:
    created_at: current_timestamp
  use-index: brin

- table: licensee
  columns:
    country: m2o country.id
    member: m2o member.id
  key:
    - country
    - member

- table: kitchensink
  columns:
    index: integer
    "true": bool
    "false": bool
    "null": varchar
    integer: integer
    bigint: bigint
    float: float
    bool: bool
    timestamp: timestamp
    date: date
    varchar: varchar
    int_array: integer[]
    float_array: float[]
    bool_array: bool[]
    ts_array: timestamp[][]
    char_array: varchar[][][]
    floor: float
    epoch: timestamp
    year: timestamp
    jsonb: jsonb
  key:
    - index

- table: timeseries
  columns:
    timestamp: timestamp
    timestamptz: timestamptz
    date: date
  index:
    - timestamp
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


@pytest.yield_fixture(scope='function', params=DB_URIS)
def session(request):
    cfg = {
        'db_uri': request.param,
        'schema': SCHEMA,
    }

    is_sqlite = request.param.startswith('sqlite')
    use_schema = '#' in request.param

    # DB cleanup
    if is_sqlite and os.path.isfile('test.db'):
        os.unlink('test.db')
    else:
        with connect(cfg):
            to_clean = [t['table'] for t in SCHEMA]
            for table in to_clean:
                if use_schema:
                    table = 'test_schema.' + table
                qr = 'DROP TABLE IF EXISTS %s' % table
                if not is_sqlite:
                    qr += ' CASCADE'
                execute(qr)

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

def test_one(session):
    expected = ('Belgium',)
    assert expected == View('country', ['name']).read().one()

    expected = None
    fltr = '(= name "Prussia")'
    assert expected == View('country', ['name']).read(fltr).one()

def test_chain(session):
    expected = ['Belgium', 'France', 'Holland']
    res = sorted(View('country', ['name']).read().chain())
    assert expected == res

    expected = ['Blue', 'Belgium', 'Blue', 'France', 'Red', 'Belgium']
    view = View('team', ['name', 'country.name'])
    res = view.read(order=['name', 'country.name']).chain()
    assert expected == list(res)


def test_link(session):
    member = Table.get('member')
    country = Table.get('country')
    team = Table.get('team')

    expected = (
        '[[<Column team M2O>, <Column country M2O>], '
        '[<Column team M2O>, <Column country M2O>, '
         '<Column licensees O2M>, <Column country M2O>]]'
    )
    assert str(member.link(country)) == expected

    expected = (
        '[[<Column country M2O>, <Column teams O2M>], '
        '[<Column members O2M>, <Column team M2O>], '
        '[<Column country M2O>, <Column teams O2M>], '
        '[<Column members O2M>, <Column team M2O>], '
        '[<Column country M2O>, <Column licensees O2M>, <Column country M2O>, '
          '<Column teams O2M>], '
        '[<Column country M2O>, <Column licensees O2M>, '
          '<Column member M2O>, <Column team M2O>]]'
    )
    assert str(team.link(team)) == expected

    expected = (
        '[[<Column teams O2M>, <Column members O2M>], '
        '[<Column licensees O2M>, <Column member M2O>]]'
    )
    assert str(country.link(member)) == expected
