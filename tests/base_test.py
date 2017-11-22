from datetime import datetime, date
import os

import pytest

from tanker import (connect, create_tables, View, logger, yaml_load, fetch,
                    save, execute, Table, LRU, ctx)

SQLITE_FILE = 'test.db'
DB_TYPES = [
    'sqlite',
    'pg',
]
logger.setLevel('DEBUG' if pytest.config.getoption('verbose')
                else 'WARNING')

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
    licensees: o2m licensee.country
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

- table: licensee
  columns:
    country: m2o country.id
    member: m2o member.id
  index:
    - country
    - member

- table: kitchensink
  columns:
    index: integer
    integer: integer
    bigint: bigint
    float: float
    bool: bool
    timestamp: timestamp
    date: date
    varchar: varchar
    int_array: integer[]
    bool_array: bool[]
    ts_array: timestamp[][]
    char_array: varchar[][][]
  index:
    - index
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
    else:
        raise ValueError('Unsupported db type "%s"' % db_type)

    cfg = {
        'db_uri': db_uri,
        'schema': schema,
    }

    return cfg


@pytest.yield_fixture(scope='function', params=DB_TYPES)
def session(request):
    db_type = request.param
    cfg = get_config(db_type)

    # DB cleanup
    if db_type == 'sqlite' and os.path.isfile(SQLITE_FILE):
        os.unlink(SQLITE_FILE)
    else:
        with connect(cfg):
            to_clean = [t['table'] for t in SCHEMA]
            for table in to_clean:
                qr = 'DROP TABLE IF EXISTS %s' % table
                if db_type == 'pg':
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

def test_kitchensink(session):
    record = {
        'index': 1,
        'integer': 1,
        'bigint': 10000000000,
        'float': 1.0,
        'bool': True,
        'timestamp': datetime(1970, 1, 1),
        'date': date(1970, 1, 1),
        'varchar': 'varchar',
        'int_array': [1,2],
        'bool_array': [True, False],
        'ts_array': [
            [datetime(1970, 1, 1), datetime(1970, 1, 2)],
            [datetime(1970, 1, 3), datetime(1970, 1, 4)],
        ],
        'char_array': [
            [['ham', 'spam'], ['foo', 'bar']],
            [['foo', 'bar'], [None, None]],
        ],
    }


    # Write actual values
    ks_view = View('kitchensink')
    ks_view.write([record])
    res = list(ks_view.read().dict())[0]
    for k, v in record.items():
        if ctx.flavor == 'sqlite' and k.endswith('array'):
            # Array support with sqlite is incomplete
            continue
        assert res[k] == v

    # Filters
    for k, v in record.items():
        if isinstance(v, list):
            continue
        res = ks_view.read('(= %s {})' % k, args=[v]).all()
        assert len(res) == 1

    # Write nulls
    for k in record:
        if k == 'index':
            continue
        record[k] = None
    ks_view.write([record])
    res = list(ks_view.read().dict())[0]
    for k, v in record.items():
        assert res[k] == v
