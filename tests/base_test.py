import pytest
import yaml

from tanker import connect, create_tables, View, logger, yaml_load

yaml_def = '''
- table: team
  columns:
    name: varchar
    country: m2o country.id
  index:
    - name
    - country
- table: country
  columns:
    name: varchar
  index:
    - name
  values:
    - name: Belgium
    - name: France
- table: member
  columns:
    name: varchar
    registration_code: varchar
    team: m2o team.id
  index:
    - registration_code
'''

cfg = {
    'db_uri': ':memory:',
    'schema': yaml_load(yaml_def),
}

teams = [
    ['Blue', 'Belgium'],
    ['Red', 'Belgium'],
    ['Blue', 'France'],
]
members = [
    ['Bob', 'Blue', '001', 'Belgium'],
    ['Alice', 'Red', '002', 'Belgium'],
    ['Trudy', 'Blue', '003', 'France'],
]


@pytest.yield_fixture(scope='function')
def session():
    # Remove previous db
    if cfg['db_uri'] != ':memory:' and os.path.exists(URI):
        os.unlink(cfg['db_uri'])

    with connect(cfg):
        try:
            view = View('team', ['name', 'country.name'])
            view.write(teams)
            view = View('member', [
                ('Name', 'name'),
                ('Team', 'team.name'),
                ('Code', 'registration_code'),
                ('Country', 'team.country.name'),
            ])
            view.write(members)
            yield 'session'
        except:
            raise
