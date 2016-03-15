from tanker import connect, create_tables, View, logger, yaml_load

try:
    import yaml
except ImportError:
    yaml = None


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
- table: member
  columns:
    name: varchar
    registration_code: varchar
    team: m2o team.id
  index:
    - registration_code
'''


if yaml is None:
    definitions = [
        {'table': 'team',
         'columns': {
             'name': 'varchar',
             'country': 'm2o country.id',
         },
         'index': ['name', 'country'],
        },
        {'table': 'country',
         'columns': {
             'name': 'varchar',
         },
         'index': ['name'],
        },
        {'table': 'member',
         'columns': {
             'name': 'varchar',
             'registration_code': 'varchar',
             'team': 'm2o team.id',
         },
         'index': ['registration_code'],
        },
    ]

else:
    definitions = yaml_load(yaml_def)

cfg = {
    'db_uri': 'sqlite:///test.db',
    'definitions': definitions,
}

countries = [['Belgium'], ['France']]
teams = [
    ['Blue', 'Belgium'],
    ['Red', 'Belgium'],
    ['Blue', 'France'],
]
members = [
    ['Bob', 'Blue', '001', 'Belgium'],
    ['Alice', 'Blue', '002', 'Belgium'],
    ['Trudy', 'Blue', '003', 'France'],
]

with connect(cfg):
    create_tables()

    # Add countries
    view = View('country')
    view.write(countries)
    res = view.read()
    logger.info(list(res))


    # Add teams
    view = View('team', ['name', 'country.name'])
    view.write(teams)
    res = view.read()
    logger.info(list(res))

    # Show team and country ids
    view = View('team', ['id', 'name', 'country.id'])
    res = view.read()
    logger.info(list(res))


    # Add members
    view = View('member', [
        ('Name', 'name'),
        ('Team', 'team.name'),
        ('Code', 'registration_code'),
        ('Country', 'team.country.name'),
    ])
    view.write(members)

    # Read them and check team id
    view = View('member', [
        ('Name', 'name'),
        ('Team ID', 'team.id'),
        ('Team Name', 'team.name'),
        ('Code', 'registration_code'),
        ('Country', 'team.country.name'),
    ])
    try:
        import pandas
        res = view.read_df()
    except ImportError:
        res = list(view.read())
    logger.info(res)


    # Delete France
    view = View('country')
    view.write([['Belgium']], delete=True)
    res = view.read()
    logger.info(list(res))

    view = View('member')
    res = view.read()
    logger.info(list(res))
