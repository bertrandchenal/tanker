from tanker import connect, create_tables, View, logger, yaml_load

try:
    import yaml
except ImportError:
    yaml = None

# Tables definitions can be written in yaml
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


# Or we can use python litteral if the yaml module is missing
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
         'values': [
             {'name': 'Belgium'},
             {'name': 'France'}
         ],
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

# Some example data, in practive this data can come from web scrapers,
# excel sheets, panda DataFrame, etc.
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


def populate():
    # Add teams
    view = View('team', ['name', 'country.name'])
    view.write(teams)
    res = view.read()
    logger.info('Teams')
    for row in res:
        logger.info('\t' + str(row))

    # Show team and country ids
    view = View('team', ['id', 'name', 'country.id'])
    res = view.read()
    logger.info('Teams and county ids')
    for row in res:
        logger.info('\t' + str(row))


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
        res = view.read_df().values
    except ImportError:
        res = view.read()
    logger.info('Members details')
    for row in res:
        logger.info('\t' + str(row))

def delete():
    # Delete France
    view = View('country')
    view.write([['Belgium']], delete=True)
    res = view.read()
    logger.info('Remaining Countries')
    for row in res:
        logger.info('\t' + str(row))

    view = View('member')
    res = view.read()
    logger.info('Remaining Members')
    for row in res:
        logger.info('\t' + str(row))


if __name__ == '__main__':
    with connect(cfg):
        # Only needed the first time the db is accessed, or when tables,
        # column or values are added.  Note that countries will be
        # automatically loaded from the definitions
        create_tables()

        populate()
        delete()
