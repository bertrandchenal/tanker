from tanker import connect, create_tables, View
from tanker import logger

definitions = [
    {'table': 'team',
     'columns': {
         'name': 'varchar',
         'country': 'm2o country.id',
     },
     'index': ['name'],
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


cfg = {
    'db-uri': 'sqlite://test.db',
    'definitions': definitions,
}

countries = [['Belgium'], ['France']]
teams = [
    ['Blue', 'Belgium'],
    ['Red', 'Belgium'],
    ['Green', 'France'],
]
members = [
    ['Bob', 'Blue', '001', 'Belgium'],
    ['Alice', 'Blue', '002', 'Belgium'],
    ['Trudy', 'Green', '003', 'France'],
]


with connect(cfg):
    create_tables()

    # Add countries
    view = View('country')
    view.write(countries)
    res = view.read()
    print list(res)

    # Add teams
    view = View('team', ['name', 'country.name'])
    view.write(teams)
    res = view.read()
    print list(res)

    # Show team and country ids
    view = View('team', ['id', 'name', 'country.id'])
    res = view.read()
    print list(res)


    # Add members
    view = View('member', [
        ('Name', 'name'),
        ('Team', 'team.name'),
        ('Code', 'registration_code'),
    ])
    view.write(members)
    res = view.read_df()
    print res

