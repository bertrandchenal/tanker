from tanker import View

from .base_test import session, check, members

def test_no_insert(session):
    team_view = View('team', ['name', 'country.name'])
    team_view.write([
        ('Orange', 'Holland'), # This is an insert
        ('Blue', 'Belgium'),
    ], insert=False)


    expected = [('Red', 'Belgium',),
                ('Blue', 'Belgium',),
                ('Blue', 'France',)]
    res = team_view.read()
    check(expected, res)


def test_no_update(session):
    team_view = View('team', ['name', 'country.name'])
    team_view.write([
        ('Orange', 'Holland'),
        ('Blue', 'Belgium'), # This is an update of Blue team
    ], update=False)

    expected = [('Red', 'Belgium',),
                ('Blue', 'Belgium',),
                ('Blue', 'France',),
                ('Orange', 'Holland',)]
    res = team_view.read()
    check(expected, res)


def test_no_fields(session):
    # No fields are provided, should fallback to table definition
    team_view = View('country')
    team_view.write([
        ('Italy',),
    ])

    expected = [('Belgium',),
                ('Italy',),
                ('France',),
                ('Holland',)]
    res = team_view.read()
    check(expected, res)


def test_purge(session):
    team_view = View('team', ['name', 'country.name'])
    team_view.write([
        ('Orange', 'Holland'), # this is an insert
        ('Blue', 'France'),    # belgium is missing
    ], purge=True, insert=False, update=False)

    expected = [('Blue', 'France',)]
    res = team_view.read()
    check(expected, res)


def test_partial_write(session):
    '''
    We want to update only some columns
    '''

    # member table is empty by default
    full_view = View('member', [
        'name',
        'team.country.name',
        'team.name',
        'registration_code'])
    full_view.write(members)

    # Collect ids and name
    name_view = View('member', ['id', 'name'])
    id2name = dict(name_view.read().all())

    partial_view = View('member', ['name', 'registration_code'])
    partial_view.write([['Bob', '001']])

    # Makes sur no other column is set to null
    res = full_view.read('(= name "Bob")').one()
    assert all(res)

    # compare ids
    for member_id, name in name_view.read():
        assert id2name[member_id] == name


def test_write_by_id(session):
    country_view = View('country', ['id', 'name'])
    res = country_view.read('(= name "Belgium")').one()
    record_id = res[0]
    res = country_view.write([(record_id, 'BELGIUM')])

    res = country_view.read('(= name "Belgium")').one()
    assert res is None

    res = country_view.read('(= name "BELGIUM")').one()
    assert res[0] == record_id


def test_nullable_fk(session):
    '''
    If we pass None value in m2o field(s),
    we should put null in the fk col
    '''
    member_view = View('member', [
        'registration_code',
        'team.name',
        'team.country.name',
    ])
    res = member_view.write([('test', None, None)])

    member_view = View('member', ['team'])
    res = member_view.read('(= registration_code "test")').one()
    assert res == (None,)


def test_purge_filters(session):
    teams = [
        ['Red', 'Belgium'],
    ] # Blue-Belgium is missing

    fltr = '(= country.name "Belgium")'  # We restrict writes to belgium
    team_view = View('team', ['name', 'country.name'])
    team_view.write(teams, purge=True, filters=fltr)

    expected = [('Red', 'Belgium',),
                ('Blue', 'France',)]
    res = team_view.read()
    check(expected, res)

    # Opposite filter
    fltr = '(!= country.name "Belgium")'  # We don't purge belgium
    team_view.write(teams, purge=True, filters=fltr)
    expected = [('Red', 'Belgium',)]
    res = team_view.read()
    check(expected, res)

def test_update_filters(session):
    # init members
    full_view = View('member', [
        'name',
        'team.country.name',
        'team.name',
        'registration_code'])
    full_view.write(members)

    # Let's update some names (the index is registration_code)
    fltr = '(= registration_code "001")'
    member_view = View('member', ['registration_code', 'name'])
    data = [
        ('001', 'BOB'),
        ('003', 'TRUDY'),
    ]
    member_view.write(data, filters=fltr)
    expected = [
        ('001', 'BOB', ),
        ('002', 'Alice'),
        ('003', 'Trudy'),
    ]
    res = member_view.read()
    check(expected, res)



def test_insert_filters(session):
    # init members
    full_view = View('member', [
        'name',
        'team.country.name',
        'team.name',
        'registration_code'])
    full_view.write(members)

    # Let's insert some names (the index is registration_code)
    fltr = '(= registration_code "004")'
    member_view = View('member', ['registration_code', 'name'])
    data = [
        ('004', 'Carol'),
        ('005', 'Dan'),
    ]
    member_view.write(data, filters=fltr)
    expected = [
        ('001', 'Bob', ),
        ('002', 'Alice'),
        ('003', 'Trudy'),
        ('004', 'Carol'),
    ]
    res = member_view.read()
    check(expected, res)
