from tanker import View, Expression, ctx
from .base_test import session, check, members

def test_no_insert(session):
    team_view = View('team', ['name', 'country.name'])
    rowcounts = team_view.write([
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
    rowcounts = team_view.write([
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
    rowcounts = team_view.write([
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
    rowcounts = team_view.write([
        ('Orange', 'Holland'),
        ('Blue', 'France'),
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
    res = full_view.read('(= name "Bob")').next()
    assert all(res)

    # compare ids
    for member_id, name in name_view.read():
        assert id2name[member_id] == name
