from tanker import View, Expression, ctx
from .base_test import session, check

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
