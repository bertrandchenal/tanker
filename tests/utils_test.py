from tanker import paginate, LRU_SIZE, View

from .base_test import session


def test_paginate(session):
    values = [
        iter([1]*10),
        iter([2]*10),
        iter([3]*10),
    ]

    cnt = 0
    for page in paginate(values, 2):
        cnt += 1
        assert len(page) == 2
        for line in page:
            assert line == (1, 2, 3)
    assert cnt == 5


def test_lru(session):
    values = [('%s' % i, ) for i in range(LRU_SIZE * 2)]
    country_view = View('country', ['name'])
    team_view = View('team', ['name', 'country.name'])


    # Fill country table, clean team table
    country_view.write(values)
    team_view.delete()

    # Fill team table to trigger lru on country fk
    values = [('%s' % i, '%s' % i,) for i in range(LRU_SIZE * 2)]
    team_view.write(values)

    teams = team_view.read().all()
    assert len(teams) == LRU_SIZE * 2
    for team_name, country_name in teams:
        assert team_name == country_name
