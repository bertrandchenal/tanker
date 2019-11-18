from tanker import View, ctx
from .base_test import session, members

def test_delete_data(session):
    # Not sure why sqlite fail on this one
    if ctx.flavor == 'sqlite':
        return

    view = View('country', ['name'])
    view.delete(data=[['France']])
    res = view.read().all()
    assert res == [('Belgium',), ('Holland',)]


def test_delete_data_extra_col(session):
    full_view = View('member', [
        'name',
        'team.country.name',
        'team.name',
        'registration_code'])
    full_view.write(members)
    assert len(full_view.read().all()) == len(members)

    full_view.delete(data=members)

    res = full_view.read().all()
    assert res == []


def test_delete_data_id(session):
    # Not sure why sqlite fail on this one
    if ctx.flavor == 'sqlite':
        return

    view = View('country', ['id'])

    data = view.read('(!= name "Belgium")').all()
    view.delete(data=[[i] for i, in data ])

    res = view.read().all()
    assert len(res) == 1


def test_delete_filter(session):
    # Not sure why sqlite fail on this one
    if ctx.flavor == 'sqlite':
        return

    # Use a list of filters
    view = View('country', ['name'])
    view.delete(['(> id 0 )' , '(< id 0)'])
    res = view.read(order='name').all()
    assert res == [('Belgium',), ('France',), ('Holland',)]

    # Filter with args
    view = View('country', ['name'])
    view.delete('(in name {names})',
                args={'names': ['France', 'Holland']})

    res = view.read().all()
    assert res == [('Belgium',)]


def test_delete_filter_dict(session):
    # Not sure why sqlite fail on this one
    if ctx.flavor == 'sqlite':
        return
    view = View('country', ['name'])
    view.delete(filters={'name': 'France'})

    res = view.read().all()
    assert res == [('Belgium',), ('Holland',)]


def test_delete_by_id(session):
    # Not sure why sqlite fail on this one
    if ctx.flavor == 'sqlite':
        return
    view = View('country', ['id'])
    data = view.read('(= name "France")').all()
    view.delete(data=data)

    res = View('country', ['name']).read().all()
    assert res == [('Belgium',), ('Holland',)]
