from tanker import View
from .base_test import session, members

def test_delete_data(session):
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

def test_delete_filter(session):
    view = View('country', ['name'])
    view.delete('(in name {names})',
                args={'names': ['France', 'Holland']})

    res = view.read().all()
    assert res == [('Belgium',)]

def test_delete_filter_dict(session):
    view = View('country', ['name'])
    view.delete(filters={'name': 'France'})

    res = view.read().all()
    assert res == [('Belgium',), ('Holland',)]

def test_delete_by_id(session):
    view = View('country', ['id'])
    data = view.read('(= name "France")').all()
    view.delete(data=data)

    res = View('country', ['name']).read().all()
    assert res == [('Belgium',), ('Holland',)]
