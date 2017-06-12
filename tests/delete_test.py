from tanker import View
from .base_test import session

def test_delete_data(session):
    view = View('country', ['name'])
    view.delete(data=[['France']])

    res = view.read().all()
    assert res == [('Belgium',), ('Holland',)]

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
