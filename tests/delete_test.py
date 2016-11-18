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

def test_delete_filter_by(session):
    view = View('country', ['name'])
    view.delete(filter_by={'name': 'France'})

    res = view.read().all()
    assert res == [('Belgium',), ('Holland',)]
