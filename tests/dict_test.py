from tanker import View, Expression, ctx
from .base_test import session


def test_read_write(session):
    view = View('country', {'Name': 'name'})
    records = list(view.read().dict())

    new_records = [{'Name': 'Italy'}]
    view.write(new_records)

    updated_records = list(view.read().dict())
    expected = records + new_records
    assert expected == updated_records
