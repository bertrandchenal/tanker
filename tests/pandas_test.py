from pandas import DataFrame
from tanker import View, Expression, ctx
from base_test import session


def test_read_write(session):
    view = View('country', {'Name': 'name'})
    db_df = view.read_df()

    new_df = DataFrame({'Name': ['Italy']})
    view.write(new_df)

    updated_df = view.read_df()
    expected = db_df.append(new_df).reset_index(drop=True)
    assert all(expected == updated_df)

