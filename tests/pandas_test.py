from pandas import DataFrame, date_range
from numpy import arange, asarray
from tanker import View, Expression, ctx
from .base_test import session


def test_read_write(session):
    view = View('country', {'Name': 'name'})
    db_df = view.read().df()

    new_df = DataFrame({'Name': ['Italy']})
    view.write(new_df)

    updated_df = view.read().df()
    expected = db_df.append(new_df).reset_index(drop=True)
    assert all(expected == updated_df)


def test_empty_read(session):
    view = View('country')
    df = view.read({'name': 'Prussia'}).df()
    assert len(df) == 0

def test_kitchensink(session):
    df = DataFrame({
        'index': arange(10),
        'bigint': arange(10),
        'float': arange(10).astype('f8'),
        'true': asarray([True] * 10),
        'false': asarray([False] * 10),
        'varchar': ['spam'] * 10,
        'timestamp': asarray(range(10), dtype="M8[s]"),
        'date': date_range('1970-01-01', '1970-01-10', freq='D')
    })
    cols = list(df.columns)
    view = View('kitchensink', cols)
    view.write(df)

    read_df = view.read().df()
    for col in cols:
        assert all(read_df[col] == df[col])
