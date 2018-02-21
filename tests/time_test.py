from datetime import datetime, date
import pytz

from pandas import date_range, DataFrame

from tanker import View, ctx, PY2
from .base_test import session

BRU = pytz.timezone('Europe/Brussels')
date_fmt = '%Y-%m-%d'
ts_formats = [
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%dT%H:%M:%S',
]
ts_tz_formats = [
    '%Y-%m-%d %H:%M:%S%z',
    '%Y-%m-%dT%H:%M:%S%z',
]
record = {
    'timestamp': datetime(2018, 1, 1, 0, 0, 0),
    'timestamptz': BRU.localize(datetime(2018, 1, 2, 0, 0, 0)),
    'date': date(2018, 1, 3),
}


def test_time_basic(session):
    view = View('timeseries')
    for ts_fmt, ts_tz_fmt in zip(ts_formats, ts_tz_formats):
        view.write([record])

        keys = list(record)
        if ctx.flavor == 'sqlite':
            # sqlite dbapi does not adapt tz
            keys.remove('timestamptz')
        for col in keys:
            value, = View('timeseries', [col]).read().one()
            assert value == record[col]


def test_time_formats(session):
    view = View('timeseries')
    for ts_fmt, ts_tz_fmt in zip(ts_formats, ts_tz_formats):
        fmt_record = {
            'timestamp': record['timestamp'].strftime(ts_fmt),
            'timestamptz': record['timestamptz'].strftime(ts_tz_fmt),
            'date': record['date'].strftime(date_fmt),
        }
        if PY2 or ctx.flavor == 'sqlite':
            # strftime doesn't know %s in py2
            fmt_record.pop('timestamptz')
        view.write([fmt_record])

        for col in fmt_record.keys():
            value, = View('timeseries', [col]).read().one()
            assert value == record[col]

def test_time_pandas(session):
    timestamp = date_range(
        '2018-01-01 00:00:00',
        '2018-01-05 00:00:00',
        freq='D')
    timestamptz = date_range(
        '2018-01-01 00:00:00+01',
        '2018-01-05 00:00:00+01',
        freq='D')
    df = DataFrame({
        'timestamp': timestamp,
        'timestamptz': timestamptz,
    })

    if ctx.flavor == 'sqlite':
        df = df[['timestamp']]

    view = View('timeseries', list(df.columns))
    view.write(df)

    res = view.read().df()
    for col in df.columns:
        assert (res[col].values == df[col].values).all()
