from datetime import datetime, date


from tanker import View, ctx
from .base_test import session


def test_reserved_words(session):
    record = {
        'index': 1,
        'true': True,
        'false': False,
        'null': None,
        'integer': 1,
        'bigint': 10000000000,
        'float': 1.0,
        'bool': True,
        'timestamp': datetime(1970, 1, 1),
        'date': date(1970, 1, 1),
        'varchar': 'varchar',
        'int_array': [1,2],
        'bool_array': [True, False],
        'ts_array': [
            [datetime(1970, 1, 1), datetime(1970, 1, 2)],
            [datetime(1970, 1, 3), datetime(1970, 1, 4)],
        ],
        'char_array': [
            [['ham', 'spam'], ['foo', 'bar']],
            [['foo', 'bar'], [None, None]],
        ],
    }


    # Write actual values
    ks_view = View('kitchensink')
    ks_view.write([record])
    res = list(ks_view.read().dict())[0]
    for k, v in record.items():
        if ctx.flavor == 'sqlite' and k.endswith('array'):
            # Array support with sqlite is incomplete
            continue
        assert res[k] == v

    # Filters
    for k, v in record.items():
        if isinstance(v, list):
            continue
        cond = '(is %s {})' if k == 'null' else '(= %s {})'
        res = ks_view.read(cond % k, args=[v]).all()
        assert len(res) == 1

    # Write nulls
    for k in record:
        if k == 'index':
            continue
        record[k] = None
    ks_view.write([record])
    res = list(ks_view.read().dict())[0]
    for k, v in record.items():
        assert res[k] == v

def test_function(session):
    # TODO add support for slite on epoch and floor
    if ctx.flavor == 'sqlite':
        return

    input_record = {
        'floor': 1.1,
        'epoch': datetime(1970, 1, 1),
        'year': datetime(1970, 1, 1),
    }
    output_record = {
        '(floor floor)': 1,
        '(extract (epoch) epoch)': 0,
        '(extract (year) year)': 1970,
        'floor': 1.1,
        '(* floor 2)': 2.2
    }

    ks_view = View('kitchensink')
    ks_view.write([input_record])

    keys, values = zip(*list(output_record.items()))
    res =  View('kitchensink', keys).read().all()
    assert res[0] == values
