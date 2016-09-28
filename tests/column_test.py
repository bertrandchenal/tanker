from itertools import product

import pytest
from tanker import View, Expression
from base_test import get_config, DB_TYPES


COL_TYPES = ['integer', 'timestamp']
PARAMS = product(COL_TYPES, DB_TYPES)

@pytest.yield_fixture(scope='function', params=PARAMS)
def session(request):
    col_type, db_type = request.params
    schema = [{
        'table': 'test',
        'columns': {
            'col': col_type,
        }
    }]
    cfg = get_config(db_type, to_clean['test'], schema=schema)

    with connect(cfg):
        yield
