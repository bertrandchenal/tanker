from itertools import product

import pytest
from tanker import View, Expression
from .base_test import DB_URIS


COL_TYPES = ['integer', 'timestamp']
PARAMS = product(COL_TYPES, DB_URIS)

@pytest.yield_fixture(scope='function', params=PARAMS)
def session(request):
    col_type, db_uri = request.params
    schema = [{
        'table': 'test',
        'columns': {
            'col': col_type,
        }
    }]
    cfg = {
        'db_uri': db_uri,
        'schema': schema,
    }

    with connect(cfg):
        yield
