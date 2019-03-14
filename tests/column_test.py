from itertools import product

import pytest
from .base_test import DB_PARAMS

db_uris = [p['uri'] for p in DB_PARAMS]
COL_TYPES = ['integer', 'timestamp']
PARAMS = product(COL_TYPES, db_uris)

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
