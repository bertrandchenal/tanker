import pytest
from tanker import connect, create_tables, Pool
from .base_test import DB_URIS, SCHEMA


@pytest.yield_fixture(scope='function', params=DB_URIS)
def session(request):
    uri = request.param
    cfg = {'db_uri': uri, 'schema': SCHEMA}
    with connect(cfg):
        create_tables()

    # clear pool cache
    Pool.clear()
    with connect({'db_uri': uri}):
        yield uri

def test_db_state(session):
    print session
