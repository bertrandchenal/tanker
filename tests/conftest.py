import pytest
import psycopg2

from tanker import Pool


@pytest.yield_fixture(scope='session', autouse=True)
def _db(request):
    conn = psycopg2.connect(dbname='postgres')
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('CREATE DATABASE tanker_test')
    yield
    Pool.disconnect()
    cursor.execute('DROP DATABASE tanker_test')
