from tanker import create_tables
from .base_test import session, members

def test_create_tables(session):
    # Call create_tables a second time, this should be harmless
    create_tables()
