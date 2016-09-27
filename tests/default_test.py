from tanker import View, Expression, ctx
from base_test import session, members

def test_timestamp(session):
    view = View('member', [
        'name',
        'team.country.name',
        'team.name',
        'registration_code'])
    view.write(members)

    assert all(View('member', ['created_at']).read())
