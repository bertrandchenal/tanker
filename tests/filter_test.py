
from tanker import View, Expression
from base_test import session


def test_subselect(session):
    view = View('team')
    cond = (
        '(in id '
          '(from member (select team) '
          '(where (= name "Bob"))))'
        )
    res = Expression(view).eval(cond)
    expected = ('team.id in ('
                'SELECT member.team FROM member WHERE member.name = %s)')
    assert res == expected
