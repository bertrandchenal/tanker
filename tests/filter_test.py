
from tanker import View, Expression, ctx
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


def test_params(session):
    ctx.cfg['cfg_team'] = 'Red'
    view = View('team', ['name'])
    cond = '(in name {cfg_team} {name})'
    rows = view.read(cond).args(name='Blue')
    assert sorted(rows) == [('Blue',), ('Red',)]
