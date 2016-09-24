
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


def test_args(session):
    ctx.cfg['cfg_team'] = 'Red'
    view = View('team', ['name'])

    # simple test
    cond = '(= name {name})'
    rows = view.read(cond).args(name='Blue')
    assert sorted(rows) == [('Blue',)]

    # Mix value from config
    cond = '(in name {cfg_team} {name})'
    rows = view.read(cond).args(name='Blue')
    assert sorted(rows) == [('Blue',), ('Red',)]

    # Test with a list in args
    cond = '(in name {names})'
    rows = view.read(cond).args(names=['Red', 'Blue'])
    assert sorted(rows) == [('Blue',), ('Red',)]
