from datetime import datetime

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
    # Add config value, to use it later
    ctx.cfg['cfg_team'] = 'Red'
    view = View('team', ['name'])

    # simple test
    cond = '(= name {name})'
    rows = view.read(cond).args(name='Blue')
    assert sorted(rows) == [('Blue',), ('Blue',)]

    # simple test, anonymous
    cond = '(= name {})'
    rows = view.read(cond).args('Red')
    assert sorted(rows) == [('Red',)]

    # Mix value from config
    cond = '(in name {cfg_team} {name})'
    rows = view.read(cond).args(name='Blue')
    assert sorted(rows) == [('Blue',), ('Blue',), ('Red',)]

    # Test with a list in args
    cond = '(in name {names})'
    rows = view.read(cond).args(names=['Red', 'Blue'])
    assert sorted(rows) == [('Blue',), ('Blue',), ('Red',)]

    # Test with an object
    cond = '(in name {obj.name})'
    class Obj:
        pass
    obj = Obj()
    obj.name = 'Blue'
    rows = view.read(cond).args(obj=obj)
    assert sorted(rows) == [('Blue',), ('Blue',)]

    # Test with an dict
    cond = '(in name {data.name})'
    data = {'name': 'Red'}
    rows = view.read(cond).args(data=data)
    assert sorted(rows) == [('Red',)]

def test_limit_order(session):
    view = View('country', ['name'])
    res = view.read(limit=1, order='name').all()
    assert res == [('Belgium',)]

    res = view.read(limit=1, order=('name', 'DESC')).all()
    assert res == [('Holland',)]

def test_aliases(session):
    # Add alias
    now = datetime.now()
    ctx.aliases.update({
        'now': now
    })

    view = View('country', ['name', 'now'])
    expected = view.read().all()
    if ctx.flavor == 'sqlite':
        ok = lambda r: r[1] == str(now)
    else:
        ok = lambda r: r[1] == now
    assert all(ok for r in expected)
