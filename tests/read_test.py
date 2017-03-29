from datetime import datetime

from tanker import View, Expression, ctx, Expression
from .base_test import session


def test_filters(session):
    view = View('team', ['name'])
    filters = '(= country.name "France")'
    res = view.read(filters).all()
    assert res == [('Blue',)]

def test_subselect(session):
    view = View('team')
    cond = (
        '(in id '
          '(from member (select team) '
          '(where (= name "Bob"))))'
        )
    ast = Expression(view).parse(cond)
    expected = ('team.id in ('
                'SELECT member.team FROM member WHERE member.name = %s)')
    assert ast.eval() == expected


def test_args(session):
    # Add config value, to use it later
    ctx.cfg['cfg_team'] = 'Red'
    view = View('team', ['name'])

    # Simple test
    cond = '(= name {name})'
    rows = view.read(cond).args(name='Blue')
    assert sorted(rows) == [('Blue',), ('Blue',)]

    # Simple test, anonymous
    cond = '(= name {0})'
    rows = view.read(cond).args('Red')
    assert sorted(rows) == [('Red',)]

    # Mix value from config
    cond = '(in name {cfg_team})'
    rows = view.read(cond)
    assert sorted(rows) == [('Red',)]

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

    # Test with a dict
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
    res = view.read().all()
    if ctx.flavor == 'sqlite':
        ok = lambda r: r[1] == str(now)
    else:
        ok = lambda r: r[1] == now
    assert all(ok for r in res)

    ctx.aliases.update({
        'type': 'TYPE'
    })
    view = View('country', ['name', 'type'])
    filters = '(= name "France")'
    res = view.read(filters).all()
    assert res == [('France', 'TYPE')]

def test_field_eval(session):
    view = View('country', ['(= name "Belgium")'])
    res = view.read(order='name').all()
    assert res == [(True,), (False,), (False,),]

def test_aggregation(session):
    view = View('country', ['(count *)'])
    res = view.read().all()
    assert res == [(3,)]

    view = View('team', ['name', '(count *)'])
    res = view.read().all()
    assert res == [('Blue', 2), ('Red', 1)]

    view = View('team', ['(max name)', '(count *)'])
    res = view.read().all()
    assert res == [('Red', 3)]

    view = View('member', ['(max team.name)'])
    res = view.read(groupby='team.country.name').all()
    assert res == [('Red', 2), ('Red', 2)]
