from datetime import datetime, date

from tanker import View, Expression, ctx, Expression
from .base_test import session, members


def test_filters(session):
    view = View('team', ['name'])
    filters = '(= country.name "France")'
    res = view.read(filters).all()
    assert res == [('Blue',)]

    filters = [
        '(= country.name "France")',
        '(= country.name "Belgium")',
    ]
    res = view.read(filters).all()
    assert res == []

    fltr = '(0)' if ctx.flavor == 'sqlite' else '(false)'
    res = view.read(fltr).all()
    assert res == []

    fltr = '(1)' if ctx.flavor == 'sqlite' else '(true)'
    res = view.read(fltr).all()
    assert len(res) == 3


def test_no_fields(session):
    view = View('team')
    res = view.read().all()
    expected = [('Blue', 'Belgium'), ('Blue', 'France'), ('Red', 'Belgium')]
    assert sorted(res) == expected


def test_o2m(session):
    view = View('country', ['name', 'teams.name'])
    res = view.read().all()
    expected = [('Belgium', 'Blue'), ('Belgium', 'Red'),
                ('France', 'Blue'), ('Holland', None)]
    assert res == expected


def test_args(session):
    # Add config value, to use it later
    ctx.cfg['cfg_team'] = 'Red'
    view = View('team', ['name'])

    # Simple test
    cond = '(= name {name})'
    rows = view.read(cond).args(name='Blue')
    assert sorted(rows) == [('Blue',), ('Blue',)]

    # Simple test explicit position
    cond = '(= name {0})'
    rows = view.read(cond).args('Red')
    assert sorted(rows) == [('Red',)]
    cond = '(or (= name {0}) (= name {1}))'
    args = ['Red', 'Blue']
    rows = view.read(cond, args=args)
    assert sorted(rows) == [('Blue',), ('Blue',), ('Red',)]
    # test params are unafected
    assert args == ['Red', 'Blue']

    # Simple test, implicit position
    cond = '(= name {})'
    rows = view.read(cond).args('Red')
    assert sorted(rows) == [('Red',)]
    cond = '(or (= name {}) (= name {}))'
    args = ['Red', 'Blue']
    rows = view.read(cond, args=args)
    # test output
    assert sorted(rows) == [('Blue',), ('Blue',), ('Red',)]
    # test params are unafected
    assert args == ['Red', 'Blue']

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

    # Provide direction
    res = view.read(limit=1, order=('name', 'DESC')).all()
    assert res == [('Holland',)]

    # Sort on several columns
    res = view.read(limit=1, order=['name', 'name']).all()
    assert res == [('Belgium',)]

    # Sort on expression
    res = view.read(limit=1, order=['(!= name "Belgium")']).all()
    assert res == [('Belgium',)]


def test_aliases(session):
    # Add alias
    now = datetime.now()
    ctx.aliases.update({
        'now': now
    })

    view = View('country', ['name', '{now}'])
    res = view.read().all()
    if ctx.flavor == 'sqlite':
        ok = lambda r: r[1] == str(now)
    else:
        ok = lambda r: r[1] == now
    assert all(ok for r in res)

    ctx.aliases.update({
        'type': 'TYPE'
    })
    view = View('country', ['name', '{type}'])
    filters = '(= name "France")'
    res = view.read(filters).all()
    assert res == [('France', 'TYPE')]


def test_field_eval(session):
    view = View('country', ['(= name "Belgium")'])
    res = view.read(order='name').all()
    assert res == [(True,), (False,), (False,),]


def test_aggregation(session):
    # Count
    view = View('country', ['(count)'])
    res = view.read().all()
    assert res == [(3,)]

    # Sum
    view = View('country', ['(sum 1)'])
    res = view.read().all()
    assert res == [(3,)]

    # Min
    view = View('country', ['(min 1)'])
    res = view.read().all()
    assert res == [(1,)]

    # Max
    view = View('country', ['(max 1)'])
    res = view.read().all()
    assert res == [(1,)]

    # Aggregates on expression
    view = View('country', ['(max (+ 1 1))'])
    res = view.read().all()
    assert res == [(2,)]

    # Aggregates & grouping
    view = View('team', ['name', '(count)'])
    res = view.read(groupby='name').all()
    assert res == [('Blue', 2), ('Red', 1)]

    # Aggregates all fields
    view = View('team', ['(max name)', '(count)'])
    res = view.read().all()
    assert res == [('Red', 3)]

    # Aggregates on fk
    view = View('team', ['(max name)'])
    res = view.read(groupby='country.name', order='country.name').all()
    assert res == [('Red',), ('Blue',)]

    # Aggregates & auto-grouping
    view = View('team', ['name', '(count)'])
    res = view.read().all()
    assert res == [('Blue', 2), ('Red', 1)]

    # Group on expression
    view = View('team', {
        'cnt': '(count)',
        'country_match': '(in country 1 2)',
    })

    for c, _ in view.read(groupby='country_match'):
        assert c == 3

    for c, _ in view.read(groupby='(in country 1 2)'):
        assert c == 3

    # Group on several fields
    view = View('team', '(count)')
    res = view.read(groupby=['name', 'country']).all()
    for c, in res:
        assert c == 1


def test_m2o(session):
    pass # TODO


def test_cast(session):
    # Test int -> char conversion
    view = View('country', ['(cast id (varchar))'])
    for i, in view.read():
        assert isinstance(i, str)

    # Test int -> float conversion
    view = View('country', ['(cast id (float))'])
    for i, in view.read():
        assert isinstance(i, float)

    # created_at in member is a timestamp
    View('member', [
        'name',
        'team.country.name',
        'team.name',
        'registration_code']).write(members)

    view = View('member', ['(cast "1" (integer))'])
    for x, in view.read():
        assert isinstance(x, int)

    # (Sqlite doesn't know other conversions and fallback to numeric)
    if ctx.flavor == 'sqlite':
        return

    # Test int -> bool conversion
    view = View('country', ['(cast id (bool))'])
    for i, in view.read():
        assert isinstance(i, bool)

    # Test timestamp -> date conversion
    view = View('member', ['(cast created_at (date))'])
    for x, in view.read():
        assert isinstance(x, date)

    # Test str -> timestamp conversion
    view = View('member', ['(cast "1970-01-01" (timestamp))'])
    for x, in view.read():
        assert isinstance(x, datetime)


def test_like_ilike(session):
    view = View('country', ['name'])
    fltr = '(like name "%e%")'
    res = view.read(fltr).all()
    assert res == [('Belgium',), ('France',)]

    fltr = '(ilike name "H%")'
    res = view.read(fltr).all()
    assert res == [('Holland',)]

    fltr = '(ilike name {prefix})'
    res = view.read(fltr, args={'prefix': 'H%'}).all()
    assert res == [('Holland',)]


def test_array(session):
    data = {
        'int': [(1, [1, 2])],
        'float': [(1, [1.0, 2.0])],
        'bool': [(1, [True, False])],
        # TODO add timestamp & date
    }
    for kind in data:
        print(kind)
        datum = data[kind]
        view = View('kitchensink', ['index', '%s_array' % kind])
        view.write(datum)
        res = view.read().all()
        assert res == datum

    if ctx.flavor == 'sqlite':
        return

    # postgres-specific operations
    flrt = '(= 1 (any int_array))'
    res = view.read(flrt).all()
    assert len(res) == 1

    flrt = '(!= 3 (all int_array))'
    res = view.read(flrt).all()
    assert len(res) == 1

    res = View('kitchensink', ['index', '(unnest int_array)']).read().all()
    assert len(res) == 2


def test_jsonb(session):
    data = [(1, {'ham': 'spam'})]
    view = View('kitchensink', ['index', 'jsonb'])
    view.write(data)

    res = view.read().all()
    assert res[0][1]['ham'] == 'spam'

    if ctx.flavor == 'sqlite':
        return
    # postgres-specific operator
    flrt = '(= "spam" (->> jsonb "ham"))'
    res = view.read(flrt).all()
    assert len(res) == 1
    assert res[0][1]['ham'] == 'spam'

def test_distinct(session):
    view = View('team', ['country.name'])
    expected = sorted(set(view.read().all()))
    res = sorted(view.read(distinct=True).all())
    assert res == expected
