import pytest
from tanker import Table, View, Column, Expression, ctx
from .base_test import session


def test_bitwise_operators(session):
    exp = Expression(View('member'))
    ops =  ('<', '>', '<=', '>=', '!=', 'like', 'ilike', 'is', 'isnot')
    for op in ops:
        ast = exp.parse('(%s name "foo")' % op)
        res = ast.eval()

        if op == 'isnot':
            op = 'is not'
        assert res == '"member"."name" %s %%s' % op
        assert ast.params == ['foo']


def test_cast(session):
    exp = Expression(View('member'))
    ast = exp.parse('(cast id (varchar))')
    res = ast.eval()
    assert res == 'CAST ("member"."id" AS varchar)'
    assert ast.params == []


def test_other_operators(session):
    exp = Expression(View('member'))
    ops = {
        'and': 'AND',
        'or': 'OR',
        '+': '+',
        '-': '-',
        '/': '/',
        '*': '*',
    }
    for op in ops:
        ast = exp.parse('(%s 1 2)' % op)
        res = ast.eval()
        assert res == '(%%s %s %%s)' % ops[op]
        assert ast.params == [1, 2]

        ast = exp.parse('(%s 1 2 3)' % op)
        res = ast.eval()
        sep = ' %s ' % ops[op]
        assert res == '(%s)' % sep.join(['%s']*3)
        assert ast.params == [1, 2, 3]


def test_in_notin(session):
    exp = Expression(View('member'))
    for op in ('in', 'notin'):
        res = exp.parse('(%s name 1 2)' % op).eval()


def test_not(session):
    exp = Expression(View('member'))
    res = exp.parse('(not (= name 1))').eval()
    assert res == 'not "member"."name" = %s'


def test_select(session):
    exp = Expression(View('member'))
    ast = exp.parse('(select 1)')
    res = ast.eval()
    assert res == 'SELECT %s'
    assert ast.params == [1]


def test_from(session):
    exp = Expression(View('team'))
    ast = exp.parse('(FROM member (SELECT id name _parent.name))')
    res = ast.eval()
    assert res == 'SELECT "member"."id", "member"."name", "team"."name" FROM "member"'
    assert ast.params == []


def test_join(session):
    exp = Expression(View('member'))
    ast = exp.parse('(= team.name "spam-team")')
    assert ast.eval() == '"team_0"."name" = %s'
    assert ast.params == ['spam-team']

    ast = exp.parse('(= team.country.name "BE")')
    assert ast.eval() == '"country_1"."name" = %s'
    assert ast.params == ['BE']

    ast = exp.parse('(and (= team.country.name "BE") '
                   '(= team.country.name "BE"))')
    assert ast.eval() == '("country_1"."name" = %s AND "country_1"."name" = %s)'
    assert ast.params == ['BE', 'BE']


def test_exists(session):
    exp = Expression(View('team'))
    ast = exp.parse('(exists 1)')
    assert ast.eval() == 'EXISTS (%s)'
    assert ast.params == [1]

    ast = exp.parse('(and '
                    '(exists ('
                     'from member (select 1) '
                      '(where (= team _parent.id)))) '
                    '(= name "spam-team")'
                    '(= members.name "Bob")'
                   ')')
    assert ast.eval() == (
        '(EXISTS (SELECT %s FROM "member" WHERE "member"."team" = "team"."id") '
        'AND "team"."name" = %s AND "member_0"."name" = %s)')
    assert ast.params == [1, 'spam-team', 'Bob']


def test_multi_parent(session):
    if ctx.pg_schema:
        return
    exp = Expression(View('country'))
    ast = exp.parse('''
     (from team (select country) (where (in id
       (from member (select team) (where (= team _parent.id)
                                        (= name _parent._parent.name)
       )
     ))))''')
    assert ast.eval() == (
        'SELECT "team"."country" FROM "team" WHERE "team"."id" in ('
          'SELECT "member"."team" FROM "member" '
          'WHERE "member"."team" = "team"."id" '
            'AND "member"."name" = "country"."name"'
          ')')
    assert ast.params == []


def test_subexpression_join(session):
    if ctx.pg_schema:
        return
    exp = Expression(View('team'))
    ast = exp.parse('(exists 1)')
    assert ast.eval() == 'EXISTS (%s)'
    assert ast.params == [1]

    ast = exp.parse('(and '
                    '(exists ('
                     'from member (select 1) '
                      '(where (= team _parent.id) (= team.country.name "BE")))) '
                    '(= country.name "BE")'
                   ')')

    assert ast.eval() == (
        '(EXISTS (SELECT %s FROM "member" '
        'LEFT JOIN "team" AS "team_0" ON ("member"."team" = "team_0"."id") '
        'LEFT JOIN "country" AS "country_1" '
          'ON ("team_0"."country" = "country_1"."id") '
        'WHERE "member"."team" = "team"."id" AND "country_1"."name" = %s) '
        'AND "country_2"."name" = %s)')
    assert ast.params == [1, 'BE', 'BE']


def test_subselect(session):
    if ctx.pg_schema:
        return
    view = View('team')
    cond = (
        '(in id '
          '(from member (select team) '
          '(where (= name "Bob"))))'
        )
    ast = Expression(view).parse(cond)
    expected = ('"team"."id" in ('
                'SELECT "member"."team" FROM "member" WHERE "member"."name" = %s)')
    assert ast.eval() == expected


def test_field(session):
    exp = Expression(View('team'))
    assert exp.parse('name').eval() == '"team"."name"'
    assert exp.parse('country.name').eval() == '"country_0"."name"'
    assert exp.parse('members.team.name').eval() == '"team_2"."name"'
    assert exp.parse('members.name').eval() == '"member_1"."name"'


def test_env(session):
    view = View('member', {
        'created_date': '(cast created_at (date))',
    })
    exp = Expression(view)
    expected = 'CAST ("member"."created_at" AS date)'
    assert exp.parse('(cast created_at (date))').eval() == expected
    assert exp.parse('created_date').eval() == expected


def test_table_alias(session):
    if ctx.pg_schema:
        return
    exp = Expression(View('team'), table_alias='tmp')
    ast = exp.parse('name')
    assert ast.eval() == '"tmp"."name"'

    exp = Expression(View('team'), table_alias='tmp')
    ast = exp.parse('(= country.name "foo")')
    join = next(exp.ref_set.get_sql_joins())
    expected = ('LEFT JOIN "country" AS "country_0" '
                'ON ("tmp"."country" = "country_0"."id")')
    assert join == expected

    # This kind of expression will probably actually fail later (in
    # the default use case, the id column is not created in tmp)
    exp = Expression(View('team'), table_alias='tmp')
    ast = exp.parse('(= members.name "foo")')
    join = next(exp.ref_set.get_sql_joins())
    expected = ('LEFT JOIN "member" AS "member_0" '
                'ON ("tmp"."id" = "member_0"."team")')
    assert join == expected
