import pytest
from tanker import Table, View, Column, Expression, ctx
from .base_test import session


def test_bitwise_operator(session):
    exp = Expression(View('member'))
    for op in ('<', '>', '<=', '>=', '!=', 'like', 'ilike', 'is', 'isnot'):
        res = exp.eval('(%s name "foo")' % op)

        if op == 'isnot':
            op = 'is not'
        assert res == 'member.name %s %%s' % op
        assert exp.params == ['foo']


def test_and_or(session):
    exp = Expression(View('member'))
    for op in ('and', 'or'):
        res = exp.eval('(%s 1 2)' % op)
        assert res == '(%%s %s %%s)' % op.upper()
        assert exp.params == [1, 2]

        res = exp.eval('(%s 1 2 3)' % op)
        sep = ' %s ' % op.upper()
        assert res == '(%s)' % sep.join(['%s']*3)
        assert exp.params == [1, 2, 3]

def test_in_notin(session):
    exp = Expression(View('member'))
    for op in ('in', 'notin'):
        res = exp.eval('(%s name 1 2)' % op)

def test_not(session):
    exp = Expression(View('member'))
    res = exp.eval('(not (= name 1))')
    assert res == 'not member.name = %s'

def test_select(session):
    exp = Expression(View('member'))
    res = exp.eval('(select 1)')
    assert res == 'SELECT %s FROM member'
    assert exp.params == [1]

def test_from(session):
    exp = Expression(View('team'))
    res = exp.eval('(FROM member (SELECT id name _parent.name))')
    assert res == 'SELECT member.id, member.name, team.name FROM member'
    assert exp.params == []

def test_join(session):
    exp = Expression(View('member'))
    res = exp.eval('(= team.name "spam-team")')
    assert res == 'team_0.name = %s'
    assert exp.params == ['spam-team']

    res = exp.eval('(= team.country.name "BE")')
    assert res == 'country_1.name = %s'
    assert exp.params == ['BE']

    res = exp.eval('(and (= team.country.name "BE") '
                   '(= team.country.name "BE"))')
    assert res == '(country_1.name = %s AND country_1.name = %s)'
    assert exp.params == ['BE', 'BE']


def test_exists(session):
    exp = Expression(View('team'))
    res = exp.eval('(exists 1)')
    assert res == 'EXISTS (%s)'
    assert exp.params == [1]

    res = exp.eval('(and '
                    '(exists ('
                     'from member (select 1) '
                      '(where (= team _parent.id)))) '
                    '(= name "spam-team")'
                    '(= members.name "Bob")'
                   ')')
    assert res == (
        '(EXISTS (SELECT %s FROM member WHERE member.team = team.id) '
        'AND team.name = %s AND member_0.name = %s)')
    assert exp.params == [1, 'spam-team', 'Bob']


def test_multi_parent(session):
    exp = Expression(View('country'))
    res = exp.eval('''(from country (select id) (where (in id
     (from team (select country) (where (in id
       (from member (select team) (where (= team _parent.id)
                                        (= name _parent._parent.name)
       )
     ))))
    )))''')
    assert res == (
        'SELECT country.id FROM country '
        'WHERE country.id in ('
          'SELECT team.country FROM team WHERE team.id in ('
            'SELECT member.team FROM member '
            'WHERE member.team = team.id AND member.name = country.name'
          ')'
        ')')
    assert exp.params == []

def test_subexpression_join(session):
    exp = Expression(View('team'))
    res = exp.eval('(exists 1)')
    assert res == 'EXISTS (%s)'
    assert exp.params == [1]

    res = exp.eval('(and '
                    '(exists ('
                     'from member (select 1) '
                      '(where (= team _parent.id) (= team.country.name "BE")))) '
                    '(= country.name "BE")'
                   ')')

    assert res == (
        '(EXISTS (SELECT %s FROM member '
        'LEFT JOIN team AS team_0 ON (member.team = team_0.id) '
        'LEFT JOIN country AS country_1 ON (team_0.country = country_1.id) '
        'WHERE member.team = team.id AND country_1.name = %s) '
        'AND country_2.name = %s)')
    assert exp.params == [1, 'BE', 'BE']
