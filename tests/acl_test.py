from tanker import View, ctx
from .base_test import session, members

member_cols = [
    'name',
    'team.country.name',
    'team.name',
    'registration_code',
]


def inject(table, kind, rules):
    # Inject acl rules on the live context, this simplify tests
    assert kind in ('acl-read', 'acl-write')
    ctx.cfg[kind] = {table: rules}

def reset(table):
    # Remove all acl rules
    ctx.cfg['acl-read'] = {table: []}
    ctx.cfg['acl-write'] = {table: []}

def test_read(session):
    inject('country', 'acl-read', ['(= name "Belgium")'])

    # Test that main table is filtered
    res = View('country', ['name']).read().all()
    assert res == [('Belgium',)]

    # Test with a relation
    inject('team', 'acl-read', ['(= country.name "Belgium")'])
    res = View('team', ['name']).read().all()
    assert res == [('Blue',), ('Red',)]


    # # Test that acl is implictly enforced on relations
    # res = View('team', ['country.name']).read().all()
    # assert res == [('Belgium',), ('Belgium',)]

def test_insert(session):
    inject('member', 'acl-write', ['(= registration_code "001")'])
    # Test that main table is filtered on insertion
    view = View('member', ['registration_code', 'name'])
    cnt = view.write([
        ('001', 'Bob'),
        ('002', 'Alice'),
     ])
    assert cnt['filtered'] == 1
    bob, = view.read().all()
    assert bob == ('001', 'Bob')
    view.delete()

    # Test on insert with filter on relation
    inject('member', 'acl-write', ['(= team.name "Blue")'])
    cnt = View('member', member_cols).write([
    ['Bob', 'Belgium', 'Blue', '001'],
    ['Alice', 'Belgium', 'Red', '002'],
     ])
    assert cnt['filtered'] == 1
    bob, = view.read().all()
    assert bob == ('001', 'Bob')

def test_update_simple_filter(session):
    # Add all members to table
    inject('member', 'acl-write', [])
    view = View('member', ['registration_code', 'name'])
    View('member', member_cols).write(members)

    # Test on update
    inject('member', 'acl-write', ['(= registration_code "001")'])
    # Test that main table is filtered on insertion
    view = View('member', ['registration_code', 'name'])
    cnt = view.write([
        ('001', 'BOB'),
        ('002', 'ALICE'),
     ])
    assert cnt['filtered'] == 1
    res = View('member', ['name']).read().all()
    assert sorted(name for name, in res) == ['Alice', 'BOB', 'Trudy']

def test_update_relation_filter(session):
    # Add all members to table
    inject('member', 'acl-write', [])
    view = View('member', ['registration_code', 'name'])
    View('member', member_cols).write(members)

    # Test update with filter on relation
    inject('member', 'acl-write', ['(= team.name "Blue")'])
    view = View('member', ['registration_code', 'name'])

    view.write([
        ('001', 'BOB'),
        ('002', 'ALICE'),
     ])
    res = view.read('(in registration_code "001" "002")').all()
    assert sorted(res) == [('001', 'BOB'), ('002', 'Alice')]

    # Nasty test, when we change the value of the column supporting the filter
    view = View('member', [
        'registration_code', 'team.name', 'team.country.name'])
    view.write([
        ('001', 'Red', 'Belgium'), # Blue to Red transition
        ('002', 'Blue', 'Belgium'),# Red to Blue transition
     ])

    res = view.read('(in registration_code "001" "002")').all()
    assert sorted(res) == [('001', 'Blue', 'Belgium'),
                           ('002', 'Red', 'Belgium')]
