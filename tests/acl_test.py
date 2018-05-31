from tanker import View, ctx, execute
from .base_test import session


def inject(table, rules):
    # Inject acl rules on the live context, this simplify tests
    ctx.cfg['acl_rules'] = {table: rules}

def reset(table):
    # Inject acl rules on the live context, this simplify tests
    ctx.cfg['acl_rules'] = {table: []}

def test_read(session):
    inject('country', ['(= name "Belgium")'])

    # Test that main table is filtered
    res = View('country', ['name']).read().all()
    assert res == [('Belgium',)]

    # # Test that acl is enforced on relations
    # res = View('team', ['country.name']).read().all()
    # assert res == [('Belgium',), ('Belgium',)]

def test_write(session):
    inject('member', ['(= registration_code "001")'])

    # Test that main table is filtered
    view = View('member', ['registration_code', 'name'])
    res = view.write([
        ('001', 'UPDATED'),
        ('002', 'UPDATED'),
     ])

    cur = execute('SELECT registration_code, name FROM member')
    res = list(cur)
    assert res == [('001', 'UPDATED')]
