from tanker import Table, Expression
from .base_test import session

def test_expand(session):
    # Test formatting features
    tbl = Table.get('team')
    qr = '(in {} {spam!r} {foo:>5})'
    ast = Expression(tbl).parse(qr)
    qr = ast.eval(
        args=['ham'],
        kwargs={'spam': 'spam', 'foo': 'foo'})
    assert qr == '%s in (%s, %s)'
    assert ast.params == ['ham', "'spam'", '  foo']
