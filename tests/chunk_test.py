from tanker import View, Expression
from .base_test import session

def test_expand(session):
    # Test formatting features
    view = View('team')
    qr = '(in {} {spam!r} {foo:>5})'
    ast = Expression(view).parse(qr)
    qr = ast.eval(
        args=['ham'],
        kwargs={'spam': 'spam', 'foo': 'foo'})
    assert qr == '%s in (%s, %s)'
    assert ast.params == ['ham', "'spam'", '  foo']
