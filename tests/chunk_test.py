from tanker import View, Expression, Chunk
from .base_test import session

def test_expand(session):
    # Test formatting features
    view = View('team')
    qr = '(in {} {spam!r} {foo:>5})'
    chunk = Chunk(Expression(view), qr)
    qr, args = chunk.expand(
        args=['ham'],
        kwargs={'spam': 'spam', 'foo': 'foo'})
    assert qr == '%s in (%s, %s)'
    assert args == ('ham', "'spam'", '  foo')
