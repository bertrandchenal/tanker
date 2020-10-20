from .base_test import session
from tanker import ctx, execute

def test_kitchensink_types(session):
    if ctx.flavor != 'postgresql':
        return

    qr = '''SELECT column_name, data_type
    FROM information_schema.columns WHERE table_name = 'kitchensink'
    '''
    col_type = dict(execute(qr))
    assert col_type['id'] == 'bigint'
