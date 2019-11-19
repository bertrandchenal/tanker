# Plugin to integrate Tanker with Bottle. It creates a new tanker
# connection on each request (tanker re-use pooled pg connections)

# Usage:
# from bottle import install
# install(TankerPlugin(cfg))

from functools import wraps
from tanker import connect


class TankerPlugin():
    '''
    Plugin class to add tanker support to a bottle app
    '''

    name = 'TankerPlugin'
    api = 2

    def __init__(self, cfg):
        self.cfg = cfg

    def apply(self, callback, route):
        @wraps(callback)
        def wrapper(*args, **kwargs):
            with connect(self.cfg):
                return callback(*args, **kwargs)
        return wrapper
