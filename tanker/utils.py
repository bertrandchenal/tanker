from collections import OrderedDict
from datetime import datetime
from itertools import islice
import logging
import os
import threading

try:
    import pandas
except ImportError:
    pandas = None

LRU_SIZE = 10000
LRU_PAGE_SIZE = 100
basestring = (str, bytes)

__version__ = '0.8.8'

COLUMN_TYPE = (
    'BIGINT',
    'BOOL',
    'DATE',
    'FLOAT',
    'INTEGER',
    'M2O',
    'O2M',
    'TIMESTAMP',
    'TIMESTAMPTZ',
    'VARCHAR',
    'JSONB',
    'BYTEA',
)


fmt = '%(levelname)s:%(asctime).19s: %(message)s'
logging.basicConfig(format=fmt)
logger = logging.getLogger("tanker")
log_level = os.environ.get('TK_LOG_LEVEL', '').upper()
levels = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG', 'NOTSET']
if log_level in levels:
    logger.setLevel(log_level)


def yaml_load(stream):
    import yaml

    class OrderedLoader(yaml.Loader):
        pass

    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return OrderedDict(loader.construct_pairs(node))

    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping
    )
    return yaml.load(stream, OrderedLoader)


def interleave(value, items):
    """
    like str.join but for lists, automatically chain list of lists
    """
    if not items:
        return
    it = iter(items)
    looping = False
    while True:
        try:
            head = next(it)
        except StopIteration:
            break

        if looping:
            yield value
        else:
            looping = True
        if isinstance(head, (list, tuple)):
            for i in head:
                yield i
        else:
            yield head


def paginate(iterators, size=None):
    rows = zip(*iterators)
    while True:
        page = list(islice(rows, size or LRU_PAGE_SIZE))
        if not page:
            break
        yield page


TIME_FMT = {
    "TIMESTAMP": ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",],
    "TIMESTAMPTZ": ["%Y-%m-%d %H:%M:%S%z", "%Y-%m-%dT%H:%M:%S%z",],
    "DATE": ["%Y-%m-%d",],
}


def strptime(val, kind):
    for fmt in TIME_FMT[kind]:
        try:
            res = datetime.strptime(val, fmt)
        except ValueError:
            continue
        if kind == "DATE":
            return res.date()
        return res

    raise ValueError('Unable to parse "%s" as %s' % (val, kind.lower()))


class LRU:
    def __init__(self, init_data=None, size=None):
        self.size = size or LRU_SIZE
        self.recent = init_data or {}
        self.least_recent = {}

    def set(self, key, value):
        self.recent[key] = value
        self.vaccum()

    def update(self, values):
        self.recent.update(values)
        self.vaccum()

    def get(self, key, default=None):
        if key in self.recent:
            return self.recent[key]

        if key in self.least_recent:
            value = self.least_recent[key]
            self.recent[key] = value
            return value

        return default

    def vaccum(self):
        if len(self.recent) > self.size:
            self.least_recent = self.recent
            self.recent = {}

    def __contains__(self, key):
        if key in self.recent:
            return True
        if key in self.least_recent:
            self.recent[key] = self.least_recent[key]
            return True
        return False

    def __len__(self):
        return len(self.recent) + len(self.least_recent)


class ContextStack:
    def __init__(self):
        self._local = threading.local()

    def reset(self, contexts):
        self._local.contexts = contexts

    def push(self, cfg, new_ctx):
        if not hasattr(self._local, "contexts"):
            self._local.contexts = []

        self._local.contexts.append(new_ctx)
        new_ctx.enter()
        return new_ctx

    def pop(self, exc=None):
        popped = self._local.contexts.pop()
        popped.leave(exc)

    def active_context(self):
        return self._local.contexts[-1]


class ShallowContext:
    def __getattr__(self, name):
        return getattr(CTX_STACK.active_context(), name)


CTX_STACK = ContextStack()
ctx = ShallowContext()
