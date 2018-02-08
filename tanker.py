from collections import OrderedDict, defaultdict
from contextlib import contextmanager
from datetime import datetime, date, timedelta
from itertools import chain, islice
from string import Formatter
from threading import Thread
try:
    # PY2
    from urlparse import urlparse
except ImportError:
    # PY3
    from urllib.parse import urlparse
import argparse
import csv
import io
import logging
import os
import re
import shlex
import sqlite3
import sys
import textwrap
import threading
try:
    import pandas
except ImportError:
    pandas = None

PG_POOLS = {}
try:
    import psycopg2
    from psycopg2.pool import ThreadedConnectionPool
except ImportError:
    psycopg2 = None

# Python2/Python3 magic
PY2 = sys.version_info[0] == 2
if PY2:
    from itertools import izip
    BuffIO = io.BytesIO
    zip = izip
else:
    BuffIO = io.StringIO
if not PY2:
    basestring = (str, bytes)

__version__ = '0.7.1'

COLUMN_TYPE = (
    'BIGINT',
    'BOOL',
    'DATE',
    'FLOAT',
    'INTEGER',
    'M2O',
    'O2M',
    'TIMESTAMP',
    'VARCHAR',
)
QUOTE_SEPARATION = re.compile(r"(.*?)('.*?')", re.DOTALL)
NAMED_RE = re.compile(r"%\(([^\)]+)\)s")
EPOCH = datetime(1970, 1, 1)
LRU_SIZE = 10000
LRU_PAGE_SIZE = 100

all_none = lambda xs: all(x is None for x in xs)
skip_none = (lambda fn: (
    lambda x: None
    if x is None or (pandas and pandas.isnull(x))
    else fn(x)))
fmt = '%(levelname)s:%(asctime).19s: %(message)s'
logging.basicConfig(format=fmt)
logger = logging.getLogger('tanker')
logger.setLevel(logging.INFO)


def interleave(value, items):
    '''
    like str.join but for lists, automatically chain list of lists
    '''
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
            raise StopIteration
        yield page

TIMESTAMP_FMT = [
    '%Y-%m-%d %H:%M:%S',
    '%Y-%m-%dT%H:%M:%S',
]
DATE_FMT = [
    '%Y-%m-%d',
]
def strptime(val, kind):
    kind_fmt = TIMESTAMP_FMT if kind == 'timestamp' else DATE_FMT
    for fmt in kind_fmt:
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    raise ValueError('Unable to parse "%s" as datetime' % val)


class TankerThread(Thread):

    def __init__(self, *args, **kwargs):
        if CTX_STACK._local.contexts:
            # Capture current context if any
            self.stack = [ctx.copy()]
        else:
            self.stack = []
        super(TankerThread, self).__init__(*args, **kwargs)

    def run(self):
        CTX_STACK.reset(self.stack)
        super(TankerThread, self).run()


class Pool:

    _pools = {}

    def __init__(self, db_uri, cfg):
        self.cfg = cfg
        self.registry = OrderedDict()
        uri = urlparse(db_uri)
        dbname = uri.path[1:]
        self.flavor = uri.scheme
        self.pg_schema = None
        if self.flavor == 'sqlite':
            self.conn_args = [dbname]
            self.conn_kwargs = {
                'check_same_thread': False,
                'detect_types': sqlite3.PARSE_DECLTYPES,
                'isolation_level': 'DEFERRED',
            }

        elif self.flavor == 'postgresql':
            self.pg_schema = uri.fragment
            if psycopg2 is None:
                raise ImportError(
                    'Cannot connect to "%s" without psycopg2 package '
                    'installed' % db_uri)

            con_info = "dbname='%s' " % dbname
            if uri.hostname:
                con_info += "host='%s' " % uri.hostname
            if uri.username:
                con_info += "user='%s' " % uri.username
            if uri.password:
                con_info += "password='%s' " % uri.password
            if uri.port:
                con_info += "port='%s' " % uri.port

            pool_size = cfg.get('pg_pool_size', 10)
            self.pg_pool = ThreadedConnectionPool(1, pool_size, con_info)

        else:
            raise ValueError('Unsupported scheme "%s" in uri "%s"' % (
                uri.scheme, uri))

    def __enter__(self):
        if self.flavor == 'sqlite':
            connection = sqlite3.connect(*self.conn_args, **self.conn_kwargs)
            connection.text_factory = str
            connection.execute('PRAGMA foreign_keys=ON')
            connection.execute('PRAGMA journal_mode=wal')
        elif self.flavor == 'postgresql':
            connection = self.pg_pool.getconn()
            if self.pg_schema:
                cur = connection.cursor()
                cur.execute('CREATE SCHEMA IF NOT EXISTS %s' % self.pg_schema)
                cur.execute('SET search_path TO %s' % self.pg_schema)
        else:
            raise ValueError('Unexpected flavor "%s"' % self.flavor)

        new_ctx = CTX_STACK.push(connection, self)

        # Load schema as yaml if a string is given
        schema = self.cfg and self.cfg.get('schema')
        if isinstance(schema, basestring):
            schema = yaml_load(schema)
        if not schema:
            schema = new_ctx.introspect_db(auto=True)

        # Makes new_ctx init the pool registry if still empty
        if not self.registry and schema:
            for table_def in schema:
                new_ctx.register(table_def)

        return new_ctx

    def __exit__(self, exc_type, exc_value, traceback):
        connection = CTX_STACK.pop()
        if exc_value:
            connection.rollback()
        else:
            connection.commit()
        if self.flavor == 'postgresql':
            self.pg_pool.putconn(connection)

    @classmethod
    def disconnect(cls):
        for pool in cls._pools.values():
            if pool.flavor == 'postgresql':
                pool.pg_pool.closeall()
        cls.clear()

    @classmethod
    def clear(cls):
        cls._pools = {}

    @classmethod
    def get_pool(cls, cfg):
        db_uri = cfg.get('db_uri', 'sqlite:///:memory:')
        pool = cls._pools.get(db_uri)
        if pool:
            # Return existing pool for current db if any
            return pool

        pool = Pool(db_uri, cfg)
        cls._pools[db_uri] = pool
        return pool


class ContextStack:

    def __init__(self):
        self._local = threading.local()

    def reset(self, contexts):
        self._local.contexts = contexts

    def push(self, connection, pool):
        if not hasattr(self._local, 'contexts'):
            self._local.contexts = []

        new_ctx = Context(connection, pool)
        self._local.contexts.append(new_ctx)
        return new_ctx

    def pop(self):
        popped_ctx = self._local.contexts.pop()
        return popped_ctx.connection

    def active_context(self):
        return self._local.contexts[-1]


class ShallowContext:

    def __getattr__(self, name):
        return getattr(CTX_STACK.active_context(), name)


class Context:

    def __init__(self, connection, pool):
        self.flavor = pool.flavor
        self.pg_schema = pool.pg_schema
        self.encoding = pool.cfg.get('encoding', 'utf-8')
        self.connection = connection
        if self.flavor == 'postgresql':
            self.legacy_pg = connection.server_version < 90500
        self.cfg = pool.cfg
        self.aliases = {'null': None}
        self._fk_cache = {}
        self.db_tables = set()
        self.db_columns = defaultdict(OrderedDict)
        self.db_constraints = set()
        self.db_indexes = set()
        # Share pool registry
        self.pool = pool
        self.registry = pool.registry

    def copy(self):
        '''
        Create a clone of self, will trigger instanciation of a new cursor
        (the connection is shared)
        '''
        new_ctx = Context(self.connection, self.pool)
        new_ctx.aliases = self.aliases
        new_ctx.db_columns = self.db_columns
        new_ctx.db_tables = self.db_tables
        new_ctx.db_constraints = self.db_constraints
        new_ctx.db_indexes = self.db_indexes
        new_ctx.registry = self.registry
        new_ctx.cfg = self.cfg.copy()
        return new_ctx

    def _prepare_query(self, query):
        if self.flavor == 'postgresql':
            return query

        if self.flavor == 'sqlite':
            # Tranform named params: %(foo)s -> :foo
            query = NAMED_RE.sub(r':\1', query)

            # Transform positional params: %s -> ?. s/ilike/like.
            buf = ''
            for nquote, quote in QUOTE_SEPARATION.findall(query + "''"):
                nquote = nquote.replace('?', '??')
                nquote = nquote.replace('%s', '?')
                nquote = nquote.replace('ilike', 'like')
                buf += nquote + quote
            query = buf[:-2]
            return query

    def register(self, table_def):
        values = table_def.get('values')
        defaults = table_def.get('defaults', {})
        columns = []
        for col_name, col_type in table_def['columns'].items():
            new_col = Column(
                col_name, col_type, default=defaults.get(col_name))
            columns.append(new_col)
        # Instanciating the table adds it to current registry
        Table(name=table_def['table'], columns=columns,
              values=values,
              key=table_def.get('key', table_def.get('index')),
              unique=table_def.get('unique'))

    def reset_cache(self, table=None):
        if table is None:
            self._fk_cache = {}
        else:
            for key in list(self._fk_cache):
                if key[0] == table:
                    del self._fk_cache[key]

    def resolve_fk(self, fields, values):
        remote_table = fields[0].col.get_foreign_table().name
        key = (remote_table,) + fields
        mapping = self._fk_cache.get(key)
        if mapping is None:
            read_fields = list(self._fk_fields(fields))
            view = View(remote_table, read_fields + ['id'])
            db_values = view.read(disable_acl=True, limit=LRU_SIZE,
                                  order=('id', 'desc'))
            mapping = dict((val[:-1], val[-1])
                           for val in db_values)

            # Enable lru if fk mapping reach LRU_SIZE
            if len(mapping) == LRU_SIZE:
                mapping = LRU(mapping)
            self._fk_cache[key] = mapping

        if isinstance(mapping, LRU):
            read_fields = list(self._fk_fields(fields))
            view = View(remote_table, read_fields + ['id'])
            base_filter = '(AND %s)' % ' '.join(
                '(= %s {})' % f for f in read_fields)

            # Value is a list of column, paginate yield page that is a
            # small chunk of rows
            for page in paginate(values):
                missing = set(
                    val for val in page
                    if not all_none(val) and val not in mapping)
                if missing:
                    fltr = '(OR %s)' % ' '.join(base_filter for _ in missing)
                    for row in view.read(fltr, args=list(chain(*missing))):
                        # row[-1] is id
                        mapping.set(row[:-1], row[-1])
                for val in self._emit_fk(page, mapping, remote_table):
                    yield val

        else:
            for val in self._emit_fk(zip(*values), mapping, remote_table):
                yield val

    def _fk_fields(self, fields):
        for field in fields:
            yield field.desc.split('.', 1)[1]

    def _emit_fk(self, values, mapping, remote_table):
        for val in values:
            if all_none(val):
                yield None
                continue
            res = mapping.get(val)
            if res is None:
                raise ValueError('Values (%s) are not known in table "%s"' % (
                    ', '.join(map(repr, val)), remote_table))
            yield res

    def introspect_db(self, auto=False):
        # Collect table info
        if self.flavor == 'sqlite':
            qr = "SELECT name FROM sqlite_master WHERE type = 'table'"
        elif self.flavor == 'postgresql':
            qr = "SELECT table_name FROM information_schema.tables " \
                 "WHERE table_schema = '%s'" % (self.pg_schema or 'public')
        self.db_tables.update(name for name, in execute(qr))

        # Collect columns
        for table_name in self.db_tables:
            if self.flavor == 'sqlite':
                qr = 'PRAGMA table_info("%s")' % table_name
                cursor = execute(qr)
                current_cols = {x[1]: x[2] for x in cursor}
            elif self.flavor == 'postgresql':
                qr = '''
                 SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '%s' ''' % table_name
                cursor = execute(qr)
                current_cols = {x[0]: x[1] for x in cursor}
            self.db_columns[table_name] = current_cols

        # Collect indexes
        if self.flavor == 'sqlite':
            qr = "SELECT name FROM sqlite_master WHERE type = 'index'"
        elif self.flavor == 'postgresql':
            schema = self.pg_schema or 'public'
            qr = "SELECT indexname FROM pg_indexes " \
                 "WHERE schemaname = '%s'" % schema
        self.db_indexes = set(name for name, in execute(qr))

        # Collect constraints
        if self.flavor != 'sqlite':
            qr = 'SELECT constraint_name '\
                 'FROM information_schema.table_constraints'
            self.db_constraints = set(name for name, in execute(qr))

        if not auto:
            return

        foreign_keys = {}
        if self.flavor == 'sqlite':
            # Example invocation of fk pragma:
            #  sqlite> PRAGMA foreign_key_list(member);
            #  id|seq|table|from|to|on_update|on_delete|match
            #  0|0|team|team|id|NO ACTION|NO ACTION|NONE
            type_map = {
                # TODO
            }
            qr = 'PRAGMA foreign_key_list(%s)'
            for table_name in self.db_tables:
                cur = execute(qr % table_name)
                foreign_keys.update({
                    (table_name, r[3]): (r[2], r[4]) for r in cur})

        elif self.flavor == 'postgresql':
            qr = '''
            SELECT
              tc.table_name,
              kcu.column_name,
              ccu.table_name AS foreign_table_name,
              ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage
              AS kcu ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage
              AS ccu ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY';
            '''
            cur = execute(qr)
            foreign_keys.update({
                (r[0], r[1]): (r[2], r[3]) for r in cur})


        schema = defaultdict(OrderedDict)
        for table_name in self.db_tables:
            for name, data_type in self.db_columns[table_name].items():
                pass # TODO populate schema

        return schema

    def create_tables(self):
        # First we collect db info
        self.introspect_db()

        # Create tables and simple columns
        for table in self.registry.values():
            if table.name in self.db_tables:
                continue
            id_type = 'INTEGER' if self.flavor == 'sqlite' else 'SERIAL'
            col_defs = ['id %s PRIMARY KEY' % id_type]
            for col in table.columns:
                if col.ctype in ('M2O', 'O2M') or col.name == 'id':
                    continue
                col_defs.append('"%s" %s' % (col.name, col.sql_definition()))
                self.db_columns[table.name][col.name] = col.ctype

            qr = 'CREATE TABLE "%s" (%s)' % (table.name, ', '.join(col_defs))
            execute(qr)
            self.db_tables.add(table.name)
            logger.info('Table "%s" created', table.name)

        # Add columns
        for table_name in self.db_tables:
            if table_name not in self.registry:
                continue
            table_cols = self.db_columns[table_name]

            # Execute alter table queries
            table = self.registry[table_name]
            for col in table.own_columns:
                if col.name in table_cols:
                    continue
                table_cols[col.name] = col.ctype
                qr = 'ALTER TABLE "%(table)s" '\
                     'ADD COLUMN "%(name)s" %(def)s'
                params = {
                    'table': table.name,
                    'name': col.name,
                    'def': col.sql_definition(),
                }
                execute(qr % params)
                if not(self.flavor == 'sqlite' and col.ctype == 'M2O'):
                    continue
                # the on delete cascade is not enabled for sqlite
                # because the 'INSERT OR REPLACE' operation execute a
                # delete and thus execute the delete cascade. But it
                # does not execute triggers (see
                # https://stackoverflow.com/a/32554601)
                execute(
                    'CREATE TRIGGER on_delete_trigger_%(table)s_%(col)s '
                    'AFTER DELETE ON %(remote)s '
                    'BEGIN '
                    'DELETE FROM %(table)s '
                    'WHERE %(table)s.%(col)s=OLD.id;'
                    'END' % {
                        'remote': col.foreign_table,
                        'table': table.name,
                        'col': col.name,
                    })

        # Create indexes
        for table in self.registry.values():
            if not table.key:
                continue
            idx = 'unique_index_%s' % table.name
            if idx in self.db_indexes:
                continue
            self.db_indexes.add(idx)
            cols = ', '.join('"%s"' % c for c in table.key)
            qr = 'CREATE UNIQUE INDEX "%s" ON "%s" (%s)' % (
                idx, table.name, cols)
            execute(qr)

        # Add unique constrains (not supported by sqlite)
        if self.flavor != 'sqlite':
            unique_qr = 'ALTER TABLE "%s" ADD CONSTRAINT %s UNIQUE (%s)'
            for table in self.registry.values():
                for cols in table.unique:
                    cons_name = 'unique_' + '_'.join(cols)
                    if len(cons_name) > 63:
                        msg = 'Constrain name "%s" is too big'
                        ValueError(msg % cons_name)
                    if cons_name in self.db_constraints:
                        continue
                    self.db_constraints.add(cons_name)
                    cons_cols = ', '.join(cols)
                    execute(unique_qr % (table.name, cons_name, cons_cols))

        # Add pre-defined data
        for table in self.registry.values():
            if not table.values:
                continue
            logger.info('Populate %s' % table.name)
            view = View(table.name, fields=list(table.values[0].keys()))
            view.write(table.values)

def log_sql(query, params=None, exception=False):
    if not exception and logger.getEffectiveLevel() > logging.DEBUG:
        return
    indent = '  '
    query = textwrap.fill(query, initial_indent=indent,
                          subsequent_indent=indent)
    if params is None:
        args = ('SQL Query:\n%s', query)
    else:
        params = str(params)
        if len(params) > 1000:
            params = params[:1000] + '...'
        args = ('SQL Query:\n%s\nSQL Params:\n%s%s',
                query, indent, params)

    if exception:
        logger.error(*args)
    else:
        logger.debug(*args)


CTX_STACK = ContextStack()
ctx = ShallowContext()


# Build tuple of backend exceptions we want to catch
DB_EXCEPTION = (sqlite3.OperationalError,)
if psycopg2 is not None:
    DB_EXCEPTION += (psycopg2.ProgrammingError,)


class DBError(Exception):
    pass


def execute(query, params=None):
    log_sql(query, params)
    query = ctx._prepare_query(query)
    cursor = ctx.connection.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
    except DB_EXCEPTION as e:
        log_sql(query, params, exception=True)
        raise DBError(e)
    return cursor


def executemany(query, params):
    query = ctx._prepare_query(query)
    log_sql(query, params)
    cursor = ctx.connection.cursor()
    try:
        cursor.executemany(query, params)
    except DB_EXCEPTION as e:
        log_sql(query, params, exception=True)
        raise DBError(e)
    return cursor


def copy_from(buff, table, **kwargs):
    log_sql('"COPY FROM" called on table %s' % table)
    cursor = ctx.connection.cursor()
    cursor.copy_from(buff, table, **kwargs)
    return cursor


def create_tables():
    ctx.create_tables()


def fetch(tablename, filter_by):
    columns = [c.name for c in Table.get(tablename).own_columns]
    view = View(tablename, ['id'] + columns)
    values = view.read(filters=filter_by).one()
    if values is None:
        return
    keys = (f.name for f in view.fields)
    return dict(zip(keys, values))


def save(tablename, data):
    fields = data.keys()
    view = View(tablename, list(fields))
    view.write([data])


class ViewField:

    def __init__(self, name, desc, table):
        self.name = name
        self.desc = desc
        self.ref = None
        self.ctx = ctx

        if desc.startswith('('):
            ftype = ctype = 'EXPRESSION'
            self.col = None

        elif '.' in desc:
            ftype = 'INTEGER'
            self.ref = ReferenceSet(table).get_ref(desc)
            remote_col = self.ref.remote_table.get_column(
                self.ref.remote_field)
            ctype = remote_col.ctype
            self.col = table.get_column(desc.split('.')[0])

        elif desc.startswith('{'):
            ftype = ctype = 'ALIAS'
            self.col = None

        else:
            self.col = table.get_column(desc)
            ctype = self.col.ctype
            if ctype == 'M2O':
                ctype = ftype = 'INTEGER'
            else:
                ftype = ctype

        self.ctype = ctype.upper()
        self.ftype = ftype.upper()

    def __repr__(self):
        if self.name != self.desc:
            return '<ViewField %s (%s)>' % (self.desc, self.name)
        return '<ViewField %s>' % self.desc


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


class View(object):

    def __init__(self, table, fields=None):
        self.ctx = ctx
        self.table = Table.get(table)
        if fields is None:
            fields = []
            for col in self.table.own_columns:
                if col.ctype == 'M2O':
                    ft = col.get_foreign_table()
                    fields.extend('.'.join((col.name, i)) for i in ft.key)
                else:
                    fields.append(col.name)
        if isinstance(fields, basestring):
            fields = [[fields, fields]]
        elif isinstance(fields, dict):
            fields = fields.items()
        elif isinstance(fields, (list, tuple)) and isinstance(fields[0], basestring):
            fields = zip(fields, fields)
        elif isinstance(fields, (list, tuple)) and isinstance(fields[0], tuple):
            fields = fields

        self.fields = [ViewField(name.strip(), desc, self.table)
                       for name, desc in fields]
        self.field_dict = dict((f.name, f) for f in self.fields)

        # field_map hold relation between fields given by the user and
        # the one from the db, field_idx keep their corresponding
        # positions
        self.field_map = defaultdict(list)
        self.field_idx = defaultdict(list)
        idx = 0
        for view_field in self.fields:
            if view_field.col is None:
                continue
            if self.field_map[view_field.col]:
                if view_field.col.ctype not in ('M2O', 'O2M'):
                    raise ValueError(
                        'Column %s is specified several time in view'
                        % view_field.col.name)
            self.field_map[view_field.col].append(view_field)
            self.field_idx[view_field.col].append(idx)
            idx += 1

        # Key fields identify each line in the data
        self.key_fields = [f for f in self.fields
                             if f.col and f.col.name in self.table.key]
        # Key cols identify each row in the table
        id_col = self.table.get_column('id')
        if id_col in self.field_map:
            # Use id if present
            self.key_cols = [id_col.name]
        else:
            # Use natural key if not
            self.key_cols = [c.name for c in self.field_map
                               if c.name in self.table.key]

    def get_field(self, name):
        return self.field_dict.get(name)

    def _build_filter_cond(self, exp, *filters):
        res = []
        for fltr in filters:
            if not fltr:
                continue

            # filters can be a dict
            if isinstance(fltr, dict):
                # Add simple equal conditions
                for key, val in fltr.items():
                    ast = exp.parse('(= %s {}) ' % key)
                    ast.args = [val]
                    res.append(ast)
                continue

            # Filters can be a query string or a list of query string
            if isinstance(fltr, basestring):
                fltr = [fltr]
            # Parse expression filters
            for line in fltr:
                ast = exp.parse(line)
                res.append(ast)

        return list(interleave(' AND ', res))

    def read(self, filters=None, args=None, order=None, groupby=None,
             limit=None, distinct=False, offset=None, disable_acl=False):

        if isinstance(filters, basestring):
            filters = [filters]

        acl_filters = None
        if not disable_acl:
            acl_filters = self.ctx.cfg.get('acl_rules', {}).get(self.table.name)

        exp = Expression(self)

        # Add select fields
        statement = '(select-distinct %s)' if distinct else '(select %s)'
        select_ast = exp.parse(statement % ' '.join(
            f.desc for f in self.fields))
        select_chunk = [select_ast]
        select_chunk.append('FROM "%s"' % self.table.name)

        # Identify aggregates
        aggregates = []
        for pos, atom in enumerate(select_ast.atoms[1:]):
            if not isinstance(atom, AST):
                continue
            head = atom.atoms[0]
            if head.token in Expression.aggregates:
                aggregates.append(pos)

        # Add filters
        filter_chunks = self._build_filter_cond(exp, filters, acl_filters)
        if filter_chunks:
            filter_chunks = ['WHERE'] + filter_chunks

        # Add group by
        groupby_chunks = []
        group_fields = []
        if groupby and isinstance(groupby, basestring):
                groupby = [groupby]
        elif aggregates and not groupby:
            groupby = []
            for pos, field in enumerate(self.fields):
                if pos in aggregates:
                    continue
                groupby.append(field.desc)

        if groupby:
            group_fields = [exp.parse(f) for f in groupby]
            groupby_chunks = ['GROUP BY'] + list(interleave(',', group_fields))

        if order:
            order_chunks = []
            if isinstance(order, (str, tuple)):
                order = [order]
            for item in order:
                if isinstance(item, (list, tuple)):
                    item, how = item
                else:
                    how = None
                chunk = [exp.parse(item)]

                if how:
                    if how.upper() not in ('ASC', 'DESC'):
                        msg = 'Unexpected value "%s" for sort direction' % how
                        raise ValueError(msg)
                    chunk.append(how)
                order_chunks += [chunk]
            order_chunks = ['ORDER BY'] + list(interleave(',', order_chunks))
        else:
            order_chunks = []

        join_chunks = [exp.ref_set]
        all_chunks = (select_chunk + join_chunks + filter_chunks
                      + groupby_chunks + order_chunks)

        if limit is not None:
            all_chunks += ['LIMIT %s' % int(limit)]
        if offset is not None:
            all_chunks += ['OFFSET %s' % int(limit)]

        return TankerCursor(self, all_chunks, args=args)

    def format(self, data):
        for col in self.field_map:
            idx = self.field_idx[col]
            if col.ctype == 'M2O':
                fields = tuple(f for f in self.field_map[col])
                values = tuple(data[i] for i in idx)
                if len(fields) == 1 and fields[0].ref is None:
                    # Handle update of fk by id
                    yield map(int, data[idx[0]])
                else:
                    # Resole foreign key reference
                    fmt_cols = lambda a: tuple(
                        a[0].col.format(a[1], astype=a[0].ctype))
                    values = map(fmt_cols, zip(fields, values))
                    yield ctx.resolve_fk(fields, values)
            else:
                yield col.format(data[idx[0]])

    def delete(self, filters=None, data=None, args=None, table_alias=None,
               swap=False):
        '''
        Delete rows from table that:
        - match `filters` if set (or that doesn't match `filters` if
          swap is set
        - match `data` based on key columns (or doesn't match if swap is set)
        Only one of `filters` or `data` can be passed.

        table_alias allows to pass an alternate table name (that will
        act as self.table).
        `args` is a dict of values that allows to parameterize `filters`.
        '''
        if table_alias and not filters:
            raise ValueError('table_alias parameter is only supported with '
                             'non-empty filters parameters')
        if not any((data, filters)):
            qr = 'DELETE FROM "%s"' % self.table.name
            return execute(qr)

        if data and filters:
            raise ValueError('Deletion by both data and filter not supported')

        exp = Expression(self, table_alias=table_alias)
        filter_chunks = self._build_filter_cond(exp, filters)

        if data:
            # Transform rows into columns
            data = list(zip(*data))
            data = list(self.format(data))
            with self._prepare_write(data) as join_cond:
                qr = 'DELETE FROM "%(main)s" WHERE id %(op)s (' \
                     'SELECT "%(main)s".id FROM "%(main)s" ' \
                     'INNER JOIN tmp on %(join_cond)s)'
                qr = qr % {
                    'main': self.table.name,
                    'op': 'NOT IN' if swap else 'IN',
                    'join_cond': ' AND '.join(join_cond),
                }
                cur = execute(qr)

        else:
            qr = ('DELETE FROM "%(main_table)s" WHERE id %(op)s ('
                  'SELECT "%(main_table)s".id FROM "%(main_table)s"')
            qr = qr % {
                'main_table': table_alias or self.table.name,
                'op': 'NOT IN' if swap else 'IN',
            }
            chunks = [qr, exp.ref_set]
            if filter_chunks:
                chunks += ['WHERE'] + filter_chunks
            chunks.append(')')
            cur = TankerCursor(self, chunks, args=args).execute()
        return cur.rowcount

    @contextmanager
    def _prepare_write(self, data, filters=None, disable_acl=False, args=None):
        # An id column is needed to enable filters (and for sqlite
        # REPLACE)
        extra_id = 'id' not in self.field_dict
        not_null = lambda n: 'NOT NULL' if n in self.key_fields else ''
        # Create tmp
        qr = 'CREATE TEMPORARY TABLE tmp (%s)'
        col_defs = ', '.join('"%s" %s %s' % (
            col.name,
            fields[0].ftype,
            not_null(col.name))
            for col, fields in self.field_map.items())
        if extra_id:
            id_type = 'INTEGER' if ctx.flavor == 'sqlite' else 'SERIAL'
            col_defs += ', id %s PRIMARY KEY' % id_type
        qr = qr % col_defs
        execute(qr)

        # Fill tmp
        if self.ctx.flavor == 'postgresql':
            buff = BuffIO()
            writer = csv.writer(buff, delimiter='\t')
            # postgresql COPY doesn't like line feed
            repl = lambda x: x.replace(
                '\n', '\\n').replace(
                '\t', '\\t').replace(
                '\r', '\\r')
            clean = lambda x: repl(x) if isinstance(x, str) else x
            # Clean by column
            for pos, c in enumerate(self.field_map):
                if c.ctype != 'VARCHAR':
                    continue
                data[pos] = [clean(v) for v in data[pos]]

            # Append to writer by row
            for row in zip(*data):
                writer.writerow(row)
            buff.seek(0)
            copy_from(buff, 'tmp', null='',
                      columns=['"%s"' % c.name for c in self.field_map])

        else:
            qr = 'INSERT INTO tmp (%(fields)s) VALUES (%(values)s)'
            qr = qr % {
                'fields': ', '.join('"%s"' % c.name for c in self.field_map),
                'values': ', '.join('%s' for _ in self.field_map),
            }
            executemany(qr, zip(*data))

        # Create join conditions
        join_cond = []
        for name in self.key_cols:
            join_cond.append('tmp."%s" = "%s"."%s"' % (
                name, self.table.name, name))

        # Apply filters if any
        if not disable_acl:
            filters = filters or []
            acl = self.ctx.cfg.get('acl_rules', {})
            filters += acl.get(self.table.name, [])

        if filters:
            # Filter is based on existing line in self.table, so it
            # only affect updates (and not inserts)
            # (We introduced acl in filters, so we disable them)
            self._purge(join_cond, filters, disable_acl=True, action='update',
                        args=args)

        yield join_cond

        if filters:
            # Delete inserted lines that do not match the filters
            # (We introduced acl in filters, so we disable them)
            self._purge(join_cond, filters, disable_acl=True, action='insert',
                        args=args)

        # Clean tmp table
        execute('DROP TABLE tmp')

    def write(self, data, purge=False, insert=True, update=True, filters=None,
              disable_acl=False, args=None):
        '''
        Write given data to view table. If insert is true, new lines will
        be inserted.  if update is true, existing line will be
        updated. If purge is true existing line that are not present
        in data (and that match filters) will be deleted.
        '''

        # Handle list of dict and dataframes
        if isinstance(data, list) and data and isinstance(data[0], dict):
            data = [[record.get(f.name) for record in data]
                    for f in self.fields]
        elif pandas and isinstance(data, pandas.DataFrame):
            fields = [f.name for f in self.fields]
            data = [data[f].values for f in fields]
        else:
            # Transform rows into columns
            data = list(zip(*data))
            # Zip wont create a list of empty list if given data is
            # empty:
            if not data:
                data = [[] for _ in self.fields]
        # Format values
        data = list(self.format(data))

        if isinstance(filters, basestring):
            filters = [filters]

        # Launch upsert
        with self._prepare_write(
                data, filters=filters, disable_acl=disable_acl, args=args
        ) as join_cond:
            rowcounts = {}
            if self.ctx.flavor == 'sqlite':
                self._sqlite_upsert(join_cond, insert, update)
            elif ctx.legacy_pg:
                if insert:
                    cnt = self._insert(join_cond)
                    rowcounts['insert'] = cnt
                if update:
                    cnt = self._update(join_cond)
                    rowcounts['update'] = cnt
            else:
                # ON-CONFLICT is available since postgres 9.5
                cnt = self._pg_upsert(join_cond, insert=insert,
                                      update=update)
                rowcounts['upsert'] = cnt

            if purge:
                cnt = self._purge(join_cond, filters, disable_acl,
                                  action='purge', args=args)
                rowcounts['delete'] = cnt

        # Clean cache for current table
        self.ctx.reset_cache(self.table.name)
        return rowcounts

    def _sqlite_upsert(self, join_cond, insert, update):
        # As sqlite cannot update only some columns whe have to also
        # update fields not in the query
        qr_cols = [f.name for f in self.field_map]
        other_cols = [col.name for col in self.table.own_columns \
                      if col.name not in qr_cols]
        qr = 'INSERT OR REPLACE INTO "%(main)s" (%(fields)s) %(select)s'
        select = 'SELECT %(tmp_fields)s FROM tmp '\
                 '%(join_type)s JOIN "%(main_table)s" ON ( %(join_cond)s)'
        tmp_fields = ', '.join('tmp."%s"' % c for c in qr_cols)
        if other_cols:
            tmp_fields += ', '
            tmp_fields += ', '.join('"%s"."%s"' % (self.table.name, f)\
                                    for f in other_cols)
        if 'id' not in self.field_dict:
            other_cols.append('id')
            tmp_fields += ', "%s".id' % self.table.name

        select = select % {
            'tmp_fields': tmp_fields,
            'main_table': self.table.name,
            'join_cond': ' AND '.join(join_cond),
            'join_type': 'LEFT' if insert else 'INNER',
        }
        qr = qr % {
            'main': self.table.name,
            'fields': ', '.join('"%s"' % c for c in qr_cols + other_cols),
            'select': select,
        }
        cur = TankerCursor(self, [qr]).execute()
        return cur.rowcount

    def _pg_upsert(self, join_cond, insert, update):
        tmp_fields = ', '.join('tmp."%s"' % f.name for f in self.field_map)
        main_fields = ', '.join('"%s"' % f.name for f in self.field_map)
        upd_fields = []
        for f in self.field_map:
            if f.name in self.key_cols:
                continue
            upd_fields.append('"%s" = EXCLUDED."%s"' % (f.name, f.name))

        qr = (
            'INSERT INTO "%(main)s" (%(main_fields)s) '
            'SELECT %(tmp_fields)s FROM tmp '
            '%(join_type)s JOIN "%(main)s" ON ( %(join_cond)s) ')
        if upd_fields and update:
            qr += 'ON CONFLICT (%(idx)s) DO UPDATE SET %(upd_fields)s'
        else:
            qr += 'ON CONFLICT (%(idx)s) DO NOTHING'

        qr = qr % {
            'main': self.table.name,
            'main_fields': main_fields,
            'tmp_fields': tmp_fields,
            'join_cond': ' AND '.join(join_cond),
            'join_type': 'LEFT' if insert else 'INNER',
            'upd_fields': ', '.join(upd_fields),
            'idx': ', '.join(self.key_cols),
        }
        cur = TankerCursor(self, qr).execute()
        return cur.rowcount

    def _insert(self, join_cond):
        qr = 'INSERT INTO "%(main)s" (%(fields)s) %(select)s'
        select = 'SELECT %(tmp_fields)s FROM tmp '\
                 'LEFT JOIN "%(main)s" ON ( %(join_cond)s) ' \
                 'WHERE %(where_cond)s'

        # Consider only new rows
        where_cond = []
        for name in self.key_cols:
            where_cond.append('%s."%s" IS NULL' % (self.table.name, name))

        tmp_fields = ', '.join('tmp."%s"' % f.name for f in self.field_map)
        select = select % {
            'tmp_fields': tmp_fields,
            'main': self.table.name,
            'join_cond': ' AND '.join(join_cond),
            'where_cond': ' AND '.join(where_cond),
        }
        qr = qr % {
            'main': self.table.name,
            'fields': ', '.join('"%s"' % f.name for f in self.field_map),
            'select': select,
        }
        cur = TankerCursor(self, qr).execute()
        return cur.rowcount

    def _update(self, join_cond):
        update_cols = [f.name for f in self.field_map
                       if f.name not in self.key_cols]
        if not update_cols:
            return 0

        where = ' AND '.join(join_cond)
        qr = 'UPDATE "%(main)s" SET '
        qr += ', ' .join('"%s" = tmp."%s"' % (n, n) for n in update_cols)
        qr += ' FROM tmp WHERE %(where)s'
        qr = qr % {
            'main': self.table.name,
            'where': where,
        }
        cur = TankerCursor(self, qr).execute()

        return cur and cur.rowcount or 0

    def _purge(self, join_cond, filters, disable_acl=False, action='purge',
               args=None):
        '''
        Delete rows from main table that are not in tmp table and evaluate
        filters to true. Do the opposite if swap_table is True (keep in tmp
        lines that are also in main and that evaluate filter to false.
        '''
        assert action in ('purge', 'update', 'insert')
        insert = action == 'insert'
        update = action == 'update'
        main = self.table.name
        tmp = 'tmp'
        if update:
            assert bool(filters), 'filters is nedded to purge on tmp'
            main, tmp = tmp, main

        # Prepare basic query
        head_qr = (
            'DELETE FROM "%(main)s" '
            'WHERE id %(filter_operator)s ('
            ' SELECT "%(main)s".id FROM "%(main)s" ')
        join_qr = '{} JOIN %(tmp)s on (%(join_cond)s) '.format(
            'INNER' if insert else 'LEFT')
        excl_cond = '' if insert else '%(tmp)s.%(field)s IS NULL'
        tail_qr = ')'

        # Format all parts of the query
        fmt = {
            'main': main,
            'tmp': tmp,
            'filter_operator': 'NOT IN' if update else 'IN',
            'join_cond': ' AND '.join(join_cond),
            'field': self.key_cols[0]
        }
        head_qr = head_qr % fmt
        join_qr = join_qr % fmt
        excl_cond = excl_cond % fmt

        # Build filters
        acl_filters = None
        if not disable_acl:
            acl_filters = self.ctx.cfg.get('acl_rules', {}).get(self.table.name)
        exp = Expression(self)
        filter_chunks = self._build_filter_cond(exp, filters, acl_filters)
        join_chunks = [exp.ref_set]

        if filter_chunks:
            qr = [head_qr] + [join_qr] + join_chunks
            if insert:
                qr += ['WHERE NOT ('] \
                      + filter_chunks \
                      + [')']
            else:
                qr += ['WHERE'] + filter_chunks
            if excl_cond:
                qr += ['OR' if update else 'AND', excl_cond]
            qr += [tail_qr]
        else:
            qr = head_qr + join_qr
            if excl_cond:
                qr += ' WHERE ' + excl_cond
            qr += tail_qr

        cur = TankerCursor(self, qr, args=args).execute()
        return cur.rowcount


class TankerCursor:

    def __init__(self, view, chunks, args=None):
        self.view = view
        self.db_cursor = None
        if isinstance(chunks, basestring):
            chunks = [chunks]
        self.chunks = chunks
        if isinstance(args, dict):
            self._kwargs = args
            self._args = None
        else:
            self._args = args and args[:]
            self._kwargs = None

    def args(self, *args, **kwargs):
        '''
        Set args for current cursor
        '''
        self._args = list(args)
        self._kwargs = kwargs
        # reset db_cursor to allow to call args & re-launch query
        self.db_cursor = None
        return self

    def execute(self):
        if self.db_cursor is not None:
            return self.db_cursor

        qr, args = self.expand()
        self.db_cursor = execute(qr, args)
        return self.db_cursor

    def executemany(self):
        if self.db_cursor is not None:
            return self.db_cursor

        qr, args = self.expand()
        self.db_cursor = executemany(qr, args)
        return self.db_cursor

    def __iter__(self):
        return self.execute()

    def split(self, x):
        if isinstance(x, ReferenceSet):
            # Delay evaluation of refset
            return ' '.join(x.get_sql_joins()), None
        if isinstance(x, ExpressionSymbol):
            return x.eval(), None
        if isinstance(x, (AST)):
            # TODO kwargs should be evaled earlier
            kwargs = self.view.ctx.aliases.copy()
            kwargs.update(self._kwargs or {})
            cfg = ctx.cfg
            kwargs.update(cfg)
            return x.eval(self._args, kwargs), x.params
        if isinstance(x, tuple):
            return x
        if isinstance(x, basestring):
            return x, None

        raise ValueError('Unable to stringify "%s"' % x)

    def expand(self):
        queries, args = zip(*map(self.split, self.chunks))
        qr = ' '.join(queries)
        chained_args = chain.from_iterable(a for a in args if a)
        return qr, tuple(chained_args)

    def __next__(self):
        return next(iter(self))

    def one(self):
        return next(iter(self), None)

    def next(self):
        return next(iter(self))

    def all(self):
        return list(self)

    def chain(self):
        items = iter(self)
        return chain(*items)

    def dict(self):
        keys = [f.name for f in self.view.fields]
        for row in self:
            yield dict(zip(keys, row))

    def df(self):
        if not pandas:
            raise ImportError('The pandas module is required by Cursor.df')
        read_columns = [f.name for f in self.view.fields]
        df = pandas.DataFrame.from_records(self, columns=read_columns)
        return df


class Table:

    def __init__(self, name, columns, key=None, unique=None, values=None):
        self.name = name
        self.columns = columns[:]
        self.unique = unique or []
        self.values = values

        # Add implicit id column
        if 'id' not in [c.name for c in self.columns]:
            self.columns.append(Column('id', 'INTEGER'))
        self.own_columns = [c for c in self.columns
                            if c.name != 'id' and c.ctype != 'O2M']

        # Set table attribute on columns object
        for col in self.columns:
            col.table = self

        # set key
        if key is None:
            if len(self.columns) == 2:
                # If there is only one column (other than id), use it
                # as key
                key = tuple(c.name for c in self.columns if c.name != 'id')
            else:
                raise ValueError('No key defined on %s' % name)
        self.key = [key] if isinstance(key, basestring) else key
        # Test key columns are members of the table
        self._column_dict = dict((col.name, col) for col in self.columns)
        for col in self.key:
            if col not in self._column_dict:
                raise ValueError('Key column "%s" does not exist' % col)
        # # Forbid array types in ey
        # for col in self.key:
        #     if col.array_dim:
        #         msg = 'Array type is not allowed on key column '\
        #               '("%s" in table "%s")'
        #         raise ValueError(msg % (col, self.name))

        self.ctx = ctx
        self.ctx.registry[name] = self

    def get_column(self, name):
        try:
            return self._column_dict[name]
        except KeyError:
            raise KeyError('Column "%s" not found in table "%s"' % (
                name, self.name))

    @classmethod
    def get(cls, table_name):
        return ctx.registry[table_name]

    def __repr__(self):
        return '<Table %s>' % self.name

    def link(self, dest):
        '''
        Returns all the possible set of relations between self and dest
        '''
        wave = [self]
        paths = defaultdict(list)

        while True:
            new_wave = []
            for tbl in wave:
                visited = set(chain.from_iterable(paths[tbl]))
                for col in tbl.columns:
                    # Follow non-visited relations
                    if col.ctype not in ('M2O', 'O2M'):
                        continue
                    if col in visited:
                        continue

                    # Add column to ancestor paths
                    foreign_table = col.get_foreign_table()
                    if paths[tbl]:
                        foreign_paths = [p + [col] for p in paths[tbl]]
                        paths[foreign_table].extend(foreign_paths)
                    else:
                        paths[foreign_table] = [[col]]
                    new_wave.append(foreign_table)
            if not new_wave:
                # No table to visit anymore
                break
            wave = new_wave
        return sorted(paths[dest], key=len)


class Column:

    def __init__(self, name, ctype, default=None):
        if ' ' in ctype:
            full_ctype = ctype
            ctype, self.fk = ctype.split()
            if '.' not in self.fk:
                msg = 'Malformed column definition "%s" for %s'
                raise ValueError(msg % (full_ctype, name))
            self.foreign_table, self.foreign_col = self.fk.split('.')
        else:
            self.fk = None
            self.foreign_table = self.foreign_col = None
        self.name = name
        self.default = default

        # Build ctype, array_dim and base_type
        self.ctype = ctype.upper()
        self.base_type = self.ctype
        self.array_dim = 0
        while self.base_type.endswith('[]'):
            self.base_type = self.base_type[:-2]
            self.array_dim += 1
        if self.array_dim and ctx.flavor == 'sqlite':
            self.ctype = 'BLOB'
        if self.array_dim and self.base_type in ('O2M', 'M2O'):
            msg = 'Array type is not supported on "%s" (for column "%s")'
            raise ValueError(msg % (self.base_type, name))
        if self.base_type not in COLUMN_TYPE:
            raise ValueError('Unexpected type %s for column %s' % (ctype, name))

    def sql_definition(self):
        # Simple field
        if not self.fk:
            if self.default:
                return '%s DEFAULT %s' % (self.ctype, self.default)
            return self.ctype
        # O2M
        if self.ctype == 'O2M':
            return None
        # M2O
        table = Table.get(self.foreign_table).name
        cascade = 'ON DELETE CASCADE' if ctx.flavor == 'postgresql' else ''
        return 'INTEGER REFERENCES "%s" ("%s") %s' % (
            table, self.foreign_col, cascade)

    def get_foreign_table(self):
        if not self.foreign_table:
            raise ValueError('The "%s" column of "%s" is not a foreign key' % (
                self.name, self.table.name))
        return Table.get(self.foreign_table)

    def format_array(self, array, astype, array_dim):
        if array is None:
            return None
        if array_dim == 1:
            items = self.format(array, astype=astype, array_dim=0)
            items = map(lambda x: 'null' if x is None else str(x), items)
        else:
            items = (
                self.format_array(v, astype=astype, array_dim=array_dim-1)
                for v in array)
        items = ','.join(items)
        return '{%s}' % items

    def format(self, values, astype=None, array_dim=None):
        '''
        Sanitize a column of values wrt the column type of the current
        field.
        '''
        astype = astype or self.base_type
        array_dim = self.array_dim if array_dim is None else array_dim

        if array_dim:
            for array in values:
                yield self.format_array(array, astype, array_dim)

        elif astype in ('INTEGER', 'BIGINT'):
            for v in map(skip_none(int), values):
                yield v

        elif astype == 'VARCHAR':
            for value in values:
                if not value:
                    value = None
                elif not isinstance(value, basestring):
                    value = str(value)
                else:
                    if PY2 and isinstance(value, unicode):
                        value = value.encode(ctx.encoding)
                    elif not PY2 and isinstance(value, bytes):
                        value = value.encode(ctx.encoding)
                yield value

        elif astype == 'TIMESTAMP':
            for value in values:
                if not value:
                    yield None
                elif isinstance(value, datetime):
                    yield value
                elif hasattr(value, 'timetuple'):
                    value = datetime(*value.timetuple()[:7])
                    yield value
                elif hasattr(value, 'tolist'):
                    # tolist is a numpy.datetime64 method that
                    # returns nanosecond from 1970. EPOCH + delta(val)
                    # suppors values far in the past (or future)
                    ts = value.tolist()
                    if ts is None:
                        value = None
                    else:
                        value = EPOCH + timedelta(seconds=ts/1e9)
                    yield value
                elif isinstance(value, basestring):
                    yield strptime(value, 'timestamp')
                else:
                    raise ValueError(
                        'Unexpected value "%s" for type "%s"' % (
                            value, astype))

        elif astype == 'DATE':
            for value in values:
                if not value:
                    yield None
                elif isinstance(value, date):
                    yield value
                elif hasattr(value, 'timetuple'):
                    value = date(*value.timetuple()[:3])
                    yield value
                elif hasattr(value, 'tolist'):
                    ts = value.tolist()
                    if ts is None:
                        value = None
                    else:
                        dt = EPOCH + timedelta(seconds=ts/1e9)
                        value = date(*dt.timetuple()[:3])
                    yield value
                elif isinstance(value, basestring):
                    yield strptime(value, 'date')
                else:
                    raise ValueError(
                        'Unexpected value "%s" for type "%s"' % (
                            value, astype))
        else:
            for v in values:
                yield v

    def __repr__(self):
        return '<Column %s %s>' % (self.name, self.ctype)


class Reference:

    def __init__(self, remote_table, remote_field, rjoins, join_alias, column):
        self.remote_table = remote_table
        self.remote_field = remote_field
        self.rjoins = rjoins
        self.join_alias = join_alias
        self.column = column

    def __repr__(self):
        return '<Reference table=%s field=%s>' % (
            self.remote_table.name,
            self.remote_field)


class ReferenceSet:

    def __init__(self, table, table_alias=None, parent=None):
        '''
        A ReferenceSet helps to 'browse' across table by joining them. The
        ReferenceSet hold the set of joins that has to be done to
        resolve the cols that were added through the add() method.
        '''
        self.table = table
        self.table_alias = table_alias or table.name
        self.joins = OrderedDict()
        self.references = []
        self.parent = parent
        self.children = []
        if parent:
            parent.children.append(self)

    def add(self, desc):
        ref = self.get_ref(desc)
        self.references.append(ref)
        return ref

    def get_sql_joins(self):
        for key, alias in self.joins.items():
            left_table, right_table, left_col, right_col = key
            yield 'LEFT JOIN "%s" AS "%s" ON ("%s"."%s" = "%s"."%s")' % (
                right_table, alias, left_table, left_col, alias,
                right_col
            )

    def get_ref(self, desc, table=None, alias=None):
        table = table or self.table
        alias = alias or self.table_alias

        # Simple col, return
        if '.' not in desc:
            col = table.get_column(desc)
            return Reference(table, desc, self.joins, alias, col)

        # Resolve column
        head, tail = desc.split('.', 1)
        rel = table.get_column(head)
        foreign_table = rel.get_foreign_table()

        # Compute join
        left_table = alias
        right_table = foreign_table.name

        if rel.ctype == 'M2O':
            left_col = head
            right_col = rel.foreign_col
        else:
            # O2M, defined like other_table.fk
            fk = rel.foreign_col
            # left_col is the column pointed by the fk
            left_col = foreign_table.get_column(fk).foreign_col
            right_col = fk

        key_alias = '%s_%s' % (right_table, self.get_nb_joins())
        key = (left_table, right_table, left_col, right_col)
        foreign_alias = self.joins.setdefault(key, key_alias)

        # Recurse
        return self.get_ref(tail, table=foreign_table, alias=foreign_alias)

    def get_nb_joins(self, up=True):
        if up and self.parent:
            return self.parent.get_nb_joins()
        cnt = len(self.joins)
        for child in self.children:
            cnt += child.get_nb_joins(up=False)
        return cnt

    def __iter__(self):
        return iter(self.references)

    def __repr__(self):
        return '<ReferenceSet [%s]>' % ', '.join(map(str, self.references))


class Expression(object):
    # Inspired by http://norvig.com/lispy.html

    builtins = {
        '+': lambda *xs: '(%s)' % ' + '.join(xs),
        '-': lambda *xs: '(%s)' % ' - '.join(xs),
        '*': lambda *xs: '(%s)' % ' * '.join(xs),
        '/': lambda *xs: '(%s)' % ' / '.join(xs),
        'and': lambda *xs: '(%s)' % ' AND '.join(xs),
        'or': lambda *xs: '(%s)' % ' OR '.join(xs),
        '>=': lambda x, y: '%s >= %s' % (x, y),
        '<=': lambda x, y: '%s <= %s' % (x, y),
        '=': lambda x, y: '%s = %s' % (x, y),
        '>': lambda x, y: '%s > %s' % (x, y),
        '<': lambda x, y: '%s < %s' % (x, y),
        '!=': lambda x, y: '%s != %s' % (x, y),
        'like': lambda x, y: '%s like %s' % (x, y),
        'ilike': lambda x, y: '%s ilike %s' % (x, y),
        'in': lambda *xs: ('%%s in (%s)' % (
            ', '.join('%s' for _ in xs[1:]))) % xs,
        'notin': lambda *xs: ('%%s not in (%s)' % (
            ', '.join('%s' for _ in xs[1:]))) % xs,
        'any': lambda x: 'any(%s)' % x,
        'all': lambda x: 'all(%s)' % x,
        'unnest': lambda x: 'unnest(%s)' % x,
        'is': lambda x, y: '%s is %s' % (x, y),
        'isnot': lambda x, y: '%s is not %s' % (x, y),
        'not': lambda x: 'not %s' % x,
        'exists': lambda x: 'EXISTS (%s)' % x,
        'where': lambda *x: 'WHERE ' + ' AND '.join(x),
        'select': lambda *x: 'SELECT ' + ', '.join(x),
        'select-distinct': lambda *x: 'SELECT DISTINCT ' + ', '.join(x),
        'cast': lambda x, y: 'CAST (%s AS %s)' % (x, y),
        'extract': lambda x, y: 'EXTRACT (%s FROM %s)' % (x, y),
        'floor': lambda x: 'floor(%s)' % x,
    }

    aggregates = {
        'avg': lambda *x: 'avg(%s)' % x,
        'count': lambda *x: 'count(%s)' % ', '.join(x or ['*']),
        'max': lambda *x: 'max(%s)' % x,
        'min': lambda *x: 'min(%s)' % x,
        'sum': lambda *x: 'sum(%s)' % x,
    }

    def __init__(self, view, ref_set=None, parent=None, table_alias=None):
        self.view = view
        # Populate env with view fields
        self.env = self.base_env(view.table)
        self.builtins = {'from': self._sub_select}
        self.builtins.update(Expression.builtins)
        self.builtins.update(Expression.aggregates)
        # Inject user-defined aliases
        self.parent = parent

        # Add refset
        if not ref_set:
            parent_rs = parent and parent.ref_set
            ref_set = ReferenceSet(view.table, table_alias=table_alias,
                                   parent=parent_rs)
        self.ref_set = ref_set

    def _sub_select(self, *items):
        select = items[0]
        tail = ' '.join(items[1:])
        from_ = 'FROM "%s"' % (self.ref_set.table_alias)
        joins = ' '.join(self.ref_set.get_sql_joins())

        items = (select, from_, joins, tail)
        return ' '.join(it for it in items if it)

    def base_env(self, table, ref_set=None):
        env = {}
        for field in self.view.fields:
            env[field.name] = field
        return env

    def parse(self, exp):
        lexer = shlex.shlex(exp)
        lexer.wordchars += '.!=<>:{}-'
        tokens = list(lexer)
        ast = self.read(tokens)
        return ast

    def read(self, tokens, top_level=True, first=False):
        if len(tokens) == 0:
            raise SyntaxError('unexpected EOF while reading')
        token = tokens.pop(0)
        if token == '(':
            L = []
            exp = self
            if tokens[0].upper() == 'FROM':
                from_ = tokens.pop(0)  # pop off 'from'
                table = tokens.pop(0)
                exp = Expression(View(table), parent=self)
                L.append(ExpressionSymbol(from_, exp, first=True))
            first = True
            while tokens[0] != ')':
                L.append(exp.read(tokens, top_level=False, first=first))
                first = False
            tokens.pop(0)  # pop off ')'
            if tokens and top_level:
                raise ValueError('Unexpected tokens after ending ")"')
            return AST(L, exp)
        elif token == ')':
            raise SyntaxError('unexpected )')
        elif token in self.env:
            desc = self.env[token].desc
            if desc != token and desc[0] == '(':
                return self.parse(desc)

        return self.atom(token, first=first)

    def atom(self, token, first=False):
        '''
        Parse the token and try to identify it as param, int, float or
        symbol. The 'first' argument tells if the token if the first item
        in the expression (aka just after a '(').
        '''
        for q in ('"', "'"):
            if token[0] == q and token[-1] == q:
                return token[1:-1]

        if len(token) > 1 and token[0] == '{' and token[-1] == '}':
            return ExpressionParam(token[1:-1])

        try:
            return int(token)
        except ValueError:
            pass
        try:
            return float(token)
        except ValueError:
            return ExpressionSymbol(token, self, first=first)


class ExpressionSymbol:

    def __init__(self, token, exp, first=False):
        self.token = token
        self.params = []
        self.ref = None
        self.builtin = None
        ref = None
        if self.token.startswith('_parent.'):  # XXX replace with '_.' ?
            tail = self.token
            parent = exp
            while tail.startswith('_parent.'):
                head, tail = tail.split('.', 1)
                parent = parent.parent
            try:
                ref = parent.ref_set.add(tail)
            except KeyError:
                pass
        elif first:
            self.builtin = exp.builtins.get(self.token.lower(), self.token)
            return
        elif self.token in exp.env:
            val = exp.env[self.token]
            ref = exp.ref_set.add(val.desc)
        else:
            try:
                ref = exp.ref_set.add(self.token)
            except KeyError:
                pass

        if not ref:
            raise ValueError('"%s" not understood' % self.token)
        self.ref = ref

    def eval(self):
        if self.ref:
            return '"%s"."%s"' % (self.ref.join_alias, self.ref.remote_field)
        return self.builtin

    def __repr__(self):
        return '<ExpressionSymbol "%s">' % self.token


class ExpressionParam:

    def __init__(self, token):
        self.token = token
        self.key = ''
        self.tail = ''

        self.fmt_spec = self.conversion = None
        if ':' in token:
            token, self.fmt_spec = token.split(':', 1)

        if '!' in token:
            token, self.conversion = token.split('!', 1)

        dotted = token.split('.', 1)
        self.key, self.tail = dotted[0], dotted[1:]

    def eval(self, ast, env):
        # Get value from env
        try:
            as_int = int(self.key)
        except ValueError:
            as_int = None

        if self.key == '':
            value = ast.args.pop(0)
        elif as_int is not None:
            value = ast.args[as_int]
        else:
            value = ast.kwargs[self.key] \
                    if self.key in ast.kwargs else env[self.key]

        # Resolve dotted expression
        for attr in self.tail:
            if isinstance(value, dict):
                value = value[attr]
            else:
                value = getattr(value, attr)

        # Formating
        if self.fmt_spec:
            value = ast.formatter.format_field(value, self.fmt_spec)
        if self.conversion:
            value = ast.formatter.convert_field(value, self.conversion)
        return value


class AST(object):

    formatter = Formatter()

    def __init__(self, atoms, exp):
        self.atoms = atoms
        self.exp = exp
        self.params = []
        self.args = []
        self.kwargs = {}

    def eval(self, args=None, kwargs=None, params=None):
        self.params = params if params is not None else self.params
        self.args = args if args else self.args
        self.kwargs = kwargs or self.kwargs

        # Eval ast wrt to env
        res = self._eval(self.atoms, self.exp.env)
        return res

    def _eval(self, atom, env):
        if isinstance(atom, ExpressionSymbol):
            return atom.eval()

        elif isinstance(atom, ExpressionParam):
            value = atom.eval(self, env)
            return self.emit_literal(value)

        elif isinstance(atom, AST):
            return atom.eval(self.args, self.kwargs, self.params)

        elif not isinstance(atom, list):
            return self.emit_literal(atom)

        else:
            head = atom.pop(0)
            head = self._eval(head, env)
            params = []
            for x in atom:
                val = self._eval(x, env)
                params.append(val)
            if callable(head):
                head = head(*params)
            return head

    def emit_literal(self, x):
        # Collect literal and return placeholder
        if isinstance(x, (tuple, list, set)):
            self.params.extend(x)
            return ', '.join('%s' for _ in x)
        self.params.append(x)
        return '%s'

    def __repr__(self):
        return '<AST [%s]>' % ' '.join(map(str, self.atoms))


def connect(cfg=None, action=None):
    pool = Pool.get_pool(cfg or {})
    if not action:
        return pool
    if action == 'enter':
        return pool.__enter__()
    elif action == 'exit':
        return pool.__exit__(None, None, None)
    else:
        ValueError('Unexpected value "%s" for action parameter' % action)


def yaml_load(stream):
    import yaml

    class OrderedLoader(yaml.Loader):
        pass

    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return OrderedDict(loader.construct_pairs(node))
    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load(stream, OrderedLoader)


def cli():
    parser = argparse.ArgumentParser(description='Tanker CLI')
    parser.add_argument('action', help='info, read, write, init or delete',
                        nargs=1)
    parser.add_argument('table', help='Table to query',
                        nargs='*')
    parser.add_argument('--config', help='Config file (defaults to ".tk.yaml")',
                        default='.tk.yaml')
    parser.add_argument('-l', '--limit', help='Limit number of results',
                        type=int)
    parser.add_argument('-o', '--offset', help='Offset results',
                        type=int)
    parser.add_argument('-F', '--filter', action='append', help='Add filter',
                        default=[])
    parser.add_argument('-p', '--purge', help='Purge table after write',
                        action='store_true')
    parser.add_argument('-s', '--sort', action='append', help='Sort results',
                        default=[])
    parser.add_argument('-f', '--file', help='Read/Write to file '
                        '(instead of stdin/stdout)')
    parser.add_argument('--yaml', help='Enable YAML input / ouput '
                        '(defaults to csv)', action='store_true')
    parser.add_argument('-d', '--debug', help='Enable debugging',
                        action='store_true')

    args = parser.parse_args()
    if args.debug:
        logger.setLevel('DEBUG')
    cfg = yaml_load(open(args.config))
    cfg['schema'] = yaml_load(open(os.path.expanduser(cfg['schema'])))

    with connect(cfg):
        cli_main(args)


def cli_input_data(args):
    fields = args.table[1:] or None
    fh = None
    if args.file:
        fh = open(args.file)
    elif args.action in ('write', 'delete'):
        fh = sys.stdin
    if not fh:
        return fields, None

    if args.yaml:
        data = yaml_load(fh)
    else:
        reader = csv.reader(fh)
        data = list(reader)

    # If not field is given we infer them from the data
    if not fields and data:
        if args.yaml:
            fields = data[0].keys()
        else:
            fields = data[0]
            data = data[1:]

    return fields, data


def cli_main(args):
    action = args.action[0]
    table = args.table[0] if args.table else None
    order = map(lambda x: x.split(':') if ':' in x else x, args.sort)
    fields, data = cli_input_data(args)

    if action == 'read':
        view = View(table, fields)
        res = view.read(
            args.filter,
            order=list(order),
            limit=args.limit,
            offset=args.offset,
        )
        if args.file:
            fh = open(args.file, 'w')
        else:
            fh = sys.stdout
        if args.yaml:
            import yaml
            fh.write(yaml.dump(
                list(res.dict()),
                default_flow_style=False)
            )
        else:
            writer = csv.writer(fh)
            writer.writerow([f.name for f in view.fields])
            writer.writerows(res)

    elif action == 'delete':
        View(table, fields).delete(filters=args.filter, data=data)

    elif action == 'write':
        # Extract data
        fields, data = cli_input_data(args)
        View(table, fields).write(data, purge=args.purge)

    elif action == 'info':
        if table:
            columns = sorted(Table.get(table).columns, key=lambda x: x.name)
            for col in columns:
                if col.ctype in ('M2O', 'O2M'):
                    details = '%s -> %s' % (col.ctype, col.fk)
                else:
                    details = col.ctype
                print('%s (%s)' % (col.name, details))
        else:
            for name in sorted(ctx.registry):
                print(name)

    elif action == 'init':
        create_tables()

    else:
        print('Action "%s" not supported' % action)


if __name__ == '__main__':
    cli()
