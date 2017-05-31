from collections import OrderedDict, defaultdict
from contextlib import contextmanager
from datetime import datetime, date, timedelta
from itertools import chain
from string import Formatter
from threading import Thread
try:
    # PY2
    from urlparse import urlparse
except ImportError:
    # PY3
    from urllib.parse import urlparse
import csv
import io
import logging
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
    BuffIO = io.BytesIO
else:
    BuffIO = io.StringIO
if not PY2:
    basestring = (str, bytes)

__version__ = '0.5'

COLUMN_TYPE = ('TIMESTAMP', 'DATE', 'FLOAT', 'INTEGER', 'BIGINT', 'M2O', 'O2M',
               'VARCHAR', 'BOOL')
QUOTE_SEPARATION = re.compile(r"(.*?)('.*?')", re.DOTALL)
NAMED_RE = re.compile(r"%\(([^\)]+)\)s")
EPOCH = datetime(1970, 1, 1)


fmt = '%(levelname)s:%(asctime).19s: %(message)s'
logging.basicConfig(format=fmt)
logger = logging.getLogger('tanker')
logger.setLevel(logging.INFO)


def join(value, items):
    '''
    like str.join but for lists
    '''
    if not items:
        return
    it = iter(items)
    yield next(it)
    for item in it:
        yield value
        yield item

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

        if self.flavor == 'sqlite':
            self.conn_args = [dbname]
            self.conn_kwargs = {
                'check_same_thread': False,
                'detect_types': sqlite3.PARSE_DECLTYPES,
            }

        elif self.flavor == 'postgresql':
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

            pool_size = cfg.get('pg_pool_size', 10)
            self.pg_pool = ThreadedConnectionPool(1, pool_size, con_info)

        else:
            raise ValueError('Unsupported scheme "%s" in uri "%s"' % (
                uri.scheme, uri))

    @contextmanager
    def get_context(self):
        if self.flavor == 'sqlite':
            connection = sqlite3.connect(*self.conn_args, **self.conn_kwargs)
            connection.text_factory = str
            connection.execute('PRAGMA foreign_keys=ON')
        elif self.flavor == 'postgresql':
            connection = self.pg_pool.getconn()
        else:
            raise ValueError('Unexpected flavor "%s"' % self.flavor)

        new_ctx = CTX_STACK.push(connection, self)

        # Load schema as yaml if a string is given
        schema = self.cfg and self.cfg.get('schema')
        if isinstance(schema, basestring):
            schema = yaml_load(schema)
        # Makes new_ctx init the pool registry if still empty
        if not self.registry and schema:
            for table_def in schema:
                new_ctx.register(table_def)

        try:
            yield new_ctx
            connection.commit()
        except:
            connection.rollback()
            raise
        finally:
            CTX_STACK.pop()
            if self.flavor == 'postgresql':
                self.pg_pool.putconn(connection)

    @classmethod
    def disconnect(cls):
        for pool in cls._pools.values():
            if pool.flavor == 'postgresql':
                pool.pg_pool.closeall()

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
        self._local.contexts.pop()

    def active_context(self):
        return self._local.contexts[-1]


class ShallowContext:

    def __getattr__(self, name):
        return getattr(CTX_STACK.active_context(), name)


class Context:

    def __init__(self, connection, pool):
        self.flavor = pool.flavor
        self.encoding = pool.cfg.get('encoding', 'utf-8')
        self.connection = connection
        if self.flavor == 'postgresql':
            self.legacy_pg = connection.server_version < 90500
        self.cursor = connection.cursor()
        self.cfg = pool.cfg
        self.aliases = {'null': None}
        self._fk_cache = {}
        self.db_tables = set()
        self.db_fields = set()
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
        new_ctx.db_fields = self.db_fields
        new_ctx.db_tables = self.db_tables
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
              index=table_def.get('index'),
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
        if key not in self._fk_cache:
            read_fields = []
            for field in fields:
                _, desc = field.desc.split('.', 1)
                read_fields.append(desc)

            view = View(remote_table, read_fields + ['id'])
            res = dict((val[:-1], val[-1])
                       for val in view.read(disable_acl=True))
            self._fk_cache[key] = res

        for val in zip(*values):
            res = self._fk_cache[key].get(val)
            if res is None:
                raise ValueError('Values (%s) are not known in table "%s"' % (
                    ', '.join(map(repr, val)), remote_table))
            yield res

    def create_tables(self):
        # Collect table info
        if self.flavor == 'sqlite':
            qr = "SELECT name FROM sqlite_master WHERE type = 'table'"
        elif self.flavor == 'postgresql':
            schema = ctx.cfg.get('pg_schema', 'public')
            qr = "SELECT table_name FROM information_schema.tables " \
                 "WHERE table_schema = '%s'" % schema
        self.db_tables.update(name for name, in execute(qr))

        # Create tables and simple columns
        for table in self.registry.values():
            if table.name in self.db_tables:
                continue
            if self.flavor == 'sqlite':
                id_type = 'INTEGER'
            elif self.flavor == 'postgresql':
                id_type = 'SERIAL'

            col_defs = ['id %s PRIMARY KEY' % id_type]
            for col in table.columns:
                if col.ctype in ('M2O', 'O2M') or col.name == 'id':
                    continue
                col_defs.append('%s %s' % (col.name, col.sql_definition()))

            qr = 'CREATE TABLE "%s" (%s)' % (table.name, ', '.join(col_defs))
            execute(qr)
            self.db_tables.add(table.name)
            logger.info('Table "%s" created', table.name)

        # Add M2O columns
        for table_name in self.db_tables:
            if self.flavor == 'sqlite':
                qr = 'PRAGMA table_info("%s")' % table_name
                execute(qr)
                current_cols = [x[1] for x in self.cursor]
            elif self.flavor == 'postgresql':
                qr = "SELECT column_name FROM information_schema.columns "\
                     "WHERE table_name = '%s' " % table_name
                execute(qr)
                current_cols = [x[0] for x in self.cursor]

            self.db_fields.update(
                '%s.%s' % (table_name, c) for c in current_cols)

            if table_name not in self.registry:
                continue

            # Add M2O columns
            table = self.registry[table_name]
            for col in table.columns:
                if col.ctype != 'M2O':
                    continue
                if col.name in current_cols:
                    continue
                qr = 'ALTER TABLE %(table)s '\
                     'ADD COLUMN "%(name)s" %(def)s'
                params = {
                    'table': table.name,
                    'name': col.name,
                    'def': col.sql_definition(),
                }
                execute(qr % params)

        # Create indexes
        if self.flavor == 'sqlite':
            qr = "SELECT name FROM sqlite_master WHERE type = 'index'"
        elif self.flavor == 'postgresql':
            schema = ctx.cfg.get('pg_schema', 'public')
            qr = "SELECT indexname FROM pg_indexes " \
                 "WHERE schemaname = '%s'" % schema

        indexes = set(name for name, in execute(qr))

        for table in self.registry.values():
            if not table.index:
                continue

            idx = 'unique_index_%s' % table.name
            if idx in indexes:
                continue

            cols = ', '.join('"%s"' % c for c in table.index)
            qr = 'CREATE UNIQUE INDEX "%s" ON "%s" (%s)' % (
                idx, table.name, cols)
            execute(qr)

        # Add unique constrains (not supported by sqlite)
        if self.flavor != 'sqlite':
            qr = 'SELECT constraint_name '\
                 'FROM information_schema.table_constraints'
            db_cons = set(name for name, in execute(qr))

            unique_qr = 'ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)'
            for table in self.registry.values():
                for cols in table.unique:
                    cons_name = 'unique_' + '_'.join(cols)
                    if len(cons_name) > 63:
                        msg = 'Constrain name "%s" is too big'
                        ValueError(msg % cons_name)
                    if cons_name in db_cons:
                        continue
                    cons_cols = ', '.join(cols)
                    execute(unique_qr % (table.name, cons_name, cons_cols))

        # Add pre-defined data
        for table in self.registry.values():
            if not table.values:
                continue
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
    try:
        if params:
            ctx.cursor.execute(query, params)
        else:
            ctx.cursor.execute(query)
    except DB_EXCEPTION as e:
        log_sql(query, params, exception=True)
        raise DBError(e)
    return ctx.cursor


def executemany(query, params):
    query = ctx._prepare_query(query)
    log_sql(query, params)
    try:
        ctx.cursor.executemany(query, params)
    except DB_EXCEPTION as e:
        log_sql(query, params, exception=True)
        raise DBError(e)
    return ctx.cursor


def copy_from(buff, table, **kwargs):
    log_sql('"COPY FROM" called on table %s' % table)
    cursor = ctx.cursor
    cursor.copy_from(buff, table, **kwargs)
    return cursor


def create_tables():
    ctx.create_tables()


def fetch(tablename, filter_by):
    view = View(tablename)
    values = next(view.read(filters=filter_by), None)
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


class View(object):

    def __init__(self, table, fields=None):
        self.ctx = ctx
        self.table = Table.get(table)
        if fields is None:
            fields = [(f.name, f.name) for f in self.table.own_columns]
        elif isinstance(fields, basestring):
            fields = [[fields, fields]]
        elif isinstance(fields, dict):
            fields = fields.items()
        elif isinstance(fields, list) and isinstance(fields[0], basestring):
            fields = zip(fields, fields)
        elif isinstance(fields, list) and isinstance(fields[0], tuple):
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
            if self.field_map[view_field.col] and view_field.col.ctype != 'M2O':
                raise ValueError(
                    'Column %s is specified several time in view'
                    % view_field.col.name)
            self.field_map[view_field.col].append(view_field)
            self.field_idx[view_field.col].append(idx)
            idx += 1

        # Index fields identify each line in the data
        self.index_fields = [f for f in self.fields
                             if f.col and f.col.name in self.table.index]
        # Index cols identify each row in the table
        self.index_cols = [c.name for c in self.field_map
                           if c.name in self.table.index]

    def get_field(self, name):
        return self.field_dict.get(name)

    def _build_filter_cond(self, exp, *filters):
        for fltr in filters:
            if not fltr:
                continue

            # filters can be a dict
            if isinstance(fltr, dict):
                # Add simple equal conditions
                for key, val in fltr.items():
                    ast = exp.parse('(= %s {}) ' % key)
                    ast.args = [val]
                    yield ast
                continue

            # Filters can be a query string or a list of query string
            if isinstance(fltr, basestring):
                fltr = [fltr]
            # Parse expression filters
            for line in fltr:
                ast = exp.parse(line)
                yield ast

    def read(self, filters=None, args=None, order=None, groupby=None,
             limit=None, disable_acl=False):

        if isinstance(filters, basestring):
            filters = [filters]

        acl_filters = None
        acl = self.ctx.cfg.get('acl_rules', {}).get(self.table.name)
        if acl and not disable_acl:
            acl_filters = acl['filters']

        exp = Expression(self)


        # Add select fields
        select_chunk = [exp.parse(
            '(select %s)' % ' '.join(f.desc for f in self.fields))]
        select_chunk.append(' FROM %s' % self.table.name)

        # Add filters
        filter_chunks = list(self._build_filter_cond(exp, filters, acl_filters))
        if filter_chunks:
            filter_chunks = ['WHERE'] + list(join(' AND ', filter_chunks))

        # ADD group by
        groupby_chunks = []
        group_fields = []
        if groupby:
            if isinstance(groupby, basestring):
                groupby = [groupby]
            group_fields = [exp.parse(f).eval() for f in groupby]
            groupby_chunks = ['GROUP BY ' + ','.join(group_fields)]

        if order:
            order_chunks = ['ORDER BY']
            if isinstance(order, (str, tuple)):
                order = [order]
            for item in order:
                if isinstance(item, (list, tuple)):
                    item, how = item
                else:
                    how = None
                if how:
                    if how.upper() not in ('ASC', 'DESC'):
                        msg = 'Unexpected value "%s" for sort direction' % how
                        raise ValueError(msg)
                    ptrn = '%%s.%%s %s' % how
                else:
                    ptrn = '%s.%s'

                field = self.get_field(item)
                if field is None:
                    ref = exp.ref_set.add(item)
                else:
                    ref = exp.ref_set.add(field.desc)
                order_chunks += [
                    ptrn % (ref.join_alias, ref.remote_field)]
        else:
            order_chunks = []

        join_chunks = [exp.ref_set]
        all_chunks = (select_chunk + join_chunks + filter_chunks
                      + groupby_chunks + order_chunks)

        if limit is not None:
            all_chunks += ['LIMIT %s' % int(limit)]

        return TankerCursor(self, all_chunks, args=args)

    def format(self, data):
        for col in self.field_map:
            idx = self.field_idx[col]
            if col.ctype == 'M2O':
                fields = tuple(f for f in self.field_map[col])
                values = tuple(data[i] for i in idx)
                if len(fields) == 1 and fields[0].ctype == 'INTEGER':
                    # Handle update of fk by id
                    yield map(int, data[idx[0]])
                else:
                    # Resole foreign key reference
                    values = map(
                        lambda a: tuple(a[0].col.format(a[1], astype=a[0].ctype)),
                        zip(fields, values)
                        )
                    yield ctx.resolve_fk(fields, values)
            else:
                yield col.format(data[idx[0]])

    def delete(self, filters=None, data=None, args=None):
        if not any((data, filters)):
            raise ValueError('No deletion criteria given')

        if data and filters:
            raise ValueError('Deletion by both data and filter not supported')

        exp = Expression(self)
        filter_chunks = list(self._build_filter_cond(exp, filters))

        if data:
            with self._prepare_write(data) as join_cond:
                qr = 'DELETE FROM %(main)s WHERE id IN (' \
                     'SELECT %(main)s.id FROM %(main)s ' \
                     'INNER JOIN tmp on %(join_cond)s)'
                qr = qr % {
                    'main': self.table.name,
                    'join_cond': ' AND '.join(join_cond),
                }
                execute(qr)

        else:
            qr = ('DELETE FROM %(main_table)s WHERE id IN ('
                  'SELECT %(main_table)s.id FROM %(main_table)s ')
            qr = qr % {'main_table': self.table.name}
            chunks = [qr, exp.ref_set]
            if filter_chunks:
                chunks += ['WHERE'] + filter_chunks
            chunks.append(')')
            cur = TankerCursor(self, chunks, args=args).execute()
            return cur.rowcount

    @contextmanager
    def _prepare_write(self, data):
        # Create tmp
        not_null = lambda n: 'NOT NULL' if n in self.index_fields else ''
        qr = 'CREATE TEMPORARY TABLE tmp (%s)'
        qr = qr % ', '.join('"%s" %s %s' % (
            col.name,
            fields[0].ftype,
            not_null(col.name))
            for col, fields in self.field_map.items())
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
            copy_from(buff, 'tmp', null='')

        else:
            qr = 'INSERT INTO tmp (%(fields)s) VALUES (%(values)s)'
            qr = qr % {
                'fields': ', '.join('"%s"' % c.name for c in self.field_map),
                'values': ', '.join('%s' for _ in self.field_map),
            }
            executemany(qr, data)

        # Create join conditions
        join_cond = []
        for name in self.index_cols:
            join_cond.append('tmp."%s" = "%s"."%s"' % (
                name, self.table.name, name))

        yield join_cond

        # Clean tmp table
        execute('DROP TABLE tmp')

    def write(self, data, purge=False, insert=True, update=True):
        '''
        Write given data to view table. If insert is true, new lines will
        be inserted.  if update is true, existing line will be
        updated. If purge is true existing line that are not present
        in data will be deleted.
        '''

        # Handle list of dict and dataframes
        if isinstance(data, list) and isinstance(data[0], dict):
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
        if self.ctx.flavor == 'sqlite':
            # Convert back to lines:
            data = list(zip(*data))
            rowcounts = self._sqlite_upsert(data, purge=purge, insert=insert,
                                            update=update)
        else:
            with self._prepare_write(data) as join_cond:
                rowcounts = {}
                if ctx.legacy_pg:
                    if insert:
                        cnt = self._insert(join_cond)
                        rowcounts['insert'] = cnt
                    if update:
                        cnt = self._update(join_cond)
                        rowcounts['update'] = cnt
                else:
                    # ON-CONFLICT is available since postgres 9.5
                    cnt = self._pg_upsert(join_cond, insert=insert, update=update)
                    rowcounts['upsert'] = cnt

                if purge:
                    cnt = self._purge(join_cond)
                    rowcounts['delete'] = cnt

        # Clean cache for current table
        self.ctx.reset_cache(self.table.name)
        return rowcounts

    def _sqlite_upsert(self, data, purge, insert, update):
        # Identify position and name of index fields
        key_pos = []
        key_cols = []
        upd_pos = []
        upd_fields = []
        for pos, col in enumerate(self.field_map):
            if col.name in self.index_cols:
                key_pos.append(pos)
                key_cols.append(col.name)
            else:
                upd_pos.append(pos)
                upd_fields.append(col.name)

        # Read existing data from table
        view = View(self.table.name, key_cols)
        db_keys = set(view.read())
        key_vals = lambda row: tuple(row[i] for i in key_pos)
        upd_vals = lambda row: tuple(row[i] for i in upd_pos)

        # Build sql statements
        insert_qr = 'INSERT INTO %(table)s (%(fields)s) VALUES (%(values)s)'
        insert_qr = insert_qr % {
            'table': self.table.name,
            'fields': ', '.join('"%s"' % c.name for c in self.field_map),
            'values': ', '.join('%s' for _ in self.field_map),
        }
        update_qr = 'UPDATE %(table)s SET %(upd_stm)s WHERE %(cond)s'
        update_qr = update_qr % {
            'table': self.table.name,
            'upd_stm': ', '.join('%s = %%s' % c for c in upd_fields),
            'cond': ' AND '.join('%s = %%s' % c for c in key_cols),
        }
        delete_qr = 'DELETE FROM %(table)s WHERE %(cond)s'
        delete_qr = delete_qr % {
            'table': self.table.name,
            'cond': ' AND '.join('%s = %%s' % c for c in key_cols),
        }

        # Run queries
        cnt = {'insert': 0, 'update': 0, 'deleted': 0}
        data_keys = [key_vals(line) for line in data]
        for key, line in zip(data_keys, data):
            if insert and key not in db_keys:
                cur = execute(insert_qr, line)
                cnt['insert'] +=  cur.rowcount
            if update and key in db_keys:
                vals = upd_vals(line)
                if vals:
                    cur = execute(update_qr, vals + key)
                    cnt['update'] +=  cur.rowcount
        if purge:
            to_delete = db_keys - set(data_keys)
            if to_delete:
                cnt['deleted'] = len(to_delete)
                executemany(delete_qr, to_delete)

        return cnt

    def _pg_upsert(self, join_cond, insert, update):
        tmp_fields = ', '.join('tmp."%s"' % f.name for f in self.field_map)
        main_fields = ', '.join('"%s"' % f.name for f in self.field_map)
        upd_fields = []
        for f in self.field_map:
            if f.name in self.index_cols:
                continue
            upd_fields.append('"%s" = EXCLUDED."%s"' % (f.name, f.name))

        qr = (
            'INSERT INTO %(main)s (%(main_fields)s) '
            'SELECT %(tmp_fields)s FROM tmp '
            '%(join_type)s JOIN %(main_table)s ON ( %(join_cond)s) ')
        if upd_fields and update:
            qr += 'ON CONFLICT (%(idx)s) DO UPDATE SET %(upd_fields)s'
        else:
            qr += 'ON CONFLICT (%(idx)s) DO NOTHING'

        qr = qr % {
            'main': self.table.name,
            'main_fields': main_fields,
            'tmp_fields': tmp_fields,
            'main_table': self.table.name,
            'join_cond': ' AND '.join(join_cond),
            'join_type': 'LEFT' if insert else 'INNER',
            'upd_fields': ', '.join(upd_fields),
            'idx': ', '.join(self.index_cols),
        }
        cur = TankerCursor(self, qr).execute()
        return cur.rowcount

    def _insert(self, join_cond):
        qr = 'INSERT INTO %(main)s (%(fields)s) %(select)s'
        select = 'SELECT %(tmp_fields)s FROM tmp '\
                 'LEFT JOIN %(main_table)s ON ( %(join_cond)s) ' \
                 'WHERE %(where_cond)s'

        # Consider only new rows
        where_cond = []
        for name in self.index_cols:
            where_cond.append('%s."%s" IS NULL' % (self.table.name, name))

        tmp_fields = ', '.join('tmp."%s"' % f.name for f in self.field_map)
        select = select % {
            'tmp_fields': tmp_fields,
            'main_table': self.table.name,
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
        update_cols = [c.name for c in self.field_map
                       if c.name not in self.table.index]
        if not update_cols:
            return 0

        where = ' AND '.join(join_cond)
        qr = 'UPDATE "%(main)s" SET '
        qr += ', ' .join('"%s" = tmp."%s"' % (n, n) for n in update_cols)
        qr += 'FROM tmp WHERE %(where)s'
        qr = qr % {
            'main': self.table.name,
            'where': where,
        }
        cur = TankerCursor(self, qr).execute()

        return cur and cur.rowcount or 0

    def _purge(self, join_cond):
        qr = 'DELETE FROM %(main)s WHERE id IN (' \
             'SELECT %(main)s.id FROM %(main)s ' \
             'LEFT JOIN tmp on %(join_cond)s ' \
             'WHERE tmp.%(field)s IS NULL)'
        qr = qr % {
            'main': self.table.name,
            'join_cond': ' AND '.join(join_cond),
            'field': self.index_cols[0]
        }
        cur = TankerCursor(self, qr).execute()
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
            self._args = args
            self._kwargs = None

    def args(self, *args, **kwargs):
        '''
        Set args for current cursor
        '''
        self._args = args
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

    def next(self):
        return next(iter(self))

    def all(self):
        return list(self)

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

    def __init__(self, name, columns, index=None, unique=None, values=None):
        self.name = name
        self.columns = columns[:]
        self.unique = unique or []
        self.values = values
        # Add implicit id column
        if 'id' not in [c.name for c in self.columns]:
            self.columns.append(Column('id', 'INTEGER'))
        self.own_columns = [c for c in self.columns \
                            if c.name != 'id' and c.ctype != 'O2M']

        if index is None:
            if len(self.columns) == 2:
                # If there is only one column (other then id), use it
                # as index
                index = tuple(c.name for c in self.columns if c.name != 'id')
            else:
                raise ValueError('No index defined on %s' % name)
        self.index = [index] if isinstance(index, basestring) else index
        self._column_dict = dict((col.name, col) for col in self.columns)
        ctx.registry[name] = self

        for col in self.index:
            if col not in self._column_dict:
                raise ValueError('Index column "%s" does not exist' % col)

    def get_column(self, name):
        try:
            return self._column_dict[name]
        except KeyError:
            raise KeyError('Column "%s" not found in table "%s"' % (
                name, self.name))

    def get_foreign_values(self, desc):
        rel_name, field = desc.split('.')
        rel = self.get_column(rel_name)
        foreign_table = rel.get_foreign_table()
        view = View(foreign_table.name, [field])
        return [x[0] for x in view.read()]

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
        weight = 0

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
            ctype, self.fk = ctype.split()
            self.foreign_table, self.foreign_col = self.fk.split('.')
        else:
            self.fk = None
            self.foreign_table = self.foreign_col = None
        self.name = name
        self.ctype = ctype.upper()
        self.default = default
        if self.ctype not in COLUMN_TYPE:
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
        return 'INTEGER REFERENCES "%s" (%s) ON DELETE CASCADE' % (
            self.foreign_table, self.foreign_col)

    def get_foreign_table(self):
        return Table.get(self.foreign_table)

    def format(self, values, astype=None):
        '''
        Sanitize value wrt the column type of the current field.
        '''
        skip_none = (lambda fn: (
            lambda x: None
            if x is None or (pandas and pandas.isnull(x))
            else fn(x)))
        astype = astype or self.ctype
        res = []

        if astype in ('INTEGER', 'BIGINT'):
            res = map(skip_none(int), values)

        elif astype == 'VARCHAR':
            for value in values:
                if not value:
                    value = None
                elif not isinstance(value, basestring):
                    value = str(value)
                else:
                    if PY2 and isinstance(value, unicode):
                        value =  value.encode(ctx.encoding)
                    elif not PY2 and isinstance(value, bytes):
                        value = value.encode(ctx.encoding)
                res.append(value)

        elif astype == 'TIMESTAMP':
            for value in values:
                if not value:
                    value = None
                elif not isinstance(value, datetime):
                    if hasattr(value, 'timetuple'):
                        value = datetime(*value.timetuple()[:6])
                    elif hasattr(value, 'tolist'):
                        # tolist is a numpy.datetime64 method that
                        # returns nanosecond from 1970. EPOCH + delta(val)
                        # suppors values far in the past (or future)
                        ts = value.tolist()
                        if ts is None:
                            value = None
                        else:
                            value = EPOCH + timedelta(seconds=ts/1e9)
                    else:
                        raise ValueError(
                            'Unexpected value "%s" for type "%s"' % (
                                value, astype))
                res.append(value)

        elif astype == 'DATE':
            for value in values:
                if value is None:
                    pass
                elif not isinstance(value, date):
                    if hasattr(value, 'timetuple'):
                        value = date(*value.timetuple()[:3])
                    elif hasattr(value, 'tolist'):
                        ts = value.tolist()
                        if ts is None:
                            value = None
                        else:
                            dt = EPOCH + timedelta(seconds=ts/1e9)
                            value = date(*dt.timetuple()[:3])
                    else:
                        raise ValueError(
                            'Unexpected value "%s" for type "%s"' % (
                                value, astype))
                res.append(value)
        else:
            res = values

        return res

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
            yield 'LEFT JOIN %s AS %s ON (%s.%s = %s.%s)' % (
                right_table, alias, left_table, left_col, alias, right_col
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


class ExpressionSymbol:

    def __init__(self, token, exp):
        self.token = token
        self.params = []
        self.ref = None
        self.builtin = None

        if self.token.lower() in exp.builtins:
            self.builtin = exp.builtins[self.token.lower()]
            return

        ref = None
        if self.token.startswith('_parent.'): # XXX replace with '_.' ?
            tail = self.token
            parent = exp
            while tail.startswith('_parent.'):
                head, tail = tail.split('.', 1)
                parent = parent.parent
            try:
                ref = parent.ref_set.add(tail)
            except KeyError:
                pass
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
            return '%s.%s' % (self.ref.join_alias, self.ref.remote_field)
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


class Expression(object):
    # Inspired by http://norvig.com/lispy.html

    aggregates = (
        'avg',
        'count',
        'max',
        'min',
        'sum',
    )

    builtins = {
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
        'is': lambda x, y: '%s is %s' % (x, y),
        'isnot': lambda x, y: '%s is not %s' % (x, y),
        'null': 'null',
        '*': '*',
        'date': 'date',
        'varchar': 'varchar',
        'integer': 'integer',
        'bigint': 'bigint',
        'timestamp': 'timestamp',
        'bool': 'bool',
        'float': 'float',
        'not': lambda x: 'not %s' % x,
        'exists': lambda x: 'EXISTS (%s)' % x,
        'where': lambda *x: 'WHERE ' + ' AND '.join(x),
        'select': lambda *x: 'SELECT ' + ', '.join(x),
        'count': lambda *x: 'count(%s)' % ', '.join(x),
        'max': lambda *x: 'max(%s)' % x,
        'cast': lambda x, y: 'CAST (%s AS %s)' % (x, y),
    }

    def __init__(self, view, ref_set=None, parent=None):
        self.view = view
        # Populate env with view fields
        self.env = self.base_env(view.table)
        self.builtins = Expression.builtins.copy()
        self.builtins['from'] = self._sub_select
        # Inject user-defined aliases
        self.parent = parent

        # Add refset
        if not ref_set:
            parent_rs = parent and parent.ref_set
            ref_set = ReferenceSet(view.table, parent=parent_rs)
        self.ref_set = ref_set

    def _sub_select(self, *items):
        select = items[0]
        tail = ' '.join(items[1:])
        from_ = 'FROM %s' % (self.ref_set.table_alias)
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
        lexer.wordchars += '.!=<>:{}'
        ast = self.read(list(lexer))
        return ast

    def read(self, tokens, top_level=True):
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
                L.append(ExpressionSymbol(from_, exp))
            while tokens[0] != ')':
                L.append(exp.read(tokens, top_level=False))
            tokens.pop(0)  # pop off ')'
            if tokens and top_level:
                raise ValueError('Unexpected tokens after ending ")"')
            return AST(L, exp)
        elif token == ')':
            raise SyntaxError('unexpected )')
        else:
            return self.atom(token)

    def atom(self, token):
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
            return ExpressionSymbol(token, self)


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
        self.args = args[:] if args else self.args
        self.kwargs = kwargs or self.kwargs
        # Eval ast wrt to env
        res = self._eval(self.atoms, self.exp.env)
        # res = ' '.join(self._eval(atom, self.exp.env) for atom in self.atoms)
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
            proc = self._eval(head, env)
            params = []
            for x in atom:
                val = self._eval(x, env)
                params.append(val)
            res = proc(*params)
            return res

    def emit_literal(self, x):
        # Collect literal and return placeholder
        if isinstance(x, (tuple, list, set)):
            self.params.extend(x)
            return ', '.join('%s' for _ in x)
        self.params.append(x)
        return '%s'

    def __repr__(self):
        return '<AST [%s]>' % ' '.join(map(str, self.atoms))


def connect(cfg=None):
    pool = Pool.get_pool(cfg or {})
    return pool.get_context()


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
