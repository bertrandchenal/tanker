from collections import OrderedDict, defaultdict
from contextlib import contextmanager
from itertools import chain, cycle
from string import Formatter
from threading import Thread
from urlparse import urlparse
import csv
import datetime
import io
import logging
import re
import shlex
import sqlite3
import textwrap
import threading

try:
    import pandas
except ImportError:
    pandas = None

try:
    import psycopg2
except ImportError:
    psycopg2 = None

__version__ = '0.1.2'

COLUMN_TYPE = ('TIMESTAMP', 'DATE', 'FLOAT', 'INTEGER', 'M2O', 'O2M', 'VARCHAR',
               'BOOL')
QUOTE_SEPARATION = re.compile(r"(.*?)('.*?')", re.DOTALL)
NAMED_RE = re.compile(r"%\(([^\)]+)\)s")

fmt = '%(levelname)s:%(asctime).19s: %(message)s'
logging.basicConfig(format=fmt)
logger = logging.getLogger('tanker')
logger.setLevel(logging.INFO)


class TankerThread(Thread):

    def __init__(self, *args, **kwargs):
        self.parent_context = ctx.copy()
        super(TankerThread, self).__init__(*args, **kwargs)

    def run(self):
        CTX_STACK.reset([self.parent_context])
        super(TankerThread, self).run()

class ContextStack():

    def __init__(self):
        self._local = threading.local()
        self._local.contexts = []

    def reset(self, contexts):
        self._local.contexts = contexts

    def push(self, flavor=None, connection=None, cfg=None):
        new_ctx = Context(flavor=flavor, connection=connection, cfg=cfg)
        self._local.contexts.append(new_ctx)

        # TODO: provide a cache of the registry per uri (to avoid this loop)
        schema = cfg and cfg.get('schema')

        if not new_ctx.registry and schema:
            for table_def in schema:
                new_ctx.register(table_def)

        return new_ctx

    def pop(self):
        self._local.contexts.pop()

    def active_context(self):
        return self._local.contexts[-1]


class ShallowContext:

    def __getattr__(self, name):
        return getattr(CTX_STACK.active_context(), name)


class Context:

    def __init__(self, flavor, connection, cfg):
        self.flavor = flavor
        self.connection = connection
        self.cursor = connection.cursor()
        self.cfg = cfg
        self.aliases = {'null': None}
        self._fk_cache = {}
        self.db_tables = set()
        self.db_fields = set()
        self.registry = OrderedDict()

    def copy(self):
        '''
        Create a clone of self, will trigger instanciation of a new cursor
        (the connection is shared)
        '''
        new_ctx = Context(self.flavor, self.connection, self.cfg)
        new_ctx.aliases = self.aliases
        new_ctx.db_fields = self.db_fields
        new_ctx.db_tables = self.db_tables
        new_ctx.registry = self.registry
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
            new_col = Column(col_name, col_type, default=defaults.get(col_name))
            columns.append(new_col)
        # Instanciating the table adds it to REGISTRY
        Table(name=table_def['table'], columns=columns,
              values=values,
              index=table_def.get('index'),
              unique=table_def.get('unique'),
        )

    def reset_cache(self, table=None):
        if table is None:
            self._fk_cache = {}
        else:
            if table in self._fk_cache:
                del self._fk_cache[table]

    def resolve_fk(self, fields, values):
        remote_table = fields[0].col.get_foreign_table().name
        key = (remote_table,) + fields
        if key not in self._fk_cache:
            read_fields = []
            for field in fields:
                _, desc = field.desc.split('.', 1)
                read_fields.append(desc)
            view = View(remote_table, read_fields + ['id'])
            res = dict((val[:-1], val[-1]) for val in view.read(disable_acl=True))
            self._fk_cache[key] = res

        res = self._fk_cache[key].get(values)
        if res is None:
            raise ValueError('Values (%s) are not known in table "%s"' % (
                ', '.join(map(str, values)), remote_table))
        return res

    def create_tables(self):
        # Collect table info
        if self.flavor == 'sqlite':
            qr = "SELECT name FROM sqlite_master WHERE type = 'table'"
        elif self.flavor == 'postgresql':
            qr = "SELECT table_name FROM information_schema.tables " \
            "WHERE table_schema = 'public'"
        self.db_tables.update(name for name, in execute(qr))

        # Create tables and simple columns
        for table in self.registry.itervalues():
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
            qr = "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"

        indexes = set(name for name, in execute(qr))

        for table in self.registry.itervalues():
            if not table.index:
                continue

            idx = 'unique_index_%s' % table.name
            if idx in indexes:
                continue

            cols = ', '.join('"%s"' % c for c in table.index)
            qr = 'CREATE UNIQUE INDEX %s ON %s (%s)' % (idx, table.name, cols)
            execute(qr)

        # Add unique constrains (not supported by sqlite)
        if self.flavor != 'sqlite':
            qr = 'select constraint_name from information_schema.table_constraints'
            db_cons = set(name for name, in execute(qr))

            unique_qr = 'ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)';
            for table in self.registry.itervalues():
                for cols in table.unique:
                    cons_name = 'unique_' + '_'.join(cols)
                    if len(cons_name) > 63:
                        ValueError('Constrain name "%s" is too big' % cons_name)
                    if cons_name in db_cons:
                        continue
                    cons_cols = ', '.join(cols)
                    execute(unique_qr % (table.name, cons_name, cons_cols))

        # Add pre-defined data
        for table in self.registry.itervalues():
            if not table.values:
                continue
            view = View(table.name, fields=table.values[0].keys())
            view.write(table.values)


def log_sql(query, params=None):
    if logger.getEffectiveLevel() > logging.DEBUG:
        return
    indent = '  '
    query = textwrap.fill(query, initial_indent=indent,
                          subsequent_indent=indent)
    if params is None:
        logger.debug('SQL Query:\n%s', query)
    else:
        params = str(params)
        if len(params) > 1000:
            params = params[:1000] + '...'
        logger.debug('SQL Query:\n%s\nSQL Params:\n%s%s', query, indent, params)

CTX_STACK = ContextStack()
ctx = ShallowContext()


# Build tuple of backend exceptions we want to catch
DB_EXCEPTION = (sqlite3.OperationalError,)
if psycopg2 is not None:
    DB_EXCEPTION += (psycopg2.ProgrammingError,)

class DBError(Exception):
    pass

def execute(query, params=None, args=None, kwargs=None):
    query, extra_params = format_query(query, args, kwargs)
    params = tuple(merge_params(params, extra_params))
    log_sql(query, params)
    query = ctx._prepare_query(query)

    try:
        if params:
            ctx.cursor.execute(query, params)
        else:
            ctx.cursor.execute(query)
    except DB_EXCEPTION as e:
        raise DBError(e)
    return ctx.cursor

def executemany(query, params, args=None, kwargs=None):
    query, extra_params  = format_query(query, args, kwargs)
    params = tuple(merge_params(params, extra_params))

    query = ctx._prepare_query(query)
    log_sql(query, params)
    ctx.cursor.executemany(query, params)
    return ctx.cursor

def merge_params(params, extra_params):
    if not params:
        return
    extra_it = cycle(extra_params)
    for prm in params:
        if isinstance(prm, ExpressionParam):
            for value in next(extra_it):
                yield value
        else:
            yield prm

def format_query(qr, args=None, kwargs=None):
    if not (args or kwargs):
        return qr, tuple()
    args = args or []
    kwargs = kwargs or {}
    qr_list = []
    qr_params = []
    formatter = Formatter()
    tokens = formatter.parse(qr)
    pos = 0
    for literal_text, field_name, spec, conversion in tokens:
        if literal_text:
            qr_list.append(literal_text)
        if field_name is None:
            continue
        # Get value from arg or kwargs
        if field_name == '':
            values = [args[pos]]
            pos += 1
        else:
            if '.' in field_name:
                key = field_name.split('.', 1)[0]
            else:
                key = field_name
            values = list(format_arg(
                formatter, field_name, kwargs[key], spec))

        # Collect placeholders & values
        qr_list.append(', '.join('%s' for _ in values))
        qr_params.append(values)

    return ''.join(qr_list), tuple(qr_params)

def format_arg(formatter, key, value, spec):
    # Extract dots
    if '.' in key:
        attrs = key.split('.')
        key, attrs = attrs[0], attrs[1:]
    else:
        attrs = []

    # Eval dotted attributes
    for name in attrs:
        if isinstance(value, dict):
            value = value[name]
        else:
            value = getattr(value, name)

    # auto-magically expand lists
    if isinstance(value, (tuple, list)):
        for item in value:
            yield formatter.format_field(item, spec)

    else:
        yield formatter.format_field(value, spec)


def copy_from(buff, table, **kwargs):
    log_sql('"COPY FROM" called on table %s' % table)
    cursor = ctx.cursor
    cursor.copy_from(buff, table, **kwargs)
    return cursor

def create_tables():
    ctx.create_tables()

def fetch(tablename, filter_by):
    view = View(tablename)
    values = next(view.read(filter_by=filter_by), None)
    if values is None:
        return
    keys = (f.name for f in view.fields)
    return dict(zip(keys, values))

def save(tablename, data):
    fields = data.keys()
    view = View(tablename, fields)
    view.write([data])


class ViewField:

    def __init__(self, name, desc, table):
        self.name = name
        self.desc = desc
        self.ref = None
        self.ctx = ctx

        if '.' in desc:
            ftype = 'INTEGER'
            self.ref = ReferenceSet(table).get_ref(desc)
            remote_col = self.ref.remote_table.get_column(self.ref.remote_field)
            ctype = remote_col.ctype
            self.col = table.get_column(desc.split('.')[0])

        elif desc in self.ctx.aliases:
            ftype = ctype = 'LITERAL'
            self.value = self.ctx.aliases[desc]
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

    def __init__(self, table, fields=None, melt=False):
        self.ctx = ctx
        self.table = Table.get(table)
        if fields is None:
            fields = [(f.name, f.name) for f in self.table.columns \
                      if f.name != 'id']
        elif isinstance(fields, basestring):
            fields = [[fields, fields]]
        elif isinstance(fields, dict):
            fields = fields.items()
        elif isinstance(fields, list) and isinstance(fields[0], basestring):
            fields = zip(fields, fields)
        elif isinstance(fields, list) and isinstance(fields[0], tuple):
            fields = fields

        self.fields = [ViewField(name, desc, self.table) \
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
                    'Column %s is specified several time in view' \
                    % view_field.col.name)
            self.field_map[view_field.col].append(view_field)
            self.field_idx[view_field.col].append(idx)
            idx += 1

        # Index fields identify each line in the data
        self.index_fields = [f for f in self.fields \
                             if f.col and f.col.name in self.table.index]
        # Index fields identify each row in the table
        self.index_cols = [c.name for c in self.field_map \
                           if c.name in self.table.index]

    def get_field(self, name):
        return self.field_dict.get(name)

    def _build_filter_cond(self, filters=None, filter_by=None):
        where = []
        qr_params = tuple()
        ref_set = ReferenceSet(self.table)

        # filters can be a query string or a list of query string
        if isinstance(filters, basestring):
            filters = [filters]
        elif filters is None:
            filters = []
        # filter_by is a dict containing strict equality conditions
        filter_by = filter_by or {}
        # Parse expression filters
        for line in filters:
            fltr = Expression(self, ref_set)
            sql_cond = fltr.eval(line)
            where.append(sql_cond)
            qr_params = qr_params + tuple(fltr.params)

        # Add simple filter_by conditions
        for key, val in filter_by.items():
            ref = ref_set.add(key)
            field = '%s.%s' % (ref.join_alias, ref.remote_field)
            where.append('%s = %%s' % field)
            qr_params = qr_params + (val,)

        return where, qr_params, ref_set

    def read(self, filters=None, filter_by=None, disable_acl=False,
             order=None, limit=None, args=None):

        if isinstance(filters, basestring):
            filters = [filters]

        acl_rules = self.ctx.cfg.get('acl_rules')
        if acl_rules and not disable_acl:
            rule = acl_rules.get(self.table.name)
            if rule:
                filters = rule['filters'] + (filters or [])

        selects = []
        where, qr_params, ref_set = self._build_filter_cond(
            filters=filters, filter_by=filter_by)

        # Add select fields
        field_params = tuple()
        for f in self.fields:
            if f.ftype == 'LITERAL':
                field_params += (f.value,)
                selects.append("%%s as %s" % f.desc)
            else:
                ref = ref_set.add(f.desc)
                selects.append('%s.%s' % (ref.join_alias, ref.remote_field))
        qr_params = field_params + qr_params

        qr = 'SELECT %(selects)s FROM %(main_table)s'
        qr = qr % {
            'selects': ', '.join(selects),
            'main_table': self.table.name,
        }
        qr += ' ' + ' '.join(ref_set.get_sql_joins())

        if where:
            qr += ' WHERE ' + ' AND '.join(where)

        if order:
            order_by = []
            if isinstance(order, (basestring, tuple)):
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
                    ref = ref_set.add(item)
                else:
                    ref = ref_set.add(field.desc)
                order_by.append(ptrn % (ref.join_alias, ref.remote_field))

            qr += ' ORDER BY ' + ', '.join(order_by)

        if limit is not None:
            qr += ' LIMIT %s' % int(limit)
        return Cursor(qr, qr_params, args)

    def format_line(self, row, encoding=None):
        for col in self.field_map:
            idx = self.field_idx[col]
            if col.ctype == 'M2O':
                fields = tuple(f for f in self.field_map[col])
                values = tuple(row[i] for i in idx)
                if len(fields) == 1 and fields[0].ctype == 'INTEGER':
                    # Handle update of fk by id
                    yield int(row[idx[0]])
                else:
                    # Resole foreign key reference
                    yield ctx.resolve_fk(fields, values)
            else:
                yield col.format(row[idx[0]], encoding=encoding)

    @contextmanager
    def _prepare_write(self, data):
        # Create tmp
        not_null = lambda n: 'NOT NULL' if n in self.index_fields else ''
        qr = 'CREATE TEMPORARY TABLE tmp (%s)'
        qr = qr % ', '.join('"%s" %s %s' % (
            col.name,
            fields[0].ftype,
            not_null(col.name)) \
        for col, fields in self.field_map.iteritems())
        execute(qr)

        # Handle list of dict and dataframes
        if isinstance(data, list) and isinstance(data[0], dict):
            data = [[record.get(f.name) for f in self.fields] \
                    for record in data]
        elif pandas and isinstance(data, pandas.DataFrame):
            fields = [f.name for f in self.fields]
            data = data[fields].values

        # Fill tmp
        if self.ctx.flavor == 'postgresql':
            buff = io.BytesIO()
            writer = csv.writer(buff, delimiter='\t')
            for row in data:
                line = self.format_line(row)
                writer.writerow(list(line))
            buff.seek(0)
            copy_from(buff, 'tmp', null='')
        else:
            qr = 'INSERT INTO tmp (%(fields)s) VALUES (%(values)s)'
            qr = qr % {
                'fields': ', '.join('"%s"' % c.name for c in self.field_map),
                'values': ', '.join('%s' for _ in self.field_map),
            }
            data = [list(self.format_line(row)) for row in data]
            executemany(qr, data)

        # Create join conditions
        join_cond = []
        for name in self.index_cols:
            join_cond.append('tmp."%s" = "%s"."%s"' % (
                name, self.table.name, name))

        yield join_cond

        # Clean tmp table
        execute('DROP TABLE tmp')
        # Clean cache for current table
        self.ctx.reset_cache(self.table.name)

    def delete(self, data=None, filters=None, filter_by=None):
        if not any((data, filters, filter_by)):
            raise ValueError('No deletion criteria given')

        if data and (filters or filter_by):
            raise ValueError('Deletion by both data and filter not supported')

        where, qr_params, ref_set = self._build_filter_cond(
            filters=filters, filter_by=filter_by)

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
                 'SELECT %(main_table)s.id FROM %(main_table)s '
                  '%(joins)s '
                  'WHERE %(where)s)')

            qr = qr % {
                'main_table': self.table.name,
                'where': ' AND '.join(where),
                'joins': ' '.join(ref_set.get_sql_joins())
            }
            execute(qr, qr_params)

    def write(self, data, purge=False, insert=True, update=True):
        with self._prepare_write(data) as join_cond:
            # Insertion step
            if insert:
                self._insert(join_cond)
            if update:
                self._update(join_cond)
            if purge:
                self._purge(join_cond)

        # Clean cache for current table
        ctx.reset_cache(self.table.name)

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
        execute(qr)

    def _update(self, join_cond):
        update_cols = [c.name for c in self.field_map \
                       if c.name not in self.table.index]
        for name in update_cols:
            if self.ctx.flavor == 'sqlite':
                qr = 'UPDATE "%(main)s" SET "%(name)s" = COALESCE((' \
                      'SELECT "%(name)s" FROM tmp WHERE %(where)s' \
                     '), %(name)s)'
            elif self.ctx.flavor == 'postgresql':
                qr = 'UPDATE "%(main)s" '\
                     'SET "%(name)s" = tmp."%(name)s"' \
                     'FROM tmp WHERE %(where)s'

            qr = qr % {
                'main': self.table.name,
                'name': name,
                'where': ' AND '.join(join_cond),
            }
            execute(qr)

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
        execute(qr)

    def read_df(self, filters=None, disable_acl=False, order=None, limit=None):
        if not pandas:
            raise ImportError('The pandas module is required by read_df')

        # Create df from read data
        data = self.read(filters=filters, disable_acl=disable_acl,
                         order=order, limit=limit)
        read_columns = [f.name for f in self.fields]
        df = pandas.DataFrame.from_records(data, columns=read_columns)

        return df


class Cursor:

    def __init__(self, qr, qr_params, args):
        self.db_cursor = None
        self.qr = qr
        self.qr_params = qr_params
        if isinstance(args, dict):
            self._kwargs = args
            self._args = None
        else:
            self._args = args
            self._kwargs = None

    def args(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        # reset db_cursor to allow to call args & re-launch query
        self.db_cursor = None
        return self

    def __iter__(self):
        if self.db_cursor is not None:
            return self.db_cursor

        kwargs = {}
        kwargs.update(self._kwargs or {})
        cfg = ctx.cfg
        kwargs.update(cfg)
        self.db_cursor = execute(self.qr, self.qr_params, self._args, kwargs)
        return self.db_cursor

    def next(self):
        return next(iter(self))

    def all(self):
        return list(self)

class Table:

    def __init__(self, name, columns, index=None, unique=None, values=None):
        self.name = name
        self.columns = columns[:]
        self.unique = unique or []
        self.values = values
        # Add implicit id column
        if 'id' not in [c.name for c in self.columns]:
            self.columns.append(Column('id', 'INTEGER'))

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
        return self._column_dict[name]

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


class Column:

    def __init__(self, name, ctype, default=None):
        if ' ' in ctype:
            ctype, self.fk = ctype.split()
        else:
            self.fk = None
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
        foreign_table, foreign_field = self.fk.split('.')
        return 'INTEGER REFERENCES "%s" (%s) ON DELETE CASCADE' % (
            foreign_table, foreign_field)

    def get_foreign_table(self):
        name, _ = self.fk.split('.')
        return Table.get(name)


    def format(self, value, encoding=None):
        '''
        Sanitize value wrt the column type of the current field.
        '''

        if value is None:
            return None
        elif pandas and pandas.isnull(value):
            return None

        if self.ctype == 'INTEGER' and not isinstance(value, int):
            value = int(value)
        elif self.ctype == 'VARCHAR':
            if not isinstance(value, basestring):
                value = str(value)
            value = value.strip()
            if encoding is not None:
                value = value.encode(encoding)
        elif self.ctype == 'TIMESTAMP' and hasattr(value, 'timetuple'):
            value = datetime.datetime(*value.timetuple()[:6])
        elif self.ctype == 'DATE' and hasattr(value, 'timetuple'):
            value = datetime.date(*value.timetuple()[:3])

        return value

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
        resolve the fields that were added through the add() method.
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
            left_table, right_table, left_field, right_field = key
            yield 'LEFT JOIN %s AS %s ON (%s.%s = %s.%s)' % (
                right_table, alias, left_table, left_field, alias, right_field
            )

    def get_ref(self, desc, table=None, alias=None):
        table = table or self.table
        alias = alias or self.table_alias

        # Simple field, return
        if not '.' in desc:
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
            left_field = head
            right_field = 'id'
        else:
            left_field = 'id'
            right_field = rel.fk.split('.')[1]

        key_alias = '%s_%s' % (right_table, self.get_nb_joins())
        key = (left_table, right_table, left_field, right_field)
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


class ExpressionSymbol(str):
    pass

class ExpressionParam(str):
    pass

class Expression(object):
    # Inspired by http://norvig.com/lispy.html

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
        'not': lambda x: 'not %s' % x,
        'exists': lambda x: 'EXISTS (%s)' % x,
        'where': lambda *x: 'WHERE ' + ' AND '.join(x),
    }

    def __init__(self, view, ref_set=None, parent=None):
        self.params = None
        self.view = view
        # Populate env with view fields
        self.env = self.base_env(view.table)

        self.builtins = Expression.builtins.copy()
        self.builtins['select'] = self._select

        # Build expression id
        self.parent = parent

        # Add refset
        if not ref_set:
            ref_set = ReferenceSet(view.table)
        self.ref_set = ref_set

    def base_env(self, table, ref_set=None):
        env = {}
        for field in self.view.fields:
            env[field.name] = field
        return env

    def _select(self, *fields):
        res = 'SELECT %s FROM %s' % (
            ', '.join(fields),
            self.ref_set.table_alias,
        )
        return res

    def sub_expression(self, table, tail):
        view = View(table)
        ref_set = ReferenceSet(view.table, parent=self.ref_set)
        exp = Expression(view, ref_set, parent=self)
        exp.params = self.params
        env = exp.base_env(view.table)
        res = [exp._eval(subexp , env) for subexp in tail]
        joins = ' '.join(ref_set.get_sql_joins())
        if joins:
            # First child is select, so we inject joins just after
            res.insert(1, joins)
        return ' '.join(res)

    def eval(self, exp):
        self.params = []
        # Parse string
        lexer = shlex.shlex(exp.encode('utf-8'))
        lexer.wordchars += '.!=<>:{}'
        ast = self.read(list(lexer))

        # Eval ast wrt to env
        res = self._eval(ast, self.env)
        return res

    def _eval(self, exp, env):
        if isinstance(exp, ExpressionSymbol):
            # Try to resolve x wrt current view
            if exp.lower() in self.builtins:
                return self.builtins[exp.lower()]

            ref = None
            if exp.startswith('_parent.'):
                tail = exp
                parent = self
                while tail.startswith('_parent.'):
                    head, tail = tail.split('.', 1)
                    parent = parent.parent
                try:
                    ref = parent.ref_set.add(tail)
                except KeyError:
                    pass
            elif exp in env:
                val = env[exp]
                ref = self.ref_set.add(val.desc)
            else:
                try:
                    ref = self.ref_set.add(exp)
                except KeyError:
                    pass

            if ref:
                res = '%s.%s' % (ref.join_alias, ref.remote_field)
                return res
            else:
                raise ValueError('"%s" not understood' % exp)

        elif isinstance(exp, ExpressionParam):
            self.params.append(exp)
            return exp

        elif not isinstance(exp, list):
            return self.emit_literal(exp)

        elif exp[0].upper() == 'FROM':
            return self.sub_expression(exp[1], exp[2:])

        else:
            params = []
            proc = self._eval(exp.pop(0), env)
            for x in exp:
                val = self._eval(x, env)
                params.append(val)
            res = proc(*params)
            return res

    @classmethod
    def read(cls, tokens, top_level=True):
        if len(tokens) == 0:
            raise SyntaxError('unexpected EOF while reading')
        token = tokens.pop(0)
        if token == '(':
            L = []
            while tokens[0] != ')':
                L.append(cls.read(tokens, top_level=False))
            tokens.pop(0) # pop off ')'
            if tokens and top_level:
                raise ValueError('Unexpected tokens after ending ")"')
            return L
        elif token == ')':
            raise SyntaxError('unexpected )')
        else:
            return cls.atom(token)

    @classmethod
    def atom(cls, token):
        for q in ('"', "'"):
            if token[0] == q and token[-1] == q:
                return token[1:-1]

        if len(token) > 1 and token[0] == '{' and token[-1] == '}':
            return ExpressionParam(token)

        try:
            return int(token)
        except ValueError:
            pass
        try:
            return float(token)
        except ValueError:
            return ExpressionSymbol(token)

    def emit_literal(self, x):
        # Collect literal and return placeholder
        if isinstance(x, (tuple, list)):
            self.params.extend(x)
            return ', '.join('%s' for _ in x)

        self.params.append(x)
        return '%s'

def parse_uri(db_uri):
    uri = urlparse(db_uri)
    uri.dbname = uri.path[1:] # Ignore the first /
    return uri

@contextmanager
def connect(cfg):
    uri = parse_uri(cfg.get('db_uri', 'sqlite:///:memory:'))
    flavor = uri.scheme

    if flavor == 'sqlite':
        db_path = uri.dbname
        connection = sqlite3.connect(db_path, check_same_thread=False,
                                     detect_types=sqlite3.PARSE_DECLTYPES)
        connection.execute('PRAGMA foreign_keys=ON')

    elif flavor == 'postgresql':
        if psycopg2 is None:
            raise ImportError(
                'Cannot connect to "%s" without psycopg2 package '\
                'installed' % uri)
        con_info = "dbname='%s' " % uri.dbname
        if uri.hostname:
            con_info += "host='%s' " % uri.hostname
        if uri.username:
            con_info += "user='%s' " % uri.username
        if uri.password:
            con_info += "password='%s' " % uri.password
        connection = psycopg2.connect(con_info)
        connection.set_client_encoding('UTF8')

    else:
        raise ValueError('Unsupported scheme "%s" in uri "%s"' % (
            uri.scheme, uri))

    with connection:
        new_ctx = CTX_STACK.push(flavor=flavor, connection=connection, cfg=cfg)

        try:
            yield new_ctx
        finally:
            CTX_STACK.pop()


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
